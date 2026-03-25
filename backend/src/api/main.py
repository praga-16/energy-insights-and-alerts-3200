from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import statistics
import time
import uuid
from collections import deque
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Deque, Dict, List, Optional, Set, Tuple

from fastapi import Body, FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field


def _utc_ms() -> int:
    """Return current UTC timestamp in milliseconds."""
    return int(time.time() * 1000)


def _parse_range_to_hours(range_str: str) -> int:
    """
    Parse a UI range string into hours.

    Supported:
    - 24h, 7d, 30d
    """
    r = (range_str or "").strip().lower()
    if r.endswith("h"):
        return int(r[:-1])
    if r.endswith("d"):
        return int(r[:-1]) * 24
    raise ValueError("range must be one of: 24h, 7d, 30d (or Nh/Nd)")


def _iso_or_ms(ts_ms: int) -> str:
    """Return a human-friendly ISO time string for a millisecond timestamp."""
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).isoformat()


# -----------------------------
# SQLite database integration
# -----------------------------

@dataclass(frozen=True)
class DbConfig:
    """Database configuration derived from environment variables."""
    db_path: str


def _get_db_config() -> DbConfig:
    """
    Load DB config.

    Environment variables:
    - SQLITE_DB: path to sqlite database file
      (provided by the platform; do not hardcode)
    """
    db_path = (os.getenv("SQLITE_DB") or "").strip()
    if not db_path:
        # Fallback: use local file within backend container if env var isn't set.
        # This keeps dev working while still preferring env var in deployment.
        db_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "energy.db"))
    return DbConfig(db_path=db_path)


def _connect(db: DbConfig) -> sqlite3.Connection:
    """Create a sqlite3 connection with reasonable defaults."""
    conn = sqlite3.connect(db.db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    return conn


def _init_schema(db: DbConfig) -> None:
    """Idempotently initialize the database schema."""
    conn = _connect(db)
    try:
        # Measurements: one row per interval.
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS measurements (
                id TEXT PRIMARY KEY,
                site TEXT NOT NULL,
                ts_ms INTEGER NOT NULL,
                kwh REAL NOT NULL,
                meta_json TEXT,
                created_at_ms INTEGER NOT NULL
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_measurements_site_ts ON measurements(site, ts_ms)"
        )

        # Alerts emitted by anomaly detection logic.
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS alerts (
                id TEXT PRIMARY KEY,
                site TEXT NOT NULL,
                ts_ms INTEGER NOT NULL,
                severity TEXT NOT NULL,
                title TEXT NOT NULL,
                message TEXT NOT NULL,
                acknowledged INTEGER NOT NULL DEFAULT 0,
                rule_id TEXT,
                measurement_ts_ms INTEGER,
                measurement_kwh REAL,
                baseline_mean REAL,
                baseline_std REAL,
                z_score REAL,
                created_at_ms INTEGER NOT NULL
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_alerts_site_ts ON alerts(site, ts_ms)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_alerts_ack ON alerts(acknowledged, ts_ms)")

        # Optional alert rules (simple stored config). Frontend doesn't use yet, but requested.
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS alert_rules (
                id TEXT PRIMARY KEY,
                site TEXT NOT NULL,
                name TEXT NOT NULL,
                enabled INTEGER NOT NULL DEFAULT 1,
                threshold_kwh REAL,
                z_threshold REAL,
                window_points INTEGER,
                created_at_ms INTEGER NOT NULL,
                updated_at_ms INTEGER NOT NULL
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_rules_site ON alert_rules(site)")

        # Seed default rule per site lazily on first access; no global seed here.
        conn.commit()
    finally:
        conn.close()


# -----------------------------
# Pydantic models
# -----------------------------

class SiteRef(BaseModel):
    """A lightweight site reference used by the dashboard."""
    id: str = Field(..., description="Site identifier.")
    name: str = Field(..., description="Human-friendly site name.")


class SeriesPoint(BaseModel):
    """Time-series point used by the dashboard chart."""
    ts: int = Field(..., description="Timestamp in milliseconds since epoch.")
    kwh: float = Field(..., description="Energy consumption in kWh for the interval.")


class DashboardKpis(BaseModel):
    """Key metrics displayed on the dashboard."""
    total_kwh: float = Field(..., description="Total kWh in the selected range.")
    avg_kw: float = Field(..., description="Average load (kW) approximated per hour from kWh intervals.")
    peak_kw: float = Field(..., description="Peak interval kWh in the selected range.")
    anomaly_count: int = Field(..., description="Number of anomalies detected in the selected range.")


class Insight(BaseModel):
    """Dashboard insight card."""
    id: str = Field(..., description="Insight id.")
    title: str = Field(..., description="Insight title.")
    detail: str = Field(..., description="Insight detail.")


class DashboardSnapshot(BaseModel):
    """Combined dashboard payload expected by the React UI."""
    site: SiteRef
    range: str = Field(..., description="Range string, e.g. 24h/7d/30d.")
    kpis: DashboardKpis
    series: List[SeriesPoint]
    insights: List[Insight]


class IngestMeasurement(BaseModel):
    """Single measurement ingestion payload."""
    site: str = Field(..., description="Site name/id to attribute the measurement to.")
    ts: Optional[int] = Field(None, description="Timestamp in ms. If omitted, server uses current time.")
    kwh: float = Field(..., ge=0, description="kWh consumed for the interval.")
    meta: Optional[Dict[str, Any]] = Field(None, description="Optional metadata (stored as JSON).")


class IngestBatch(BaseModel):
    """Batch ingestion payload."""
    measurements: List[IngestMeasurement] = Field(..., description="Measurements to ingest.")


class AlertOut(BaseModel):
    """Alert model aligned with frontend expectations."""
    id: str = Field(..., description="Alert id.")
    severity: str = Field(..., description="Severity: low|medium|high.")
    title: str = Field(..., description="Short title.")
    message: str = Field(..., description="Human-friendly description.")
    ts: int = Field(..., description="Detection timestamp in ms.")
    acknowledged: bool = Field(False, description="Whether the alert has been acknowledged.")


class AckResponse(BaseModel):
    """Acknowledgement response."""
    ok: bool = Field(..., description="Whether the acknowledge operation succeeded.")
    id: str = Field(..., description="Alert id.")


class AlertRuleIn(BaseModel):
    """Create/update alert rule."""
    site: str = Field(..., description="Site for the rule.")
    name: str = Field(..., description="Rule name.")
    enabled: bool = Field(True, description="Whether rule is active.")
    threshold_kwh: Optional[float] = Field(
        None, ge=0, description="Absolute kWh threshold for spike detection."
    )
    z_threshold: Optional[float] = Field(
        3.0, ge=0, description="Z-score threshold for anomaly detection."
    )
    window_points: Optional[int] = Field(
        24, ge=5, description="Rolling baseline window size (number of points)."
    )


class AlertRuleOut(AlertRuleIn):
    """Alert rule returned to clients."""
    id: str = Field(..., description="Rule id.")
    created_at_ms: int = Field(..., description="Created at (ms).")
    updated_at_ms: int = Field(..., description="Updated at (ms).")


# -----------------------------
# Realtime alert broadcasting
# -----------------------------

class RealtimeHub:
    """Keeps track of connected WebSocket clients and SSE queues."""

    def __init__(self) -> None:
        self._ws_clients: Set[WebSocket] = set()
        self._sse_queues: Set[asyncio.Queue[dict]] = set()
        self._lock = asyncio.Lock()

    async def add_ws(self, ws: WebSocket) -> None:
        async with self._lock:
            self._ws_clients.add(ws)

    async def remove_ws(self, ws: WebSocket) -> None:
        async with self._lock:
            self._ws_clients.discard(ws)

    async def add_sse(self) -> asyncio.Queue[dict]:
        q: asyncio.Queue[dict] = asyncio.Queue(maxsize=100)
        async with self._lock:
            self._sse_queues.add(q)
        return q

    async def remove_sse(self, q: asyncio.Queue[dict]) -> None:
        async with self._lock:
            self._sse_queues.discard(q)

    async def broadcast(self, payload: dict) -> None:
        """
        Broadcast to all transports.

        WebSocket: JSON message
        SSE: queued dict (converted to SSE frames per connection)
        """
        # Snapshot clients under lock, but do sending outside to avoid blocking.
        async with self._lock:
            ws_clients = list(self._ws_clients)
            sse_queues = list(self._sse_queues)

        # Send to WS clients.
        dead: List[WebSocket] = []
        for ws in ws_clients:
            try:
                await ws.send_text(json.dumps(payload))
            except Exception:
                dead.append(ws)

        if dead:
            async with self._lock:
                for ws in dead:
                    self._ws_clients.discard(ws)

        # Push to SSE queues (best-effort).
        for q in sse_queues:
            try:
                q.put_nowait(payload)
            except asyncio.QueueFull:
                # Drop oldest by draining one.
                try:
                    _ = q.get_nowait()
                except Exception:
                    pass
                try:
                    q.put_nowait(payload)
                except Exception:
                    pass


# -----------------------------
# Anomaly detection
# -----------------------------

def _severity_from_z(z: float) -> str:
    if z >= 4.0:
        return "high"
    if z >= 3.0:
        return "medium"
    return "low"


def _detect_anomaly(
    history: Deque[float],
    current: float,
    *,
    abs_threshold_kwh: Optional[float],
    z_threshold: float,
) -> Optional[Tuple[float, float, float]]:
    """
    Determine if current value is anomalous relative to rolling baseline.

    Returns tuple(mean, std, z) if anomalous, else None.
    """
    if abs_threshold_kwh is not None and current >= abs_threshold_kwh:
        # Allow absolute threshold to trigger even if rolling stats not stable.
        if len(history) >= 2:
            mean = statistics.fmean(history)
            std = statistics.pstdev(history) or 1e-9
            z = (current - mean) / std
        else:
            mean, std, z = current, 0.0, 0.0
        return (mean, std, z)

    if len(history) < 5:
        return None

    mean = statistics.fmean(history)
    std = statistics.pstdev(history) or 1e-9
    z = (current - mean) / std
    if z >= z_threshold:
        return (mean, std, z)
    return None


# -----------------------------
# App setup
# -----------------------------

openapi_tags = [
    {"name": "Health", "description": "Basic service health endpoints."},
    {"name": "Ingestion", "description": "Ingest energy measurements."},
    {"name": "Analytics", "description": "Dashboard snapshots and time-series queries."},
    {"name": "Alerts", "description": "Query and acknowledge anomaly alerts."},
    {"name": "Rules", "description": "Alert rule management (optional)."},
    {"name": "Realtime", "description": "Realtime alerts over WebSocket or SSE."},
]


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize schema on startup (idempotent)."""
    db = _get_db_config()
    _init_schema(db)
    app.state.db = db
    app.state.hub = RealtimeHub()
    yield


app = FastAPI(
    title="Energy Insights & Alerts API",
    description=(
        "REST + realtime backend for commercial energy analytics and anomaly alerts.\n\n"
        "Realtime options:\n"
        "- WebSocket: `GET /ws/alerts`\n"
        "- SSE: `GET /api/alerts/stream` (EventSource-compatible)\n"
    ),
    version="0.3.0",
    openapi_tags=openapi_tags,
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Deployment can tighten this.
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# -----------------------------
# Helpers (DB)
# -----------------------------

def _ensure_default_rule(conn: sqlite3.Connection, site: str) -> sqlite3.Row:
    """Create a default rule for the site if missing; return it."""
    cur = conn.execute(
        "SELECT * FROM alert_rules WHERE site = ? ORDER BY created_at_ms ASC LIMIT 1",
        (site,),
    )
    row = cur.fetchone()
    if row:
        return row

    now = _utc_ms()
    rid = f"rule_{uuid.uuid4().hex}"
    conn.execute(
        """
        INSERT INTO alert_rules (id, site, name, enabled, threshold_kwh, z_threshold, window_points, created_at_ms, updated_at_ms)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (rid, site, "Default anomaly rule", 1, None, 3.0, 24, now, now),
    )
    conn.commit()
    cur = conn.execute("SELECT * FROM alert_rules WHERE id = ?", (rid,))
    return cur.fetchone()


def _row_to_alert_out(r: sqlite3.Row) -> AlertOut:
    return AlertOut(
        id=r["id"],
        severity=r["severity"],
        title=r["title"],
        message=r["message"],
        ts=int(r["ts_ms"]),
        acknowledged=bool(r["acknowledged"]),
    )


def _fetch_series(
    conn: sqlite3.Connection, site: str, start_ms: int, end_ms: int, max_points: int = 500
) -> List[SeriesPoint]:
    """
    Fetch time series points.

    If there are more than max_points rows, downsample by taking every Nth row.
    """
    cur = conn.execute(
        """
        SELECT ts_ms, kwh
        FROM measurements
        WHERE site = ? AND ts_ms BETWEEN ? AND ?
        ORDER BY ts_ms ASC
        """,
        (site, start_ms, end_ms),
    )
    rows = cur.fetchall()
    if not rows:
        return []

    if len(rows) <= max_points:
        return [SeriesPoint(ts=int(r["ts_ms"]), kwh=float(r["kwh"])) for r in rows]

    step = max(1, len(rows) // max_points)
    sampled = rows[::step]
    return [SeriesPoint(ts=int(r["ts_ms"]), kwh=float(r["kwh"])) for r in sampled]


def _compute_kpis(series: List[SeriesPoint], anomaly_count: int) -> DashboardKpis:
    if not series:
        return DashboardKpis(total_kwh=0.0, avg_kw=0.0, peak_kw=0.0, anomaly_count=anomaly_count)

    total = float(sum(p.kwh for p in series))
    peak = float(max(p.kwh for p in series))
    # The UI labels avg as kW per hour; our data is "kWh per interval".
    # Assume interval ~ 1 hour for the default mock-style chart.
    avg = float(total / max(1, len(series)))
    return DashboardKpis(
        total_kwh=round(total, 3),
        avg_kw=round(avg, 3),
        peak_kw=round(peak, 3),
        anomaly_count=anomaly_count,
    )


def _derive_insights(series: List[SeriesPoint], threshold: Optional[float]) -> List[Insight]:
    """Generate lightweight, explainable insights."""
    if len(series) < 6:
        return []

    values = [p.kwh for p in series]
    mean = statistics.fmean(values)
    peak = max(values)
    insights: List[Insight] = []

    # Peak event insight
    peak_idx = values.index(peak)
    insights.append(
        Insight(
            id="ins_peak",
            title="Peak interval",
            detail=f"Highest interval was {peak:.0f} kWh at {_iso_or_ms(series[peak_idx].ts)} (avg {mean:.0f}).",
        )
    )

    # Threshold exceedances (if provided)
    if threshold is not None:
        above = sum(1 for v in values if v >= threshold)
        if above > 0:
            insights.append(
                Insight(
                    id="ins_threshold",
                    title="Threshold exceedances",
                    detail=f"{above} interval(s) met or exceeded {threshold:.0f} kWh in the selected window.",
                )
            )

    # Variability insight
    if len(values) >= 10:
        std = statistics.pstdev(values) or 0.0
        cv = (std / mean) if mean else 0.0
        if cv >= 0.25:
            insights.append(
                Insight(
                    id="ins_variability",
                    title="High variability",
                    detail=f"Consumption varied significantly (std {std:.0f} kWh). Consider investigating operational schedules.",
                )
            )

    return insights[:4]


# -----------------------------
# Routes
# -----------------------------

@app.get("/", tags=["Health"], summary="Health check")
def health_check() -> Dict[str, str]:
    """
    Health check endpoint used by the frontend to detect backend connectivity.

    Returns:
        JSON with a message.
    """
    return {"message": "Healthy"}


@app.get(
    "/docs/realtime",
    tags=["Realtime"],
    summary="Realtime usage guide",
)
def realtime_usage_guide() -> Dict[str, Any]:
    """
    Provides a short guide for using realtime alerts endpoints.

    Returns:
        JSON with example URLs and message formats.
    """
    return {
        "websocket": {
            "url": "/ws/alerts",
            "notes": "Client sends nothing; server pushes anomaly alerts as JSON.",
            "example_message": {"type": "alert", "data": {"id": "al_123", "title": "Spike detected"}},
        },
        "sse": {
            "url": "/api/alerts/stream",
            "notes": "EventSource-compatible SSE stream (text/event-stream).",
            "event": "alert",
            "data_json": {"type": "alert", "data": {"id": "al_123", "title": "Spike detected"}},
        },
    }


@app.post(
    "/api/ingest",
    tags=["Ingestion"],
    summary="Ingest a batch of measurements",
    response_description="Ingestion result",
)
async def ingest_batch(payload: IngestBatch = Body(...)) -> Dict[str, Any]:
    """
    Ingest energy measurements and run anomaly detection.

    This endpoint is safe to call repeatedly; records are inserted with generated ids.
    For de-duplication, clients should provide stable (site, ts) and use /api/ingest/upsert
    (not implemented in this step) or avoid re-sending the same points.

    Parameters:
        payload: Batch with measurements.

    Returns:
        Count of ingested rows and generated alert ids (if any).
    """
    db: DbConfig = app.state.db
    hub: RealtimeHub = app.state.hub

    if not payload.measurements:
        return {"ok": True, "inserted": 0, "alerts": []}

    inserted = 0
    alert_ids: List[str] = []
    now = _utc_ms()

    # Group points by site so we can build a rolling baseline per site efficiently.
    by_site: Dict[str, List[IngestMeasurement]] = {}
    for m in payload.measurements:
        by_site.setdefault(m.site, []).append(m)

    conn = _connect(db)
    try:
        for site, points in by_site.items():
            # Ensure default rule exists.
            rule = _ensure_default_rule(conn, site)
            enabled = bool(rule["enabled"])
            abs_threshold_kwh = rule["threshold_kwh"]
            z_threshold = float(rule["z_threshold"] or 3.0)
            window_points = int(rule["window_points"] or 24)

            # Fetch recent history for rolling baseline.
            # Take last window_points points prior to the earliest ingested timestamp.
            points_sorted = sorted(points, key=lambda x: (x.ts or now))
            earliest_ts = int(points_sorted[0].ts or now)

            cur = conn.execute(
                """
                SELECT kwh
                FROM measurements
                WHERE site = ? AND ts_ms < ?
                ORDER BY ts_ms DESC
                LIMIT ?
                """,
                (site, earliest_ts, window_points),
            )
            history_vals = [float(r["kwh"]) for r in cur.fetchall()][::-1]
            history: Deque[float] = deque(history_vals, maxlen=window_points)

            for m in points_sorted:
                ts_ms = int(m.ts or _utc_ms())
                mid = f"msr_{uuid.uuid4().hex}"
                conn.execute(
                    """
                    INSERT INTO measurements (id, site, ts_ms, kwh, meta_json, created_at_ms)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        mid,
                        site,
                        ts_ms,
                        float(m.kwh),
                        json.dumps(m.meta) if m.meta is not None else None,
                        now,
                    ),
                )
                inserted += 1

                # Run anomaly detection (if enabled).
                if enabled:
                    detected = _detect_anomaly(
                        history,
                        float(m.kwh),
                        abs_threshold_kwh=float(abs_threshold_kwh) if abs_threshold_kwh is not None else None,
                        z_threshold=z_threshold,
                    )
                    if detected:
                        mean, std, z = detected
                        severity = _severity_from_z(float(z))
                        aid = f"al_{uuid.uuid4().hex}"
                        title = "Spike detected" if (abs_threshold_kwh is not None and m.kwh >= float(abs_threshold_kwh)) else "Anomalous usage"
                        message = (
                            f"{site}: interval {m.kwh:.0f} kWh (baseline {mean:.0f} ± {std:.0f}, z={z:.1f})."
                        )
                        conn.execute(
                            """
                            INSERT INTO alerts (
                                id, site, ts_ms, severity, title, message, acknowledged,
                                rule_id, measurement_ts_ms, measurement_kwh, baseline_mean, baseline_std, z_score,
                                created_at_ms
                            )
                            VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                aid,
                                site,
                                _utc_ms(),
                                severity,
                                title,
                                message,
                                rule["id"],
                                ts_ms,
                                float(m.kwh),
                                float(mean),
                                float(std),
                                float(z),
                                _utc_ms(),
                            ),
                        )
                        alert_ids.append(aid)

                        # Broadcast in realtime to connected clients.
                        out = {
                            "type": "alert",
                            "data": AlertOut(
                                id=aid,
                                severity=severity,
                                title=title,
                                message=message,
                                ts=_utc_ms(),
                                acknowledged=False,
                            ).model_dump(),
                        }
                        await hub.broadcast(out)

                # Update rolling window after evaluation.
                history.append(float(m.kwh))

        conn.commit()
    finally:
        conn.close()

    return {"ok": True, "inserted": inserted, "alerts": alert_ids}


@app.get(
    "/api/dashboard",
    tags=["Analytics"],
    summary="Get dashboard snapshot",
    response_model=DashboardSnapshot,
)
def get_dashboard_snapshot(
    site: str = Query("Acme HQ", description="Site name/id"),
    range: str = Query("24h", description="Time range: 24h, 7d, 30d"),
    threshold: Optional[float] = Query(None, ge=0, description="Optional spike threshold used for insights"),
) -> DashboardSnapshot:
    """
    Returns the analytics snapshot expected by the React UI.

    Query parameters align with the frontend state (site/range/threshold), but the UI
    currently calls without params; defaults mirror the UI defaults.

    Returns:
        DashboardSnapshot containing KPIs, series points, and insights.
    """
    db: DbConfig = app.state.db

    try:
        hours = _parse_range_to_hours(range)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

    end_ms = _utc_ms()
    start_ms = int((datetime.now(tz=timezone.utc) - timedelta(hours=hours)).timestamp() * 1000)

    conn = _connect(db)
    try:
        series = _fetch_series(conn, site, start_ms, end_ms, max_points=500)

        # Anomalies count in range
        cur = conn.execute(
            "SELECT COUNT(1) AS c FROM alerts WHERE site = ? AND ts_ms BETWEEN ? AND ?",
            (site, start_ms, end_ms),
        )
        anomaly_count = int((cur.fetchone() or {"c": 0})["c"])

        kpis = _compute_kpis(series, anomaly_count=anomaly_count)
        insights = _derive_insights(series, threshold=threshold)

        return DashboardSnapshot(
            site=SiteRef(id=f"site_{site.lower().replace(' ', '_')}", name=site),
            range=range,
            kpis=kpis,
            series=series,
            insights=insights,
        )
    finally:
        conn.close()


@app.get(
    "/api/alerts",
    tags=["Alerts"],
    summary="List recent alerts",
    response_model=List[AlertOut],
)
def list_alerts(
    site: Optional[str] = Query(None, description="Optional site filter"),
    limit: int = Query(50, ge=1, le=200, description="Max number of alerts to return"),
) -> List[AlertOut]:
    """
    Returns recent alerts as an array (frontend expects either array or {items: []}).

    Parameters:
        site: optional site filter.
        limit: limit on number of alerts.
    """
    db: DbConfig = app.state.db
    conn = _connect(db)
    try:
        if site:
            cur = conn.execute(
                """
                SELECT id, severity, title, message, ts_ms, acknowledged
                FROM alerts
                WHERE site = ?
                ORDER BY ts_ms DESC
                LIMIT ?
                """,
                (site, limit),
            )
        else:
            cur = conn.execute(
                """
                SELECT id, severity, title, message, ts_ms, acknowledged
                FROM alerts
                ORDER BY ts_ms DESC
                LIMIT ?
                """,
                (limit,),
            )
        rows = cur.fetchall()
        return [_row_to_alert_out(r) for r in rows]
    finally:
        conn.close()


@app.post(
    "/api/alerts/{alert_id}/ack",
    tags=["Alerts"],
    summary="Acknowledge an alert",
    response_model=AckResponse,
)
def acknowledge_alert(alert_id: str) -> AckResponse:
    """
    Acknowledge an alert by id.

    The React UI calls this best-effort and will keep local acknowledgement if it fails.

    Parameters:
        alert_id: Alert identifier.

    Returns:
        AckResponse.
    """
    if not alert_id:
        raise HTTPException(status_code=400, detail="alert_id is required")

    db: DbConfig = app.state.db
    conn = _connect(db)
    try:
        cur = conn.execute("SELECT id FROM alerts WHERE id = ?", (alert_id,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Alert not found")

        conn.execute("UPDATE alerts SET acknowledged = 1 WHERE id = ?", (alert_id,))
        conn.commit()
        return AckResponse(ok=True, id=alert_id)
    finally:
        conn.close()


@app.get(
    "/api/alerts/stream",
    tags=["Realtime"],
    summary="SSE stream of anomaly alerts",
)
async def sse_alerts_stream() -> StreamingResponse:
    """
    Server-Sent Events (SSE) stream of anomaly alerts.

    Use with EventSource in browsers:
        const es = new EventSource(`${API_BASE}/api/alerts/stream`);
        es.addEventListener('alert', (evt) => console.log(JSON.parse(evt.data)));

    Returns:
        text/event-stream streaming response.
    """
    hub: RealtimeHub = app.state.hub
    q = await hub.add_sse()

    async def gen():
        # Initial comment to establish stream
        yield ": connected\n\n"
        try:
            while True:
                payload = await q.get()
                # EventSource expects "event:" + "data:" lines
                data = json.dumps(payload)
                yield f"event: alert\ndata: {data}\n\n"
        except asyncio.CancelledError:
            raise
        finally:
            await hub.remove_sse(q)

    return StreamingResponse(gen(), media_type="text/event-stream")


@app.websocket("/ws/alerts")
async def ws_alerts(websocket: WebSocket):
    """
    WebSocket realtime alerts channel.

    Operation:
    - Client connects and receives pushed anomaly alerts as JSON messages.
    - Message format matches frontend normalizer:
        { "type": "alert", "data": { ...AlertOut } }
    """
    hub: RealtimeHub = app.state.hub
    await websocket.accept()
    await hub.add_ws(websocket)

    # Send a hello message (optional; frontend ignores unknown messages).
    try:
        await websocket.send_text(json.dumps({"type": "hello", "ts": _utc_ms()}))
    except Exception:
        await hub.remove_ws(websocket)
        return

    try:
        while True:
            # Keep connection alive; ignore any incoming messages.
            _ = await websocket.receive_text()
    except WebSocketDisconnect:
        await hub.remove_ws(websocket)
    except Exception:
        await hub.remove_ws(websocket)


@app.get(
    "/api/rules",
    tags=["Rules"],
    summary="List alert rules",
    response_model=List[AlertRuleOut],
)
def list_rules(site: Optional[str] = Query(None, description="Optional site filter")) -> List[AlertRuleOut]:
    """
    List configured alert rules.

    Notes:
        Not used by the current React UI, but implemented for completeness.
    """
    db: DbConfig = app.state.db
    conn = _connect(db)
    try:
        if site:
            cur = conn.execute("SELECT * FROM alert_rules WHERE site = ? ORDER BY created_at_ms DESC", (site,))
        else:
            cur = conn.execute("SELECT * FROM alert_rules ORDER BY created_at_ms DESC")
        rows = cur.fetchall()
        out: List[AlertRuleOut] = []
        for r in rows:
            out.append(
                AlertRuleOut(
                    id=r["id"],
                    site=r["site"],
                    name=r["name"],
                    enabled=bool(r["enabled"]),
                    threshold_kwh=r["threshold_kwh"],
                    z_threshold=r["z_threshold"],
                    window_points=r["window_points"],
                    created_at_ms=int(r["created_at_ms"]),
                    updated_at_ms=int(r["updated_at_ms"]),
                )
            )
        return out
    finally:
        conn.close()


@app.post(
    "/api/rules",
    tags=["Rules"],
    summary="Create an alert rule",
    response_model=AlertRuleOut,
)
def create_rule(rule: AlertRuleIn) -> AlertRuleOut:
    """
    Create an alert rule for a site.
    """
    db: DbConfig = app.state.db
    conn = _connect(db)
    try:
        rid = f"rule_{uuid.uuid4().hex}"
        now = _utc_ms()
        conn.execute(
            """
            INSERT INTO alert_rules (id, site, name, enabled, threshold_kwh, z_threshold, window_points, created_at_ms, updated_at_ms)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                rid,
                rule.site,
                rule.name,
                1 if rule.enabled else 0,
                rule.threshold_kwh,
                rule.z_threshold,
                rule.window_points,
                now,
                now,
            ),
        )
        conn.commit()
        return AlertRuleOut(id=rid, created_at_ms=now, updated_at_ms=now, **rule.model_dump())
    finally:
        conn.close()
