#!/bin/bash
cd /home/kavia/workspace/code-generation/energy-insights-and-alerts-3200/backend
source venv/bin/activate
flake8 .
LINT_EXIT_CODE=$?
if [ $LINT_EXIT_CODE -ne 0 ]; then
  exit 1
fi

