#!/usr/bin/env bash
set -euo pipefail

export AWS_REGION="${AWS_REGION:-us-east-1}"
export COACH_BUCKET="${COACH_BUCKET:-CHANGE_ME_BUCKET}"
export COACH_PREFIX="${COACH_PREFIX:-coach/}"
export FRONTEND_ORIGINS="${FRONTEND_ORIGINS:-http://localhost:5173}"

uvicorn main:app --reload --host 0.0.0.0 --port 8080
