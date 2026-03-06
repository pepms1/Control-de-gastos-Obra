#!/usr/bin/env bash
set -euo pipefail

if rg -n "importCsv\([^\)]*metadata\s*=|run_sap_import\([^\)]*metadata\s*=" api/main.py api/scripts web/src --glob '!web/node_modules/**'; then
  echo "Guardrail failed: metadata= is not allowed in import calls"
  exit 1
fi

echo "Guardrail passed: no metadata= in import calls"
