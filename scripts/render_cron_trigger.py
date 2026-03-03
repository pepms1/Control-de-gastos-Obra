#!/usr/bin/env python3
import os
import sys
import urllib.error
import urllib.request
import urllib.parse

BASE = os.environ.get("BASE_URL", "https://control-de-gastos-obra.onrender.com")

# Proyecto a correr (Render env var: PROJECT)
# Ej: "CALDERON DE LA BARCA" o "PENSYLVANIA"
PROJECT = os.environ.get("PROJECT", "CALDERON DE LA BARCA")

# Endpoint del cron en tu backend
URL = f"{BASE}/api/cron/import/sap-latest?project={urllib.parse.quote(PROJECT)}"

# Si tu endpoint requiere Bearer token, ponlo como env var en Render:
# CRON_BEARER=eyJhbGciOi...
TOKEN = os.environ.get("CRON_BEARER", "").strip()

# (Opcional) si además usas secreto custom
SECRET = os.environ.get("CRON_SECRET", "").strip()

headers = {}
if TOKEN:
    headers["Authorization"] = f"Bearer {TOKEN}"
if SECRET:
    headers["X-Cron-Secret"] = SECRET

print("CRON START")
print("PROJECT", PROJECT)
print("URL", URL)

req = urllib.request.Request(URL, method="POST", headers=headers)

try:
    with urllib.request.urlopen(req, timeout=600) as response:
        body = response.read().decode("utf-8", "replace")
        print("STATUS", response.status)
        print("RNDR-ID", response.headers.get("rndr-id"))
        print(body)
except urllib.error.HTTPError as error:
    body = error.read().decode("utf-8", "replace")
    print("STATUS", error.code)
    print("RNDR-ID", error.headers.get("rndr-id"))
    print(body)
    sys.exit(1)
except Exception as error:
    print("ERROR", repr(error))
    sys.exit(1)
