#!/usr/bin/env python3
import os
import sys
import json
import urllib.request
import urllib.error
import urllib.parse

BASE = os.environ.get("BASE_URL", "https://control-de-gastos-obra.onrender.com").rstrip("/")
PROJECT = os.environ.get("PROJECT", "CALDERON DE LA BARCA").strip()

USERNAME = os.environ.get("CRON_USERNAME", "").strip()
PASSWORD = os.environ.get("CRON_PASSWORD", "").strip()

LOGIN_URL = f"{BASE}/api/auth/login"
CRON_URL = f"{BASE}/api/cron/import/sap-latest?project={urllib.parse.quote(PROJECT)}"

def http_json(url: str, payload: dict, headers: dict | None = None, timeout: int = 60):
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        method="POST",
        headers={"Content-Type": "application/json", **(headers or {})},
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        body = resp.read().decode("utf-8", "replace")
        return resp.status, dict(resp.headers), body

def http_post(url: str, headers: dict | None = None, timeout: int = 600):
    req = urllib.request.Request(url, method="POST", headers=headers or {})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        body = resp.read().decode("utf-8", "replace")
        return resp.status, dict(resp.headers), body

print("CRON START")
print("PROJECT", PROJECT)
print("LOGIN_URL", LOGIN_URL)
print("CRON_URL", CRON_URL)

if not USERNAME or not PASSWORD:
    print("ERROR Missing CRON_USERNAME or CRON_PASSWORD env vars")
    sys.exit(1)

try:
    # 1) Login → token fresco
    st, h, body = http_json(LOGIN_URL, {"username": USERNAME, "password": PASSWORD})
    if st != 200:
        print("LOGIN_STATUS", st)
        print("RNDR-ID", h.get("rndr-id"))
        print(body)
        sys.exit(1)

    o = json.loads(body)
    token = (o.get("access_token") or "").strip()
    if not token:
        print("ERROR login ok but missing access_token")
        print(body)
        sys.exit(1)

    print("LOGIN_STATUS", st, "TOKEN_LEN", len(token))

    # 2) Llamar cron endpoint con Bearer
    headers = {"Authorization": f"Bearer {token}"}
    st2, h2, body2 = http_post(CRON_URL, headers=headers)
    print("STATUS", st2)
    print("RNDR-ID", h2.get("rndr-id"))
    print(body2)

    if st2 >= 400:
        sys.exit(1)

except urllib.error.HTTPError as e:
    body = e.read().decode("utf-8", "replace")
    print("STATUS", e.code)
    print("RNDR-ID", getattr(e, "headers", {}).get("rndr-id") if getattr(e, "headers", None) else None)
    print(body)
    sys.exit(1)
except Exception as e:
    print("ERROR", repr(e))
    sys.exit(1)
