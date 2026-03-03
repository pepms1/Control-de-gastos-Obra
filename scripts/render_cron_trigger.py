import os
import sys
import json
import urllib.request
import urllib.error
import urllib.parse

BASE = "https://control-de-gastos-obra.onrender.com"

PROJECT = os.environ.get("CRON_PROJECT", "CALDERON DE LA BARCA").strip()
USERNAME = os.environ.get("CRON_USERNAME", "").strip()
PASSWORD = os.environ.get("CRON_PASSWORD", "").strip()

LOGIN_URL = BASE + "/api/auth/login"
CRON_URL = BASE + "/api/cron/import/sap-latest?project=" + urllib.parse.quote(PROJECT)

def http_json(url: str, method: str = "GET", headers: dict | None = None, data_obj=None, timeout: int = 600):
    headers = headers or {}
    data = None
    if data_obj is not None:
        data = json.dumps(data_obj).encode("utf-8")
        headers = {**headers, "Content-Type": "application/json"}
    req = urllib.request.Request(url, method=method, headers=headers, data=data)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        body = resp.read().decode("utf-8", "replace")
        return resp.status, body

print("CRON START")
print("PROJECT", PROJECT)
print("URL", CRON_URL)

if not USERNAME or not PASSWORD:
    print("ERROR: Missing CRON_USERNAME / CRON_PASSWORD env vars")
    sys.exit(1)

# 1) Login → token fresco
try:
    st, body = http_json(LOGIN_URL, method="POST", data_obj={"username": USERNAME, "password": PASSWORD})
    if st != 200:
        print("LOGIN STATUS", st)
        print(body)
        sys.exit(1)

    o = json.loads(body)
    token = (o.get("access_token") or "").strip()
    if not token:
        print("ERROR: login did not return access_token")
        print(body)
        sys.exit(1)

except urllib.error.HTTPError as e:
    print("LOGIN STATUS", e.code)
    print(e.read().decode("utf-8", "replace"))
    sys.exit(1)
except Exception as e:
    print("LOGIN ERROR", repr(e))
    sys.exit(1)

# 2) Llamar cron endpoint con Bearer
try:
    st, body = http_json(CRON_URL, method="POST", headers={"Authorization": f"Bearer {token}"})
    print("STATUS", st)
    print(body)
    if st >= 400:
        sys.exit(1)

except urllib.error.HTTPError as e:
    print("STATUS", e.code)
    print(e.read().decode("utf-8", "replace"))
    sys.exit(1)
except Exception as e:
    print("ERROR", repr(e))
    sys.exit(1)
