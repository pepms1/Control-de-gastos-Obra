import os
import sys
import json
import urllib.request
import urllib.error
from urllib.parse import quote

BASE = os.environ.get("CRON_BASE", "https://control-de-gastos-obra.onrender.com").rstrip("/")
PROJECT = os.environ.get("CRON_PROJECT", "CALDERON DE LA BARCA")
LOGIN_URL = BASE + "/api/auth/login"
CRON_URL = BASE + "/api/cron/import/sap-latest?project=" + quote(PROJECT)

USERNAME = os.environ.get("CRON_USERNAME", "")
PASSWORD = os.environ.get("CRON_PASSWORD", "")

def die(msg: str, code: int = 1):
    print(msg)
    sys.exit(code)

print("CRON START")
print("PROJECT", PROJECT)
print("LOGIN_URL", LOGIN_URL)
print("CRON_URL", CRON_URL)

if not USERNAME or not PASSWORD:
    die("Missing CRON_USERNAME / CRON_PASSWORD env vars")

# Login -> token
payload = json.dumps({"username": USERNAME, "password": PASSWORD}).encode("utf-8")
req = urllib.request.Request(
    LOGIN_URL,
    method="POST",
    data=payload,
    headers={"Content-Type": "application/json"},
)

try:
    with urllib.request.urlopen(req, timeout=60) as resp:
        body = resp.read().decode("utf-8", "replace")
        data = json.loads(body)
        token = data.get("access_token", "")
        print("LOGIN_STATUS", resp.status, "TOKEN_LEN", len(token))
        if not token:
            die("Login ok but access_token missing:\n" + body)
except urllib.error.HTTPError as e:
    die(f"LOGIN_STATUS {e.code}\n{e.read().decode('utf-8','replace')}")
except Exception as e:
    die("LOGIN_ERROR " + repr(e))

# Call cron endpoint with bearer
headers = {"Authorization": "Bearer " + token}
req2 = urllib.request.Request(CRON_URL, method="POST", headers=headers)

try:
    with urllib.request.urlopen(req2, timeout=600) as resp2:
        out = resp2.read().decode("utf-8", "replace")
        print("STATUS", resp2.status)
        print(out)
except urllib.error.HTTPError as e:
    print("STATUS", e.code)
    print("RNDR-ID", e.headers.get("rndr-id", ""))
    print(e.read().decode("utf-8", "replace"))
    sys.exit(1)
except Exception as e:
    die("ERROR " + repr(e))
