import os
import sys
import urllib.error
import urllib.request
import urllib.parse

BASE = os.environ.get("BASE", "https://control-de-gastos-obra.onrender.com").rstrip("/")

# Proyecto: primero env var, luego argv, si no -> default (Calderón)
project = os.environ.get("CRON_PROJECT") or (sys.argv[1] if len(sys.argv) > 1 else "CALDERON DE LA BARCA")

# endpoint
URL = f"{BASE}/api/cron/import/sap-latest?project={urllib.parse.quote(project, safe='')}"

secret = os.environ.get("CRON_SECRET", "")
headers = {"X-Cron-Secret": secret} if secret else {}

print("CRON START")
print("PROJECT", project)
print("URL", URL)

req = urllib.request.Request(URL, method="POST", headers=headers)

try:
    with urllib.request.urlopen(req, timeout=600) as response:
        body = response.read().decode("utf-8", "replace")
        print("STATUS", response.status)
        print(body)
except urllib.error.HTTPError as error:
    print("STATUS", error.code)
    print(error.read().decode("utf-8", "replace"))
    sys.exit(1)
except Exception as error:
    print("ERROR", repr(error))
    sys.exit(1)
