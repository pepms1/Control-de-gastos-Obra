import os
import sys
import urllib.error
import urllib.request

BASE = "https://control-de-gastos-obra.onrender.com"
URL = BASE + "/api/cron/import/sap-latest"

secret = os.environ.get("CRON_SECRET", "")
headers = {"X-Cron-Secret": secret} if secret else {}

print("CRON START")
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
