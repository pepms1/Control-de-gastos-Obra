import os
import sys
import urllib.error
import urllib.request
import urllib.parse

BASE = "https://control-de-gastos-obra.onrender.com"

project = os.environ.get("CRON_PROJECT", "CALDERON DE LA BARCA")

# Puedes pegarle al endpoint cron o al admin, los dos piden Bearer según tu error.
URL = BASE + "/api/cron/import/sap-latest?project=" + urllib.parse.quote(project)

bearer = os.environ.get("CRON_BEARER_TOKEN", "").strip()
secret = os.environ.get("CRON_SECRET", "").strip()

headers = {}
if bearer:
    headers["Authorization"] = f"Bearer {bearer}"
if secret:
    headers["X-Cron-Secret"] = secret

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
