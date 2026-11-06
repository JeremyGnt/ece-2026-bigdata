import os
import json
import base64
import shlex
import subprocess
from pathlib import Path
from urllib.request import Request, urlopen
from urllib.error import HTTPError
from urllib.parse import urlencode

BASE = os.environ.get("POLARIS_URI", "http://polaris:8181")
CATALOG = os.environ.get("POLARIS_CATALOG", "user-j-gonnet-ece")
API = BASE + "/api/management/v1"

encoded = subprocess.check_output([
    "kubectl", "get", "secret", "polaris-credential",
    "-o", "jsonpath={.data.POLARIS_ROOT_SECRET}"
])
root_password = base64.b64decode(encoded).decode()
basic = base64.b64encode(
    f"root:{root_password}".encode()
).decode()

req = Request(
    BASE + "/api/catalog/v1/oauth/tokens",
    data=urlencode({
        "grant_type": "client_credentials",
        "scope": "PRINCIPAL_ROLE:ALL"
    }).encode(),
    headers={
        "Authorization": "Basic " + basic,
        "Polaris-Realm": "POLARIS"
    }
)
with urlopen(req) as response:
    token = json.load(response)["access_token"]

def api(method, path, payload=None):
    body = json.dumps(payload).encode() if payload else None
    req = Request(
        API + path,
        data=body,
        method=method,
        headers={
            "Authorization": "Bearer " + token,
            "Polaris-Realm": "POLARIS",
            "Content-Type": "application/json"
        }
    )
    try:
        with urlopen(req, timeout=20) as response:
            raw = response.read()
            return json.loads(raw) if raw else {}
    except HTTPError as exc:
        if method == "GET" and exc.code == 404:
            return None
        raise RuntimeError(
            f"{method} {path}: HTTP {exc.code}"
        ) from exc


def ensure(path, collection, payload):
    existing = api("GET", path)
    return api("POST", collection, payload) if existing is None else None

result = ensure(
    "/principals/silver_tester",
    "/principals",
    {"principal": {
        "name": "silver_tester", "properties": {}
    }, "credentialRotationRequired": False}
)

secret_file = Path.home() / ".polaris-silver-reader.env"

if result:
    credentials = result["credentials"]
    value = (
        credentials["clientId"] + ":" +
        credentials["clientSecret"]
    )
    os.umask(0o077)
    secret_file.write_text(
        "export POLARIS_SILVER_READER_CREDENTIAL="
        + shlex.quote(value) + "\n"
    )
elif not secret_file.exists():
    raise RuntimeError("Existing principal credentials unavailable")

ensure(
    "/principal-roles/silver_reader_role",
    "/principal-roles",
    {"principalRole": {
        "name": "silver_reader_role", "properties": {}
    }}
)

role_path = f"/catalogs/{CATALOG}/catalog-roles/silver_reader"
ensure(
    role_path,
    f"/catalogs/{CATALOG}/catalog-roles",
    {"catalogRole": {
        "name": "silver_reader", "properties": {}
    }}
)

path = role_path + "/grants"
grants = api("GET", path).get("grants", [])

privileges = [
    ("catalog", "CATALOG_READ_PROPERTIES"),
    ("namespace", "NAMESPACE_LIST"),
    ("namespace", "NAMESPACE_READ_PROPERTIES"),
    ("namespace", "TABLE_LIST"),
    ("namespace", "TABLE_READ_PROPERTIES"),
    ("namespace", "TABLE_READ_DATA")
]

for scope, privilege in privileges:
    grant = {"type": scope, "privilege": privilege}
    if scope == "namespace":
        grant["namespace"] = ["silver"]

    if grant not in grants:
        api("PUT", path, {"grant": grant})
        print("Granted:", privilege, "on", scope)

path = "/principals/silver_tester/principal-roles"
roles = api("GET", path).get("roles", [])

if not any(r["name"] == "silver_reader_role" for r in roles):
    api("PUT", path, {
        "principalRole": {"name": "silver_reader_role"}
    })

path = (
    "/principal-roles/silver_reader_role"
    f"/catalog-roles/{CATALOG}"
)
roles = api("GET", path).get("roles", [])

if not any(r["name"] == "silver_reader" for r in roles):
    api("PUT", path, {
        "catalogRole": {"name": "silver_reader"}
    })

print("Silver reader RBAC configuration completed.")
