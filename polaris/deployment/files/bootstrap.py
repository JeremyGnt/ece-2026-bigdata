import base64
import json
import os
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

BASE = os.environ["POLARIS_URI"].rstrip("/")
MGMT = os.environ["POLARIS_MGMT_URI"].rstrip("/")
CATALOG = os.environ["POLARIS_CATALOG"]
REALM = os.environ.get("POLARIS_REALM", "POLARIS")

for attempt in range(60):
    try:
        with urlopen(MGMT + "/q/health/ready", timeout=5):
            break
    except (HTTPError, URLError, TimeoutError):
        time.sleep(5)
else:
    raise RuntimeError("Polaris never became ready")

basic = base64.b64encode(
    (os.environ["POLARIS_CLIENT_ID"] + ":" +
     os.environ["POLARIS_ROOT_SECRET"]).encode()
).decode()

form = urlencode({
    "grant_type": "client_credentials",
    "scope": "PRINCIPAL_ROLE:ALL"
}).encode()

request = Request(
    BASE + "/api/catalog/v1/oauth/tokens",
    data=form,
    headers={
        "Authorization": "Basic " + basic,
        "Polaris-Realm": REALM
    },
    method="POST"
)

with urlopen(request, timeout=20) as response:
    token = json.load(response)["access_token"]

def api(method, path, payload=None):
    body = None if payload is None else json.dumps(payload).encode()
    request = Request(
        BASE + "/api/management/v1" + path,
        data=body,
        headers={
            "Authorization": "Bearer " + token,
            "Polaris-Realm": REALM,
            "Content-Type": "application/json"
        },
        method=method
    )
    try:
        with urlopen(request, timeout=30) as response:
            raw = response.read()
            return json.loads(raw) if raw else {}
    except HTTPError as exc:
        if method == "GET" and exc.code == 404:
            return None
        raise RuntimeError(
            f"{method} {path}: HTTP {exc.code}"
        ) from exc


def ensure(path, collection, payload, label):
    if api("GET", path) is not None:
        print(f"{label} already exists", flush=True)
        return None
    response = api("POST", collection, payload)
    print(f"{label} created", flush=True)
    return response

for name in ("dev", "analyst"):
    response = ensure(
        f"/principals/{name}", "/principals",
        {
            "principal": {"name": name, "properties": {}},
            "credentialRotationRequired": False
        },
        f"Principal {name}"
    )
    if response and response.get("credentials"):
        print(json.dumps({
            "principal": name,
            "credentials": response["credentials"]
        }), flush=True)

for role in ("data_engineer", "data_analyst"):
    ensure(
        f"/principal-roles/{role}",
        "/principal-roles",
        {"principalRole": {"name": role, "properties": {}}},
        f"Principal role {role}"
    )

for name in ("dev", "analyst"):
    response = ensure(
        f"/principals/{name}", "/principals",
        {
            "principal": {"name": name, "properties": {}},
            "credentialRotationRequired": False
        },
        f"Principal {name}"
    )
    if response and response.get("credentials"):
        print(json.dumps({
            "principal": name,
            "credentials": response["credentials"]
        }), flush=True)

for role in ("data_engineer", "data_analyst"):
    ensure(
        f"/principal-roles/{role}",
        "/principal-roles",
        {"principalRole": {"name": role, "properties": {}}},
        f"Principal role {role}"
    )

location = os.environ["POLARIS_BASE_LOCATION"]
endpoint = os.environ["S3_ENDPOINT"]

ensure(
    f"/catalogs/{CATALOG}",
    "/catalogs",
    {
        "catalog": {
            "name": CATALOG,
            "type": "INTERNAL",
            "properties": {
                "default-base-location": location
            },
            "storageConfigInfo": {
                "storageType": "S3",
                "allowedLocations": [location],
                "endpoint": endpoint,
                "endpointInternal": endpoint,
                "pathStyleAccess": True,
                "region": "us-east-1",
                "stsUnavailable": True
            }
        }
    },
    f"Catalog {CATALOG}"
)

for role in ("catalog_contributor", "catalog_reader"):
    ensure(
        f"/catalogs/{CATALOG}/catalog-roles/{role}",
        f"/catalogs/{CATALOG}/catalog-roles",
        {"catalogRole": {"name": role, "properties": {}}},
        f"Catalog role {role}"
    )

for role, privileges in (
    ("catalog_contributor", (
        "CATALOG_MANAGE_ACCESS",
        "CATALOG_MANAGE_METADATA",
        "CATALOG_MANAGE_CONTENT"
    )),
    ("catalog_reader", (
        "TABLE_READ_DATA",
        "TABLE_READ_PROPERTIES",
        "CATALOG_READ_PROPERTIES"
    ))
):
    path = f"/catalogs/{CATALOG}/catalog-roles/{role}/grants"
    current = api("GET", path).get("grants", [])
    for privilege in privileges:
        if any(
            g.get("type") == "catalog"
            and g.get("privilege") == privilege
            for g in current
        ):
            continue
        api("PUT", path, {
            "grant": {"type": "catalog", "privilege": privilege}
        })
        print(f"Granted {privilege} to {role}", flush=True)

for principal, principal_role in (
    ("dev", "data_engineer"),
    ("analyst", "data_analyst")
):
    path = f"/principals/{principal}/principal-roles"
    roles = api("GET", path).get("roles", [])
    if not any(r.get("name") == principal_role for r in roles):
        api("PUT", path, {
            "principalRole": {"name": principal_role}
        })

for principal_role, catalog_role in (
    ("data_engineer", "catalog_contributor"),
    ("data_analyst", "catalog_reader")
):
    path = (
        f"/principal-roles/{principal_role}"
        f"/catalog-roles/{CATALOG}"
    )
    roles = api("GET", path).get("roles", [])
    if not any(r.get("name") == catalog_role for r in roles):
        api("PUT", path, {
            "catalogRole": {"name": catalog_role}
        })

print("Polaris RBAC bootstrap completed successfully", flush=True)
