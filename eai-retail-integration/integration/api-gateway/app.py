"""
API Gateway — single synchronous entry point (API-led integration).

EIP: Message Router (path-based) in front of all backend systems. External
clients only talk to the gateway; it routes to POS, Inventory, Accounting, and
the integration engine. Endpoints/credentials come from environment variables.
"""
import os
import requests
from flask import Flask, request, Response, jsonify

app = Flask(__name__)

ROUTES = {
    "pos": os.environ.get("POS_URL", "http://pos-service:5001"),
    "inventory": os.environ.get("INVENTORY_URL", "http://inventory-service:5002"),
    "accounting": os.environ.get("ACCOUNTING_URL", "http://accounting-service:5003"),
    "integration": os.environ.get("INTEGRATION_URL", "http://integration-engine:8080"),
}


@app.route("/health")
def health():
    status = {}
    for name, base in ROUTES.items():
        try:
            r = requests.get(f"{base}/health", timeout=3)
            status[name] = "ok" if r.status_code == 200 else f"http-{r.status_code}"
        except requests.RequestException:
            status[name] = "unreachable"
    return jsonify({"gateway": "ok", "backends": status})


@app.route("/api/<service>/<path:subpath>", methods=["GET", "POST", "PUT", "DELETE"])
def proxy(service, subpath):
    base = ROUTES.get(service)
    if not base:
        return jsonify({"error": f"unknown service '{service}'"}), 404
    url = f"{base}/{subpath}"
    try:
        resp = requests.request(
            method=request.method,
            url=url,
            data=request.get_data(),
            headers={k: v for k, v in request.headers if k.lower() != "host"},
            params=request.args,
            timeout=10,
        )
        return Response(resp.content, status=resp.status_code,
                        content_type=resp.headers.get("Content-Type", "application/json"))
    except requests.RequestException as e:
        return jsonify({"error": "backend unreachable", "detail": str(e)}), 502


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
