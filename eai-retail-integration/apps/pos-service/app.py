"""
POS Service — Point of Sale.
Data format: JSON / REST.
Own database (SQLite). Publishes 'SaleCompleted' events to the message broker
via the integration layer (it calls the integration engine's ingest endpoint;
it never touches another system's DB directly).
"""
import os
import sqlite3
import json
import datetime
import requests
from flask import Flask, request, jsonify

app = Flask(__name__)

DB_PATH = os.environ.get("POS_DB_PATH", "/data/pos.db")
INTEGRATION_INGEST_URL = os.environ.get(
    "INTEGRATION_INGEST_URL", "http://integration-engine:8080/events/sale"
)
SERVICE_NAME = os.environ.get("SERVICE_NAME", "pos-service")


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_db()
    conn.execute(
        """CREATE TABLE IF NOT EXISTS sales (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sku TEXT NOT NULL,
            product_name TEXT NOT NULL,
            qty INTEGER NOT NULL,
            unit_price REAL NOT NULL,
            total REAL NOT NULL,
            cashier TEXT,
            created_at TEXT NOT NULL
        )"""
    )
    conn.commit()
    conn.close()


@app.route("/health")
def health():
    return jsonify({"service": SERVICE_NAME, "status": "ok"})


@app.route("/sales", methods=["GET"])
def list_sales():
    conn = get_db()
    rows = conn.execute("SELECT * FROM sales ORDER BY id DESC").fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@app.route("/sales", methods=["POST"])
def create_sale():
    """
    Records a sale and emits a SaleCompleted business event to the integration
    layer. This single event must trigger automatic updates in Inventory and
    Accounting (end-to-end flow).
    """
    body = request.get_json(force=True)
    sku = body["sku"]
    product_name = body.get("product_name", sku)
    qty = int(body["qty"])
    unit_price = float(body["unit_price"])
    total = round(qty * unit_price, 2)
    cashier = body.get("cashier", "anonymous")
    created_at = datetime.datetime.utcnow().isoformat() + "Z"

    conn = get_db()
    cur = conn.execute(
        """INSERT INTO sales (sku, product_name, qty, unit_price, total, cashier, created_at)
           VALUES (?,?,?,?,?,?,?)""",
        (sku, product_name, qty, unit_price, total, cashier, created_at),
    )
    conn.commit()
    sale_id = cur.lastrowid
    conn.close()

    # POS native format = JSON
    event = {
        "event_type": "SaleCompleted",
        "source": SERVICE_NAME,
        "sale_id": sale_id,
        "sku": sku,
        "product_name": product_name,
        "qty": qty,
        "unit_price": unit_price,
        "total": total,
        "cashier": cashier,
        "timestamp": created_at,
    }

    delivered = False
    try:
        resp = requests.post(INTEGRATION_INGEST_URL, json=event, timeout=5)
        delivered = resp.status_code in (200, 202)
    except requests.RequestException as e:
        app.logger.error("Failed to reach integration layer: %s", e)

    return (
        jsonify({"sale_id": sale_id, "sale": event, "forwarded_to_integration": delivered}),
        201,
    )


if __name__ == "__main__":
    init_db()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5001)))
