"""
Inventory / Warehouse Service.
Data format: XML (consumes and returns XML payloads) — heterogeneous on purpose.
Own database (SQLite). Receives stock-deduction commands from the integration
layer as XML. Never reads POS or Accounting databases.
"""
import os
import sqlite3
import datetime
import xml.etree.ElementTree as ET
from flask import Flask, request, Response

app = Flask(__name__)

DB_PATH = os.environ.get("INVENTORY_DB_PATH", "/data/inventory.db")
SERVICE_NAME = os.environ.get("SERVICE_NAME", "inventory-service")


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_db()
    conn.execute(
        """CREATE TABLE IF NOT EXISTS stock (
            sku TEXT PRIMARY KEY,
            product_name TEXT NOT NULL,
            quantity INTEGER NOT NULL,
            updated_at TEXT NOT NULL
        )"""
    )
    # seed
    seed = [
        ("SKU-1001", "USB-C Cable", 100),
        ("SKU-1002", "Wireless Mouse", 50),
        ("SKU-1003", "Mechanical Keyboard", 30),
    ]
    for sku, name, qty in seed:
        conn.execute(
            "INSERT OR IGNORE INTO stock (sku, product_name, quantity, updated_at) VALUES (?,?,?,?)",
            (sku, name, qty, datetime.datetime.utcnow().isoformat() + "Z"),
        )
    conn.commit()
    conn.close()


def stock_to_xml():
    conn = get_db()
    rows = conn.execute("SELECT * FROM stock ORDER BY sku").fetchall()
    conn.close()
    root = ET.Element("StockList")
    for r in rows:
        item = ET.SubElement(root, "Item")
        ET.SubElement(item, "Sku").text = r["sku"]
        ET.SubElement(item, "ProductName").text = r["product_name"]
        ET.SubElement(item, "Quantity").text = str(r["quantity"])
        ET.SubElement(item, "UpdatedAt").text = r["updated_at"]
    return ET.tostring(root, encoding="unicode")


@app.route("/health")
def health():
    return Response(
        f"<Health><Service>{SERVICE_NAME}</Service><Status>ok</Status></Health>",
        mimetype="application/xml",
    )


@app.route("/stock", methods=["GET"])
def list_stock():
    return Response(stock_to_xml(), mimetype="application/xml")


@app.route("/stock/deduct", methods=["POST"])
def deduct_stock():
    """
    Accepts an XML command:
      <StockDeduction><Sku>...</Sku><Quantity>...</Quantity></StockDeduction>
    Idempotency: the integration layer attaches a correlation id; we record it
    to avoid double-applying the same business event.
    """
    raw = request.get_data(as_text=True)
    try:
        root = ET.fromstring(raw)
    except ET.ParseError:
        return Response("<Error>Invalid XML</Error>", status=400, mimetype="application/xml")

    sku = root.findtext("Sku")
    qty = int(root.findtext("Quantity", "0"))
    corr_id = root.findtext("CorrelationId", "")

    conn = get_db()
    # idempotency ledger
    conn.execute(
        """CREATE TABLE IF NOT EXISTS applied_events (
            correlation_id TEXT PRIMARY KEY, applied_at TEXT)"""
    )
    if corr_id:
        already = conn.execute(
            "SELECT 1 FROM applied_events WHERE correlation_id=?", (corr_id,)
        ).fetchone()
        if already:
            conn.close()
            return Response(
                "<Result><Status>duplicate-ignored</Status></Result>",
                mimetype="application/xml",
            )

    row = conn.execute("SELECT quantity FROM stock WHERE sku=?", (sku,)).fetchone()
    if row is None:
        conn.close()
        return Response(
            f"<Error>Unknown SKU {sku}</Error>", status=404, mimetype="application/xml"
        )

    new_qty = max(0, row["quantity"] - qty)
    now = datetime.datetime.utcnow().isoformat() + "Z"
    conn.execute("UPDATE stock SET quantity=?, updated_at=? WHERE sku=?", (new_qty, now, sku))
    if corr_id:
        conn.execute(
            "INSERT INTO applied_events (correlation_id, applied_at) VALUES (?,?)",
            (corr_id, now),
        )
    conn.commit()
    conn.close()

    return Response(
        f"<Result><Sku>{sku}</Sku><NewQuantity>{new_qty}</NewQuantity><Status>ok</Status></Result>",
        mimetype="application/xml",
    )


if __name__ == "__main__":
    init_db()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5002)))
