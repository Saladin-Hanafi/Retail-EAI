"""
Accounting / Ledger Service.
Data format: JSON, but with a DIFFERENT field schema than POS (account-centric
naming: debit/credit, account_code, memo, amount) — requires field mapping in
the integration layer (schema translation, not just format translation).
Own database (SQLite).
"""
import os
import sqlite3
import datetime
from flask import Flask, request, jsonify

app = Flask(__name__)

DB_PATH = os.environ.get("ACCOUNTING_DB_PATH", "/data/accounting.db")
SERVICE_NAME = os.environ.get("SERVICE_NAME", "accounting-service")


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_db()
    conn.execute(
        """CREATE TABLE IF NOT EXISTS journal_entries (
            entry_id INTEGER PRIMARY KEY AUTOINCREMENT,
            account_code TEXT NOT NULL,
            debit REAL NOT NULL DEFAULT 0,
            credit REAL NOT NULL DEFAULT 0,
            memo TEXT,
            correlation_id TEXT,
            posted_at TEXT NOT NULL
        )"""
    )
    conn.commit()
    conn.close()


@app.route("/health")
def health():
    return jsonify({"service": SERVICE_NAME, "status": "ok"})


@app.route("/journal", methods=["GET"])
def list_entries():
    conn = get_db()
    rows = conn.execute("SELECT * FROM journal_entries ORDER BY entry_id DESC").fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@app.route("/journal", methods=["POST"])
def post_entry():
    """
    Accepts accounting-domain JSON:
      {account_code, debit, credit, memo, correlation_id}
    Idempotency via correlation_id.
    """
    body = request.get_json(force=True)
    corr_id = body.get("correlation_id", "")

    conn = get_db()
    if corr_id:
        dup = conn.execute(
            "SELECT 1 FROM journal_entries WHERE correlation_id=?", (corr_id,)
        ).fetchone()
        if dup:
            conn.close()
            return jsonify({"status": "duplicate-ignored", "correlation_id": corr_id}), 200

    now = datetime.datetime.utcnow().isoformat() + "Z"
    cur = conn.execute(
        """INSERT INTO journal_entries (account_code, debit, credit, memo, correlation_id, posted_at)
           VALUES (?,?,?,?,?,?)""",
        (
            body["account_code"],
            float(body.get("debit", 0)),
            float(body.get("credit", 0)),
            body.get("memo", ""),
            corr_id,
            now,
        ),
    )
    conn.commit()
    entry_id = cur.lastrowid
    conn.close()
    return jsonify({"entry_id": entry_id, "status": "posted"}), 201


if __name__ == "__main__":
    init_db()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5003)))
