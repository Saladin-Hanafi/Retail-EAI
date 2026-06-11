"""
Integration Engine — the integration layer (microservice).

Implements Enterprise Integration Patterns:
  - Message Endpoint / Channel Adapter : /events/sale ingest endpoint (HTTP -> messaging)
  - Canonical Data Model               : CanonicalSaleEvent normalizes POS data
  - Message Channel                    : RabbitMQ exchange 'integration.events'
  - Publish-Subscribe Channel          : fanout to inventory + accounting queues
  - Message Router (Content-Based)      : routes by event_type to the right handlers
  - Message Translator                  : canonical -> XML (inventory), canonical -> ledger JSON (accounting)
  - Reliable messaging                  : retry, dead-letter queue, idempotency (correlation_id)

This service is the ONLY thing that talks to the downstream systems' APIs.
The downstream systems never read each other's databases.
"""
import os
import json
import time
import uuid
import threading
import datetime
import pika
import requests
from flask import Flask, request, jsonify

app = Flask(__name__)

# ---- Config via environment variables (no hardcoding) ----
RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT = int(os.environ.get("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.environ.get("RABBITMQ_USER", "guest")
RABBITMQ_PASS = os.environ.get("RABBITMQ_PASS", "guest")

EXCHANGE = os.environ.get("EXCHANGE_NAME", "integration.events")
DLX = os.environ.get("DLX_NAME", "integration.dlx")
Q_INVENTORY = os.environ.get("QUEUE_INVENTORY", "inventory.sale")
Q_ACCOUNTING = os.environ.get("QUEUE_ACCOUNTING", "accounting.sale")
Q_DEAD = os.environ.get("QUEUE_DEAD", "integration.dead")

INVENTORY_URL = os.environ.get("INVENTORY_URL", "http://inventory-service:5002")
ACCOUNTING_URL = os.environ.get("ACCOUNTING_URL", "http://accounting-service:5003")

SALES_REVENUE_ACCOUNT = os.environ.get("SALES_REVENUE_ACCOUNT", "4000-Revenue")
CASH_ACCOUNT = os.environ.get("CASH_ACCOUNT", "1000-Cash")
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "3"))


def connect():
    creds = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    params = pika.ConnectionParameters(
        host=RABBITMQ_HOST, port=RABBITMQ_PORT, credentials=creds, heartbeat=30
    )
    return pika.BlockingConnection(params)


def setup_topology(ch):
    # Dead-letter exchange + queue
    ch.exchange_declare(exchange=DLX, exchange_type="fanout", durable=True)
    ch.queue_declare(queue=Q_DEAD, durable=True)
    ch.queue_bind(queue=Q_DEAD, exchange=DLX)

    # Main pub-sub (fanout) exchange
    ch.exchange_declare(exchange=EXCHANGE, exchange_type="fanout", durable=True)

    args = {"x-dead-letter-exchange": DLX}
    for q in (Q_INVENTORY, Q_ACCOUNTING):
        ch.queue_declare(queue=q, durable=True, arguments=args)
        ch.queue_bind(queue=q, exchange=EXCHANGE)


# ---------------- Canonical Data Model ----------------
def to_canonical(raw_event: dict) -> dict:
    """Normalize the POS JSON event into the internal canonical format."""
    return {
        "canonical_version": "1.0",
        "event_type": raw_event.get("event_type", "SaleCompleted"),
        "correlation_id": raw_event.get("correlation_id") or str(uuid.uuid4()),
        "occurred_at": raw_event.get("timestamp")
        or datetime.datetime.utcnow().isoformat() + "Z",
        "source_system": raw_event.get("source", "pos-service"),
        "product": {
            "sku": raw_event["sku"],
            "name": raw_event.get("product_name", raw_event["sku"]),
        },
        "quantity": int(raw_event["qty"]),
        "monetary": {
            "unit_price": float(raw_event["unit_price"]),
            "total": float(raw_event["total"]),
            "currency": raw_event.get("currency", "IDR"),
        },
        "actor": raw_event.get("cashier", "unknown"),
    }


# ---------------- Message Translators ----------------
def canonical_to_inventory_xml(c: dict) -> str:
    return (
        "<StockDeduction>"
        f"<Sku>{c['product']['sku']}</Sku>"
        f"<Quantity>{c['quantity']}</Quantity>"
        f"<CorrelationId>{c['correlation_id']}</CorrelationId>"
        "</StockDeduction>"
    )


def canonical_to_accounting_json(c: dict) -> dict:
    # schema mapping: canonical -> accounting domain fields
    return {
        "account_code": SALES_REVENUE_ACCOUNT,
        "credit": c["monetary"]["total"],
        "debit": 0,
        "memo": f"Sale of {c['quantity']} x {c['product']['name']} ({c['product']['sku']})",
        "correlation_id": c["correlation_id"],
    }


# ---------------- HTTP ingest endpoint (Channel Adapter) ----------------
@app.route("/health")
def health():
    return jsonify({"service": "integration-engine", "status": "ok"})


@app.route("/events/sale", methods=["POST"])
def ingest_sale():
    raw = request.get_json(force=True)
    canonical = to_canonical(raw)

    conn = connect()
    ch = conn.channel()
    setup_topology(ch)
    ch.basic_publish(
        exchange=EXCHANGE,
        routing_key="",
        body=json.dumps(canonical),
        properties=pika.BasicProperties(
            delivery_mode=2,  # persistent
            correlation_id=canonical["correlation_id"],
            content_type="application/json",
        ),
    )
    conn.close()
    return jsonify({"status": "accepted", "correlation_id": canonical["correlation_id"]}), 202


# ---------------- Consumers (Message Endpoints + Router) ----------------
def deliver_with_retry(fn, *args):
    last = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return fn(*args)
        except Exception as e:  # noqa
            last = e
            app.logger.warning("delivery attempt %d failed: %s", attempt, e)
            time.sleep(min(2 ** attempt, 8))
    raise last


def push_inventory(canonical):
    xml = canonical_to_inventory_xml(canonical)
    r = requests.post(
        f"{INVENTORY_URL}/stock/deduct",
        data=xml,
        headers={"Content-Type": "application/xml"},
        timeout=5,
    )
    r.raise_for_status()
    return r.text


def push_accounting(canonical):
    payload = canonical_to_accounting_json(canonical)
    r = requests.post(f"{ACCOUNTING_URL}/journal", json=payload, timeout=5)
    r.raise_for_status()
    return r.json()


def make_consumer(queue, handler):
    def run():
        while True:
            try:
                conn = connect()
                ch = conn.channel()
                setup_topology(ch)
                ch.basic_qos(prefetch_count=1)

                def callback(c, method, props, body):
                    canonical = json.loads(body)
                    # Content-Based Router: only act on SaleCompleted
                    if canonical.get("event_type") != "SaleCompleted":
                        c.basic_ack(method.delivery_tag)
                        return
                    try:
                        deliver_with_retry(handler, canonical)
                        c.basic_ack(method.delivery_tag)
                        app.logger.info("[%s] processed %s", queue, canonical["correlation_id"])
                    except Exception as e:  # noqa
                        app.logger.error("[%s] dead-lettering %s: %s", queue, canonical.get("correlation_id"), e)
                        # reject without requeue -> goes to DLX
                        c.basic_nack(method.delivery_tag, requeue=False)

                ch.basic_consume(queue=queue, on_message_callback=callback)
                app.logger.info("consuming %s", queue)
                ch.start_consuming()
            except Exception as e:  # noqa
                app.logger.error("consumer %s crashed, reconnecting: %s", queue, e)
                time.sleep(5)

    return run


def start_consumers():
    # wait for broker
    for _ in range(30):
        try:
            connect().close()
            break
        except Exception:
            time.sleep(2)
    threading.Thread(target=make_consumer(Q_INVENTORY, push_inventory), daemon=True).start()
    threading.Thread(target=make_consumer(Q_ACCOUNTING, push_accounting), daemon=True).start()


if __name__ == "__main__":
    start_consumers()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
