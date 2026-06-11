# Retail Enterprise Application Integration

Integrates three independent enterprise systems — **POS**, **Inventory**, and **Accounting** — so that a single business event (a completed sale) automatically flows across all of them. Built as containerized microservices with a RabbitMQ-based integration layer applying Enterprise Integration Patterns.

## Systems & data formats (heterogeneous on purpose)

| System | Role | Native format | Database |
|---|---|---|---|
| POS Service | Records sales | **JSON / REST** | `pos.db` (SQLite) |
| Inventory Service | Stock levels | **XML** | `inventory.db` (SQLite) |
| Accounting Service | Journal ledger | **JSON, account-centric schema** (debit/credit/account_code) | `accounting.db` (SQLite) |

Each system owns its database. No system reads another system's database — all cross-system flow goes through the integration layer.

## Integration layer

- **Integration Engine** — ingests POS events over HTTP, normalizes to a Canonical Data Model, publishes to RabbitMQ, and (as subscriber) translates + delivers to Inventory (XML) and Accounting (ledger JSON). Only this component calls downstream APIs.
- **API Gateway** — single synchronous entry point that routes external calls to each system.
- **RabbitMQ** — fanout exchange (publish-subscribe) with durable queues and a dead-letter queue.

## Enterprise Integration Patterns implemented

1. **Canonical Data Model** — internal `CanonicalSaleEvent` decouples all systems.
2. **Content-Based Router** — routes by `event_type`.
3. **Message Translator** — canonical → XML (Inventory), canonical → ledger JSON (Accounting).
4. **Channel Adapter / Message Endpoint** — HTTP ingest → messaging.
5. **Publish-Subscribe Channel** — fanout to inventory + accounting queues.
6. **Dead-Letter Channel** + retry + **idempotency** (correlation_id) — reliable messaging.

## Run it (one command)

```bash
cp .env.example .env        # optional: set broker credentials
docker compose up --build
```

Wait until all containers are healthy, then:

```bash
./demo.sh
```

## Key endpoints (via gateway, http://localhost:8000)

| Method | Path | Description |
|---|---|---|
| GET | `/health` | Gateway + all backends |
| POST | `/api/pos/sales` | Record a sale (JSON) → triggers end-to-end flow |
| GET | `/api/pos/sales` | List sales (JSON) |
| GET | `/api/inventory/stock` | Stock levels (XML) |
| GET | `/api/accounting/journal` | Journal entries (JSON) |

RabbitMQ dashboard (observability): http://localhost:15672

## End-to-end flow

```
POST /api/pos/sales  (JSON)
  → POS stores sale, emits SaleCompleted to Integration Engine
    → Engine builds Canonical event, publishes to RabbitMQ (fanout)
      → inventory.sale queue → translate to XML → Inventory deducts stock
      → accounting.sale queue → map to ledger JSON → Accounting posts entry
```

Seeded SKUs: `SKU-1001` USB-C Cable (100), `SKU-1002` Wireless Mouse (50), `SKU-1003` Mechanical Keyboard (30).

## Repo layout

```
apps/pos-service/         JSON/REST sales system
apps/inventory-service/   XML stock system
apps/accounting-service/  JSON ledger system
integration/integration-engine/   canonical model, router, translators, broker I/O
integration/api-gateway/  single entry point
docs/architecture.svg     diagram with EIP labels
docs/openapi.yaml         API + payload transformation examples
docker-compose.yml        one-command orchestration
demo.sh                   end-to-end demo script
```
