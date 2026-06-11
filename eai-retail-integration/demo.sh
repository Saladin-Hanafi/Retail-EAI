#!/usr/bin/env bash
# End-to-end demo: one POS sale -> inventory deducted -> accounting entry posted.
set -e
GW=http://localhost:8000
echo "== Stock BEFORE (via gateway -> inventory, XML) =="
curl -s $GW/api/inventory/stock; echo
echo "== Journal BEFORE =="
curl -s $GW/api/accounting/journal; echo
echo "== POST a sale to POS (JSON) =="
curl -s -X POST $GW/api/pos/sales -H "Content-Type: application/json" \
  -d '{"sku":"SKU-1002","product_name":"Wireless Mouse","qty":3,"unit_price":150000,"cashier":"siti"}'; echo
echo "Waiting for async propagation..."; sleep 4
echo "== Stock AFTER (should be -3) =="
curl -s $GW/api/inventory/stock; echo
echo "== Journal AFTER (should have new revenue entry) =="
curl -s $GW/api/accounting/journal; echo
