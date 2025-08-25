#!/bin/bash

URL="http://localhost:8769/customer"
DATA_FILE="scripts/data/data_customers.json"

jq -c '.[]' "$DATA_FILE" | while read -r customer; do
    curl -X POST -H "Content-Type: application/json" -d "$customer" "$URL"
done