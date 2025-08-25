#!/bin/bash

URL="http://localhost:8767/flight"
DATA_FILE="scripts/data/data_flights.json"

jq -c '.[]' "$DATA_FILE" | while read -r flight; do
    curl -X POST -H "Content-Type: application/json" -d "$flight" "$URL"
done