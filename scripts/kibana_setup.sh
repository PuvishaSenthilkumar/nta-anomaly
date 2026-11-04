#!/usr/bin/env bash
# Create the Kibana data view for the scored flows (run once Elasticsearch has at least one nta-flows-* index).
set -euo pipefail
curl -sS -X POST "http://localhost:5601/api/data_views/data_view" \
  -H 'kbn-xsrf: true' -H 'Content-Type: application/json' \
  -d '{"data_view":{"title":"nta-flows-*","name":"NTA scored flows","timeFieldName":"@timestamp"}}'
echo
echo "Open http://localhost:5601/app/discover"
