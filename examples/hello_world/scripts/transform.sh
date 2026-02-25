#!/bin/bash
# granicus:
#   depends_on: [extract]

echo "Transforming data..."
if [ ! -f /tmp/hello_world/raw.json ]; then
    echo "ERROR: raw.json not found" >&2
    exit 1
fi
cat /tmp/hello_world/raw.json | while read line; do
    echo "$line" | sed 's/"name"/"customer_name"/'
done > /tmp/hello_world/transformed.json
echo "Transform complete: output written to /tmp/hello_world/transformed.json"
