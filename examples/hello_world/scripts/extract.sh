#!/bin/bash
# granicus:
#   produces: [raw_data]

echo "Extracting data from source..."
mkdir -p /tmp/hello_world
echo '{"id": 1, "name": "Alice"}' > /tmp/hello_world/raw.json
echo '{"id": 2, "name": "Bob"}' >> /tmp/hello_world/raw.json
echo "Extract complete: 2 records written to /tmp/hello_world/raw.json"
