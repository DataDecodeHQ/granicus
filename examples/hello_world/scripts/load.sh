#!/bin/bash
# granicus:
#   depends_on: [transform]

echo "Loading transformed data..."
if [ ! -f /tmp/hello_world/transformed.json ]; then
    echo "ERROR: transformed.json not found" >&2
    exit 1
fi
wc -l /tmp/hello_world/transformed.json | awk '{print "Load complete: "$1" records loaded"}'
