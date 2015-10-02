#!/bin/sh
for f in "$1"/*.gz; do
  echo "Importing $f"
  gunzip -c $f |
    curl -s -XPOST localhost:9200/kb_20140605/doc/_bulk --data-binary @- > /dev/null
done
