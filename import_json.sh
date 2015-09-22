#!/bin/bash
for f in *.gz;
do
  echo "Processing $f"
  gunzip -c $f > requests
  curl -s -XPOST localhost:9200/kb/doc/_bulk --data-binary @requests > /dev/null
done
