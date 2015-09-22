#!/bin/bash
echo "Processing $1"
gunzip -c $1 > $2
curl -s -XPOST node304:9200/kb/doc/_bulk --data-binary @$2 > /dev/null
rm $2

