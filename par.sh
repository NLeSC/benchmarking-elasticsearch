#!/bin/bash
/home/hkf700/mp/scripts/parallel --jobs 3 --bar /home/hkf700/mp/scripts/processOne.sh {} {.} ::: *.gz
