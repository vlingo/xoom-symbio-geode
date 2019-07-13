#!/bin/bash

mkdir -p /app/$HOSTNAME
gfsh start server --name=$HOSTNAME --locators=locator[10334] --dir=/app/$HOSTNAME/ "$@"
while true; do
    sleep 10
  done
done
