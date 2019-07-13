#!/bin/bash

mkdir -p /app/$HOSTNAME
gfsh start locator --name=locator --dir=/app/$HOSTNAME/ "$@"
while true; do
    sleep 10
  done
done
