#!/bin/bash

docker run \
  -v /var/run/docker.sock:/var/run/docker.sock \
  --network cbnetwork \
  --name reproducer \
  -d amoudi/ubuntu-java tail -f /dev/null

docker exec reproducer bash -c 'git clone https://github.com/amoudi87/cb-sdk-reproducer.git; cd cb-sdk-reproducer; mvn clean install'