#!/bin/sh

docker exec docker_couchbase1.host_1 \
couchbase-cli cluster-init \
-c couchbase1.host \
--cluster-username=Administrator \
--cluster-password=couchbase \
--cluster-ramsize=1024

sleep 10

echo "Importing beer-sample"
docker exec docker_couchbase1.host_1 \
  cbdocloader \
  -n couchbase1.host:8091 \
  -u Administrator -p couchbase \
  -b beer-sample \
  -s 100 \
  ./opt/couchbase/samples/beer-sample.zip

sleep 2

docker exec docker_couchbase1.host_1 \
  couchbase-cli server-add \
  -c couchbase1.host \
  -u Administrator -p couchbase \
  --server-add couchbase2.host \
  --server-add-username Administrator \
  --server-add-password couchbase

sleep 2

docker exec docker_couchbase1.host_1 \
  couchbase-cli rebalance \
  -c couchbase1.host \
  -u Administrator -p couchbase \
  --server-add couchbase3.host \
  --server-add-username Administrator \
  --server-add-password couchbase

sleep 10
