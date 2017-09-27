#!/bin/bash

echo Sleeping until Couchbase initializes...
while [ 1 ]; do
  curl -v -s -o/dev/null http://couchbase1.host:8091 && break
  sleep 5
done
echo couchbase1.host is up

while [ 1 ]; do
  curl -v -s -o/dev/null http://couchbase2.host:8091 && break
  sleep 5
done
echo couchbase2.host is up

while [ 1 ]; do
  curl -v -s -o/dev/null http://couchbase3.host:8091 && break
  sleep 5
done
echo couchbase3.host is up