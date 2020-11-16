#!/bin/bash
#
set -euo pipefail

TEST_DIR="${TEST_DIR:-$(dirname -- "${BASH_SOURCE[0]}")}"
source "${TEST_DIR}/../../shared.sh"

echo "Running $container_name"
docker run --name "$container_name" -d -e VERBOSE=yes \
  -e LOG4J_PROPS=/opt/solr/server/resources/log4j2.xml \
  -v "$TEST_DIR/log4j2.xml:/opt/solr/server/resources/log4j2.xml" \
  -v "$TEST_DIR/bogus-log4j2.xml:/var/solr/log4j2.xml" \
  "$tag" solr-precreate gettingstarted

wait_for_container_and_solr "$container_name"

echo "Loading data"
docker exec --user=solr "$container_name" bin/post -c gettingstarted example/exampledocs/manufacturers.xml
sleep 1
echo "Checking data"
data=$(docker exec --user=solr "$container_name" wget -q -O - 'http://localhost:8983/solr/gettingstarted/select?q=id%3Adell')
if ! grep -E -q 'One Dell Way Round Rock, Texas 78682' <<<"$data"; then
  echo "Test $TEST_NAME $tag failed; data did not load"
  exit 1
fi
data=$(docker exec --user=solr "$container_name" grep 'DEBUG (main)' /var/solr/logs/solr.log | wc -l)
if (( data == 0 )); then
  echo "missing DEBUG lines in the log"
  exit 1
fi

container_cleanup "$container_name"

echo "Test $TEST_NAME $tag succeeded"
