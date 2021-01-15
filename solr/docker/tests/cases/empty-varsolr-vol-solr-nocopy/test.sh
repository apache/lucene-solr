#!/bin/bash
#
set -euo pipefail

TEST_DIR="${TEST_DIR:-$(dirname -- "${BASH_SOURCE[0]}")}"
source "${TEST_DIR}/../../shared.sh"

container_cleanup "$container_name-copier"

myvarsolr="myvarsolr-${container_name}"

docker volume rm "$myvarsolr" >/dev/null 2>&1 || true
docker volume create "$myvarsolr"

# with nocopy, the /var/solr ends up owned by root, so we need to chown it first

docker run \
  -v "$myvarsolr:/var/solr:nocopy" \
  --rm \
  -e NO_INIT_VAR_SOLR=yes \
  -u "0:0" \
  "$tag" bash -c "chown 8983:8983 /var/solr"

echo "Running $container_name"
docker run \
  -v "$myvarsolr:/var/solr:nocopy" \
  --name "$container_name" \
  -d "$tag" solr-precreate getting-started

wait_for_container_and_solr "$container_name"

echo "Loading data"
docker exec --user=solr "$container_name" bin/post -c getting-started example/exampledocs/manufacturers.xml
sleep 1
echo "Checking data"
data=$(docker exec --user=solr "$container_name" wget -q -O - 'http://localhost:8983/solr/getting-started/select?q=id%3Adell')
if ! grep -E -q 'One Dell Way Round Rock, Texas 78682' <<<"$data"; then
  echo "Test $TEST_NAME $tag failed; data did not load"
  exit 1
fi

docker exec --user=solr "$container_name" ls -l /var/solr/data

container_cleanup "$container_name"

docker volume rm "$myvarsolr"

echo "Test $TEST_NAME $tag succeeded"
