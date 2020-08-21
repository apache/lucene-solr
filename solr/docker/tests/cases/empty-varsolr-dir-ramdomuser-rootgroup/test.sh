#!/bin/bash
#
set -euo pipefail

TEST_DIR="$(dirname -- "$(readlink -f "${BASH_SOURCE-$0}")")"

if (( $# == 0 )); then
  echo "Usage: ${BASH_SOURCE[0]} tag"
  exit
fi

tag=$1

if [[ -n "${DEBUG:-}" ]]; then
  set -x
fi

source "$TEST_DIR/../../shared.sh"

echo "Test $TEST_DIR $tag"
container_name='test_'$(echo "$tag" | tr ':/-' '_')
echo "Cleaning up left-over containers from previous runs"
container_cleanup "$container_name"
container_cleanup "$container_name-copier"

myvarsolr="myvarsolr-${container_name}"
prepare_dir_to_mount 7777 "$myvarsolr"

echo "Running $container_name"
docker run \
  --user 7777:0 \
  -v "$PWD/$myvarsolr:/var/solr" \
  --name "$container_name" \
  -d "$tag" solr-precreate getting-started

wait_for_container_and_solr "$container_name"

echo "Loading data"
docker exec --user=solr "$container_name" bin/post -c getting-started example/exampledocs/manufacturers.xml
sleep 1
echo "Checking data"
data=$(docker exec --user=solr "$container_name" wget -q -O - 'http://localhost:8983/solr/getting-started/select?q=id%3Adell')
if ! grep -E -q 'One Dell Way Round Rock, Texas 78682' <<<"$data"; then
  echo "Test $TEST_DIR $tag failed; data did not load"
  exit 1
fi

docker exec --user=7777 "$container_name" ls -l /var/solr/data

container_cleanup "$container_name"

# remove the solr-owned files from inside a container
docker run --rm -e VERBOSE=yes \
  --user root \
  -v "$PWD/$myvarsolr:/myvarsolr" "$tag" \
  bash -c "rm -fr /myvarsolr/*"

rm -fr "$myvarsolr"

echo "Test $TEST_DIR $tag succeeded"
