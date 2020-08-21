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

docker volume rm "$myvarsolr" >/dev/null 2>&1 || true
docker volume create "$myvarsolr"

# when we mount onto /var/solr, it will be owned by "solr", and it will copy
# the solr-owned directories and files from the container filesystem onto the
# the container. So from a container running as solr, modify permissions with
# setfacl to allow our user to write.
# If you don't have setfacl then run as root and do: chown -R $(id -u):$(id -g) /var/solr
docker run \
  -v "$myvarsolr:/var/solr" \
  --rm \
  "$tag" bash -c "setfacl -R -m u:$(id -u):rwx /var/solr"

echo "Running $container_name as $(id -u):$(id -g)"
docker run \
  -v "$myvarsolr:/var/solr" \
  --name "$container_name" \
  -u "$(id -u):$(id -g)" \
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

container_cleanup "$container_name"

docker volume rm "$myvarsolr"

echo "Test $TEST_DIR $tag succeeded"
