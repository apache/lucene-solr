#!/bin/bash
#
# A simple test of gosu. We create a myvarsolr, and chown it
#

if [[ "$OSTYPE" == "darwin"* ]]; then
  # TODO: Fix this test on Mac
  echo "WARNING: Ignoring test 'gosu' on macOS"
  exit 0
fi

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

cd "$TEST_DIR"
myvarsolr="myvarsolr-${container_name}"
prepare_dir_to_mount 8983 "$myvarsolr"

echo "Cleaning up left-over containers from previous runs"
container_cleanup "$container_name"

echo "Running $container_name"
docker run --user 0:0 --name "$container_name" -d -e VERBOSE=yes \
  -v "$PWD/$myvarsolr:/var/solr" "$tag" \
  bash -c "chown -R solr:solr /var/solr; touch /var/solr/root_was_here; exec gosu solr:solr solr-precreate gettingstarted"

wait_for_container_and_solr "$container_name"

echo "Loading data"
docker exec --user=solr "$container_name" bin/post -c gettingstarted example/exampledocs/manufacturers.xml
sleep 1
echo "Checking data"
data=$(docker exec --user=solr "$container_name" wget -q -O - 'http://localhost:8983/solr/gettingstarted/select?q=id%3Adell')
if ! grep -E -q 'One Dell Way Round Rock, Texas 78682' <<<"$data"; then
  echo "Test $TEST_DIR $tag failed; data did not load"
  exit 1
fi

# check test file was created by root
data=$(docker exec --user=root "$container_name" stat -c %U /var/solr/root_was_here )
if [[ "$data" == *'No such file or directory' ]]; then
  echo "Missing /var/solr/root_was_here"
  exit 1
fi
if [[ "$data" != root ]]; then
  echo "/var/solr/root_was_here is owned by $data"
  exit 1
fi

# check core is created by solr
data=$(docker exec --user=root "$container_name" stat -c %U /var/solr/data/gettingstarted/core.properties )
if [[ "$data" == *'No such file or directory' ]]; then
  echo "Missing /var/solr/data/gettingstarted/core.properties"
  exit 1
fi
if [[ "$data" != solr ]]; then
  echo "/var/solr/data/gettingstarted/core.properties is owned by $data"
  exit 1
fi

container_cleanup "$container_name"

# chown it back
docker run --rm --user 0:0 -d -e VERBOSE=yes \
  -v "$PWD/$myvarsolr:/myvarsolr" "$tag" \
  bash -c "chown -R $(id -u):$(id -g) /myvarsolr; ls -ld /myvarsolr"

rm -fr "$myvarsolr"

echo "Test $TEST_DIR $tag succeeded"
