#!/bin/bash
#
set -euo pipefail

TEST_DIR="${TEST_DIR:-$(dirname -- "${BASH_SOURCE[0]}")}"
source "${TEST_DIR}/../../shared.sh"

container_cleanup "$container_name-copier"

myvarsolr="${BUILD_DIR}/myvarsolr-${container_name}"
prepare_dir_to_mount 8983 "$myvarsolr"
mylogs="${BUILD_DIR}/mylogs-${container_name}"
prepare_dir_to_mount 8983 "$mylogs"
myconf="${BUILD_DIR}/myconf-${container_name}"
configsets="${BUILD_DIR}/configsets-${container_name}"

# create a core by hand:
rm -fr "$myconf" "$configsets" 2>/dev/null
docker create --name "$container_name-copier" "$tag"
docker cp "$container_name-copier:/opt/solr/server/solr/configsets" "$configsets"
docker rm "$container_name-copier"
for d in data_driven_schema_configs _default; do
  if [ -d "$configsets/$d" ]; then
    cp -r "$configsets/$d/conf" "$myconf"
    break
  fi
done
rm -fr "$configsets"
if [ ! -d "$myconf" ]; then
  echo "Could not get config"
  exit 1
fi
if [ ! -f "$myconf/solrconfig.xml" ]; then
  find "$myconf"
  echo "ERROR: no solrconfig.xml"
  exit 1
fi

# create a directory for the core
mkdir -p "$myvarsolr/data/mycore"
mkdir -p "$myvarsolr/logs"
touch "$myvarsolr/data/mycore/core.properties"

echo "Running $container_name"
docker run \
  -v "$myvarsolr:/var/solr" \
  -v "$myconf:/var/solr/data/mycore/conf:ro" \
  -v "$mylogs:/var/solr/logs" \
  --user "$(id -u):$(id -g)" \
  --name "$container_name" \
  -d "$tag"

wait_for_container_and_solr "$container_name"

echo "Loading data"
docker exec --user=solr "$container_name" bin/post -c mycore example/exampledocs/manufacturers.xml
sleep 1
echo "Checking data"
data=$(docker exec --user=solr "$container_name" wget -q -O - 'http://localhost:8983/solr/mycore/select?q=id%3Adell')
if ! grep -E -q 'One Dell Way Round Rock, Texas 78682' <<<"$data"; then
  echo "Test $TEST_NAME $tag failed; data did not load"
  exit 1
fi
container_cleanup "$container_name"

rm -fr "$myconf" "$myvarsolr" "$mylogs" "$configsets"

echo "Test $TEST_NAME $tag succeeded"
