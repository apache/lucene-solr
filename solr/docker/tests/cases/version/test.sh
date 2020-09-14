#!/bin/bash
#
set -euo pipefail

TEST_DIR="${TEST_DIR:-$(dirname -- "${BASH_SOURCE[0]}")}"
source "${TEST_DIR}/../../shared.sh"

echo "Running $container_name"
docker run --name "$container_name" -d "$tag"

wait_for_server_started "$container_name"

echo "Checking that the OS matches the tag '$tag'"
if echo "$tag" | grep -q -- -alpine; then
  alpine_version=$(docker exec --user=solr "$container_name" cat /etc/alpine-release || true)
  if [[ -z $alpine_version ]]; then
    echo "Could not get alpine version from container $container_name"
    container_cleanup "$container_name"
    exit 1
  fi
  echo "Alpine $alpine_version"
else
  debian_version=$(docker exec --user=solr "$container_name" cat /etc/debian_version || true)
  if [[ -z $debian_version ]]; then
    echo "Could not get debian version from container $container_name"
    container_cleanup "$container_name"
    exit 1
  fi
  echo "Debian $debian_version"
fi

# check that the version of Solr matches the tag
changelog_version=$(docker exec --user=solr "$container_name" bash -c "grep -E '^==========* ' /opt/solr/CHANGES.txt | head -n 1 | tr -d '= '")
echo "Solr version $changelog_version"
solr_version_from_tag=$(echo "$tag" | sed -e 's/^.*://' -e 's/-.*//')

if [[ $changelog_version != "$solr_version_from_tag" ]]; then
  echo "Solr version mismatch"
  container_cleanup "$container_name"
  exit 1
fi

container_cleanup "$container_name"

echo "Test $TEST_NAME $tag succeeded"
