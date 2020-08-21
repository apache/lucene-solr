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

cd "$TEST_DIR"
initdb="initdb-$container_name"
prepare_dir_to_mount 8983 "$initdb"

cat > "$initdb/create-was-here.sh" <<EOM
touch /var/solr/initdb-was-here
EOM
cat > "$initdb/ignore-me" <<EOM
touch /var/solr/should-not-be
EOM

echo "Running $container_name"
docker run --name "$container_name" -d -e VERBOSE=yes -v "$PWD/$initdb:/docker-entrypoint-initdb.d" "$tag"

wait_for_server_started "$container_name"

echo "Checking initdb"
data=$(docker exec --user=solr "$container_name" ls /var/solr/initdb-was-here)
if [[ "$data" != /var/solr/initdb-was-here ]]; then
  echo "Test $TEST_DIR $tag failed; script did not run"
  exit 1
fi
data=$(docker exec --user=solr "$container_name" ls /var/solr/should-not-be; true)
if [[ -n "$data" ]]; then
  echo "Test $TEST_DIR $tag failed; should-not-be was"
  exit 1
fi
echo "Checking docker logs"
log=docker.log-$container_name
if ! docker logs "$container_name" >"$log" 2>&1; then
  echo "Could not get logs for $container_name"
  exit
fi
if ! grep -q 'ignoring /docker-entrypoint-initdb.d/ignore-me' "$log"; then
  echo "missing ignoring message"
  cat "$log"
  exit 1
fi
rm "$log"

rm -fr "$initdb"
container_cleanup "$container_name"

echo "Test $TEST_DIR $tag succeeded"
