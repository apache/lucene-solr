#!/bin/bash
#
# A helper script to wait for solr
#
# Usage: wait-for-solr.sh [--max-attempts count] [--wait-seconds seconds] [--solr-url url]
# Deprecated usage: wait-for-solr.sh [ max_attempts [ wait_seconds ] ]

set -euo pipefail

SCRIPT="$0"

if [[ "${VERBOSE:-}" == "yes" ]]; then
    set -x
fi

function usage {
  echo "$1"
  echo "Usage: $SCRIPT [--max-attempts count] [--wait-seconds seconds ] [--solr-url url]"
  exit 1
}

max_attempts=12
wait_seconds=5

if [[ -v SOLR_PORT ]] && ! grep -E -q '^[0-9]+$' <<<"$SOLR_PORT"; then
  echo "Invalid SOLR_PORT=$SOLR_PORT environment variable specified"
  exit 1
fi

solr_url="http://localhost:${SOLR_PORT:-8983}"

while (( $# > 0 )); do
  case "$1" in
   --help)
     cat <<EOM
Usage: $SCRIPT [options]

Options:
  --max-attempts count: number of attempts to check Solr is up. Default: $max_attempts
  --wait-seconds seconds: number of seconds to wait between attempts. Default: $wait_seconds
  --solr-url url: URL for Solr server to check. Default: $solr_url
EOM
     exit 0
     ;;
   --solr-url)
     solr_url="$2";
     shift 2
     ;;

   --max-attempts)
     max_attempts="$2";
     shift 2;
     ;;

   --wait-seconds)
     wait_seconds="$2";
     shift 2;
     ;;

  * )
    # deprecated invocation, kept for backwards compatibility
    max_attempts=$1;
    wait_seconds=$2;
    echo "WARNING: deprecated invocation. Use $SCRIPT [--max-attempts count] [--wait-seconds seconds]"
    shift 2;
    break;
    ;;

  esac
done

grep -q -E '^[0-9]+$' <<<"$max_attempts" || usage "--max-attempts $max_attempts: not a number"
if (( max_attempts == 0 )); then
  echo "The --max-attempts argument should be >0"
  exit 1
fi
grep -q -E '^[0-9]+$' <<<"$wait_seconds" || usage "--wait-seconds $wait_seconds: not a number"
grep -q -E '^https?://' <<<"$solr_url" || usage "--solr-url $solr_url: not a URL"

((attempts_left=max_attempts))
while (( attempts_left > 0 )); do
  if wget -q -O - "$solr_url" | grep -i solr >/dev/null; then
    break
  fi
  (( attempts_left-- ))
  if (( attempts_left == 0 )); then
    echo "Solr is still not running; giving up"
    exit 1
  fi
  if (( attempts_left == 1 )); then
    attempts=attempt
  else
    attempts=attempts
  fi
  echo "Solr is not running yet on $solr_url. $attempts_left $attempts left"
  sleep "$wait_seconds"
done
echo "Solr is running on $solr_url"
