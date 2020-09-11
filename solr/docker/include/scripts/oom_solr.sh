#!/bin/bash

# Custom oom handler loosely based on
# https://github.com/apache/lucene-solr/blob/master/solr/bin/oom_solr.sh
# See solr-forgeground for how to configure OOM behaviour

if [[ -z "${SOLR_LOGS_DIR:-}" ]]; then
    if [ -d /var/solr/logs ]; then
        SOLR_LOGS_DIR=/var/solr/logs
    elif [ -d /opt/solr/server/logs ]; then
        SOLR_LOGS_DIR=/opt/solr/server/logs
    else
        echo "Cannot determine SOLR_LOGS_DIR!"
        exit 1
    fi
fi
SOLR_PID=$(pgrep -f start.jar)
if [[ -z "$SOLR_PID" ]]; then
  echo "Couldn't find Solr process running!"
  exit
fi

NOW=$(date +"%F_%H_%M_%S")
(
echo "Running OOM killer script for Solr process $SOLR_PID"
if [[ "$SOLR_PID" == 1 ]]; then
  # under Docker, when running as pid 1, a SIGKILL is ignored,
  # so use the default SIGTERM
  kill "$SOLR_PID"
  sleep 2
  # if that hasn't worked, send SIGKILL
  kill -SIGILL "$SOLR_PID"
else
  # if we're running with `--init` or under tini or similar,
  # follow the upstream behaviour
  kill -9 "$SOLR_PID"
fi
) | tee "$SOLR_LOGS_DIR/solr_oom_killer-$SOLR_PORT-$NOW.log"
