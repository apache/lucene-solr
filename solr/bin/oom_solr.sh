#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

SOLR_PORT=$1
SOLR_LOGS_DIR=$2
SOLR_PID=$(ps auxww | grep start.jar | grep $SOLR_PORT | grep -v grep | awk '{print $2}' | sort -r)
if [ -z "$SOLR_PID" ]; then
  echo "Couldn't find Solr process running on port $SOLR_PORT!"
  exit
fi
NOW=$(date +"%F_%H_%M_%S")
(
  echo "Running OOM killer script for process $SOLR_PID for Solr on port $SOLR_PORT"
  if [[ "$SOLR_PID" == 1 ]]; then
    # Under Docker when running as pid 1, SIGKILL is ignored, so use the default SIGTERM
    kill "$SOLR_PID"
  else
    # On a real system, or in a container with tini or similar, it is safe to SIGKILL
    kill -9 "$SOLR_PID"
  fi
  echo "Killed process $SOLR_PID"
) | tee $SOLR_LOGS_DIR/solr_oom_killer-$SOLR_PORT-$NOW.log
