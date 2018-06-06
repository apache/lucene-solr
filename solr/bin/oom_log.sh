#!/bin/sh

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

SOLR_PID="$1"
shift
SOLR_PORT="$1"
shift
SOLR_LOGS_DIR="$1"
shift
NOW=$(date +"%F_%H-%M-%S")
(
echo "Logging OOM event for Solr process $SOLR_PID, port $SOLR_PORT"
echo "Process has been terminated"
) | tee $SOLR_LOGS_DIR/solr_oom_log-$SOLR_PORT-$NOW.log
