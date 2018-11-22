#!/bin/bash
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


# All tests should start with solr_test

function solr_suite_before() {
  bin/solr stop -all > /dev/null 2>&1
}

function solr_suite_after() {
  bin/solr stop -all > /dev/null 2>&1
}

function solr_test_11740_checks_f() {
  # SOLR-11740
  bin/solr start
  bin/solr start -p 7574
  bin/solr stop -all 2>&1 | grep -i "forcefully killing"
  rcode=$?
  if [[ $rcode -eq 0 ]]
  then
    echo "Unexpected forceful kill - please check."
    return 2
  fi
}
