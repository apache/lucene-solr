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

source bin-test/utils/assert.sh

function solr_unit_test_before() {
  bin/solr auth disable > /dev/null 2>&1
}

function solr_test_auth_rejects_blockUnknown_option_with_invalid_boolean() {
  local auth_cmd="bin/solr auth enable -type basicAuth -credentials anyUser:anyPass -blockUnknown ture"
  local expected_output="Argument [blockUnknown] must be either true or false, but was [ture]"
  local actual_output; actual_output=$($auth_cmd)

  assert_cmd_failed "$auth_cmd" || return 1
  assert_output_contains "$actual_output" "$expected_output" || return 1
}

function solr_test_auth_rejects_updateIncludeFileOnly_option_with_invalid_boolean() {
  local auth_cmd="bin/solr auth enable -type basicAuth -credentials anyUser:anyPass -updateIncludeFileOnly ture"
  local expected_output="Argument [updateIncludeFileOnly] must be either true or false, but was [ture]"
  local actual_output; actual_output=$($auth_cmd)

  assert_cmd_failed "$auth_cmd" || return 1
  assert_output_contains "$actual_output" "$expected_output" || return 1
}

