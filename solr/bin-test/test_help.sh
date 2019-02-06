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

function solr_test_start_help_flag_prints_help() {
  local help_cmd="bin/solr start -help"
  local expected_output="Usage: solr start"
  local actual_output; actual_output=$($help_cmd)

  assert_cmd_succeeded "$help_cmd" || return 1
  assert_output_contains "$actual_output" "$expected_output" || return 1
  assert_output_not_contains "$actual_output" "ERROR" || return 1
}

function solr_test_stop_help_flag_prints_help() {
  local help_cmd="bin/solr stop -help"
  local expected_output="Usage: solr stop"
  local actual_output; actual_output=$($help_cmd)

  assert_cmd_succeeded "$help_cmd" || return 1
  assert_output_contains "$actual_output" "$expected_output" || return 1
  assert_output_not_contains "$actual_output" "ERROR" || return 1
}

function solr_test_restart_help_flag_prints_help() {
  local help_cmd="bin/solr restart -help"
  local expected_output="Usage: solr restart"
  local actual_output; actual_output=$($help_cmd)

  assert_cmd_succeeded "$help_cmd" || return 1
  assert_output_contains "$actual_output" "$expected_output" || return 1
  assert_output_not_contains "$actual_output" "ERROR" || return 1
}

function solr_test_status_help_flag_prints_help() {
  #TODO Currently the status flag doesn't return nice help text!
  return 0
}

function solr_test_healthcheck_help_flag_prints_help() {
  local help_cmd="bin/solr healthcheck -help"
  local expected_output="Usage: solr healthcheck"
  local actual_output; actual_output=$($help_cmd)

  assert_cmd_succeeded "$help_cmd" || return 1
  assert_output_contains "$actual_output" "$expected_output" || return 1
  assert_output_not_contains "$actual_output" "ERROR" || return 1
}

function solr_test_create_help_flag_prints_help() {
  local help_cmd="bin/solr create -help"
  local expected_output="Usage: solr create"
  local actual_output; actual_output=$($help_cmd)

  assert_cmd_succeeded "$help_cmd" || return 1
  assert_output_contains "$actual_output" "$expected_output" || return 1
  assert_output_not_contains "$actual_output" "ERROR" || return 1
}

function solr_test_create_core_help_flag_prints_help() {
  local help_cmd="bin/solr create_core -help"
  local expected_output="Usage: solr create_core"
  local actual_output; actual_output=$($help_cmd)

  assert_cmd_succeeded "$help_cmd" || return 1
  assert_output_contains "$actual_output" "$expected_output" || return 1
  assert_output_not_contains "$actual_output" "ERROR" || return 1
}

function solr_test_create_collection_help_flag_prints_help() {
  local help_cmd="bin/solr create_collection -help"
  local expected_output="Usage: solr create_collection"
  local actual_output; actual_output=$($help_cmd)

  assert_cmd_succeeded "$help_cmd" || return 1
  assert_output_contains "$actual_output" "$expected_output" || return 1
  assert_output_not_contains "$actual_output" "ERROR" || return 1
}

function solr_test_delete_help_flag_prints_help() {
  local help_cmd="bin/solr delete -help"
  local expected_output="Usage: solr delete"
  local actual_output; actual_output=$($help_cmd)

  assert_cmd_succeeded "$help_cmd" || return 1
  assert_output_contains "$actual_output" "$expected_output" || return 1
  assert_output_not_contains "$actual_output" "ERROR" || return 1
}

function solr_test_version_help_flag_prints_help() {
  #TODO Currently the version -help flag doesn't return nice help text!
  return 0
}

function solr_test_zk_help_flag_prints_help() {
  local help_cmd="bin/solr zk -help"
  local expected_output="Usage: solr zk"
  local actual_output; actual_output=$($help_cmd)

  assert_cmd_succeeded "$help_cmd" || return 1
  assert_output_contains "$actual_output" "$expected_output" || return 1
  assert_output_not_contains "$actual_output" "ERROR" || return 1
}

function solr_test_auth_help_flag_prints_help() {
  local help_cmd="bin/solr auth -help"
  local expected_output="Usage: solr auth"
  local actual_output; actual_output=$($help_cmd)

  assert_cmd_succeeded "$help_cmd" || return 1
  assert_output_contains "$actual_output" "$expected_output" || return 1
  assert_output_not_contains "$actual_output" "ERROR" || return 1
}

function solr_test_assert_help_flag_prints_help() {
  #TODO Currently the assert -help flag doesn't return nice help text!
  # It returns autogenerated SolrCLI help, which is similar but not _quite_
  # the same thing.
  return 0
}
