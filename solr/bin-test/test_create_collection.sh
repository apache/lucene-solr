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
source bin-test/utils/cleanup.sh


# All tests should start with solr_test

function solr_suite_before() {
  bin/solr stop -all > /dev/null 2>&1
  bin/solr start -c > /dev/null 2>&1


  local source_configset_dir="server/solr/configsets/sample_techproducts_configs"
  TMP_CONFIGSET_DIR="/tmp/test_config"
  rm -rf $TMP_CONFIGSET_DIR; cp -r $source_configset_dir $TMP_CONFIGSET_DIR
}

function solr_suite_after() {
  bin/solr stop -all > /dev/null 2>&1
  rm -rf $TMP_CONFIGSET_DIR
}

function solr_unit_test_before() {
  delete_all_collections > /dev/null 2>&1
}

function solr_unit_test_after() {
  delete_all_collections > /dev/null 2>&1
}


function solr_test_can_create_collection() {
  local create_cmd="bin/solr create_collection -c COLL_NAME"
  local expected_output="Created collection 'COLL_NAME'"
  local actual_output; actual_output=$($create_cmd)

  assert_cmd_succeeded "$create_cmd" || return 1
  assert_output_contains "$actual_output" "$expected_output" || return 1
}

function solr_test_rejects_d_option_with_invalid_config_dir() {
  local create_cmd="bin/solr create_collection -c COLL_NAME -d /asdf"
  local expected_output="Specified configuration directory /asdf not found!"
  local actual_output; actual_output=$($create_cmd)

  assert_cmd_failed "$create_cmd" || return 1
  assert_output_contains "$actual_output" "$expected_output" || return 1
}

function solr_test_accepts_d_option_with_explicit_builtin_config() {
  local create_cmd="bin/solr create_collection -c COLL_NAME -d sample_techproducts_configs"
  local expected_output="Created collection 'COLL_NAME'"
  local actual_output; actual_output=$($create_cmd)

  assert_cmd_succeeded "$create_cmd" || return 1
  assert_output_contains "$actual_output" "$expected_output" || return 1
}

function solr_test_accepts_d_option_with_explicit_path_to_config() {
  local create_cmd="bin/solr create_collection -c COLL_NAME -d $TMP_CONFIGSET_DIR"
  local expected_output="Created collection 'COLL_NAME'"
  local actual_output; actual_output=$($create_cmd)

  assert_cmd_succeeded "$create_cmd" || return 1
  assert_output_contains "$actual_output" "$expected_output" || return 1
}

function solr_test_accepts_n_option_as_config_name() {
  local create_cmd="bin/solr create_collection -c COLL_NAME -n other_conf_name"
  local expected_name_output="Created collection 'COLL_NAME'"
  local expected_config_name_output="config-set 'other_conf_name'"
  local actual_output; actual_output=$($create_cmd)

  # Expect to fail, change to success
  assert_cmd_succeeded "$create_cmd" || return 1
  assert_output_contains "$actual_output" "$expected_name_output" || return 1
  assert_output_contains "$actual_output" "$expected_config_name_output" || return 1
}

function solr_test_allows_config_reuse_when_n_option_specifies_same_config() {
  local create_cmd1="bin/solr create_collection -c COLL_NAME_1 -n shared_config"
  local expected_coll_name_output1="Created collection 'COLL_NAME_1'"
  local create_cmd2="bin/solr create_collection -c COLL_NAME_2 -n shared_config"
  local expected_coll_name_output2="Created collection 'COLL_NAME_2'"
  local expected_config_name_output="config-set 'shared_config'"

  local actual_output1; actual_output1=$($create_cmd1)
  assert_cmd_succeeded "$create_cmd1" || return 1
  assert_output_contains "$actual_output1" "$expected_coll_name_output1" || return 1
  assert_output_contains "$actual_output1" "$expected_config_name_output" || return 1

  local actual_output2; actual_output2=$($create_cmd2)
  assert_cmd_succeeded "$create_cmd2" || return 1
  assert_output_contains "$actual_output2" "$expected_coll_name_output2" || return 1
  assert_output_contains "$actual_output2" "$expected_config_name_output" || return 1
}

function solr_test_create_multisharded_collections_when_s_provided() {
  local create_cmd="bin/solr create_collection -c COLL_NAME -s 2"
  local expected_coll_name_output="Created collection 'COLL_NAME'"
  local expected_shards_output="2 shard(s)"

  local actual_output; actual_output=$($create_cmd)
  assert_cmd_succeeded "$create_cmd" || return 1
  assert_output_contains "$actual_output" "$expected_coll_name_output" || return 1
  assert_output_contains "$actual_output" "$expected_shards_output" || return 1
}

function solr_test_creates_replicated_collections_when_r_provided() {
  local create_cmd="bin/solr create_collection -c COLL_NAME -rf 2"
  local expected_coll_name_output="Created collection 'COLL_NAME'"
  local expected_rf_output="2 replica(s)"

  local actual_output; actual_output=$($create_cmd)
  assert_cmd_succeeded "$create_cmd" || return 1
  assert_output_contains "$actual_output" "$expected_coll_name_output" || return 1
  assert_output_contains "$actual_output" "$expected_rf_output" || return 1
}
