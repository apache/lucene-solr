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
}

function solr_suite_after() {
  bin/solr stop -all > /dev/null 2>&1
}

function solr_unit_test_before() {
  delete_all_collections
}

function solr_unit_test_after() {
  delete_all_collections
}

function solr_test_can_delete_collection() {
  bin/solr create_collection -c COLL_NAME
  assert_collection_exists "COLL_NAME" || return 1

  bin/solr delete -c "COLL_NAME"
  assert_collection_doesnt_exist "COLL_NAME" || return 1
}

function solr_test_deletes_accompanying_zk_config_by_default() {
  bin/solr create_collection -c "COLL_NAME"
  assert_config_exists "COLL_NAME" || return 1

  bin/solr delete -c "COLL_NAME"
  assert_config_doesnt_exist "COLL_NAME" || return 1
}

function solr_test_deletes_accompanying_zk_config_with_nondefault_name() {
  bin/solr create_collection -c "COLL_NAME" -n "NONDEFAULT_CONFIG_NAME"
  assert_config_exists "NONDEFAULT_CONFIG_NAME" || return 1

  bin/solr delete -c "COLL_NAME"
  assert_config_doesnt_exist "NONDEFAULT_CONFIG_NAME"
}

function solr_test_deleteConfig_option_can_opt_to_leave_config_in_zk() {
  bin/solr create_collection -c "COLL_NAME"
  assert_config_exists "COLL_NAME"

  bin/solr delete -c "COLL_NAME" -deleteConfig false
  assert_config_exists "COLL_NAME"
}
