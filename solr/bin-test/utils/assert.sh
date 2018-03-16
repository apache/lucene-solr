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

ASSERT_SUCCEEDED=1
ASSERT_FAILURE=0

function assert_cmd_succeeded() {
  retval=$?
  
  if [[ $? -ne 0 ]]; then
    echo "Expected command $1 to succeed, but exited with $retval"
    return $ASSERT_FAILURE
  fi

  return $ASSERT_SUCCESS
}

function assert_cmd_failed() {
  retval=$?
  
  if [[ $? -eq 0 ]]; then
    echo "Expected command $1 to fail, but exited with $retval"
    return $ASSERT_FAILURE
  fi

  return $ASSERT_SUCCESS
}

function assert_output_contains() {
  local actual_output="$1"
  local needle="$2"

  if echo "$actual_output" | grep -q "$needle"; then
    return $ASSERT_SUCCESS
  fi

  echo "Expected to find "$needle" in output [$actual_output]"
  return $ASSERT_FAILURE
}

function assert_output_not_contains() {
  local actual_output="$1"
  local needle="$2"

  if echo "$actual_output" | grep -q "$needle"; then
    echo "Didn't expect to find "$needle" in output [$actual_output]"
    return $ASSERT_FAILURE
  fi

  return $ASSERT_SUCCESS
}

function assert_collection_exists() {
  local coll_name=$1
  local coll_list=$(bin/solr zk ls /collections -z localhost:9983)

  for coll in "$coll_list";
  do
    if [[ $(echo $coll | tr -d " ") -eq $coll_name ]]; then
      return $ASSERT_SUCCESS
    fi
  done

  echo "Expected to find collection named [$coll_name], but could only find: $coll_list"
  return $ASSERT_FAILURE
}

function assert_collection_doesnt_exist() {
  local coll_name=$1
  local coll_list=$(bin/solr zk ls /collections -z localhost:9983)
  for coll in "$coll_list";
  do
    if [[ $(echo $coll | tr -d " ") -eq $coll_name ]]; then
      echo "Expected not to find collection [$coll_name], but it exists"
      return $ASSERT_FAILURE
    fi
  done

  return $ASSERT_SUCCESS
}

function assert_config_exists() {
  local config_name=$1
  local config_list=$(bin/solr zk ls /configs -z localhost:9983)

  for config in "$config_list";
  do
    if [[ $(echo $config | tr -d " ") -eq $config_name ]]; then
      return $ASSERT_SUCCESS
    fi
  done

  echo "Expected to find config named [$config_name], but could only find: $config_list"
  return $ASSERT_FAILURE
}

function assert_config_doesnt_exist() {
  local config_name=$1
  local config_list=$(bin/solr zk ls /configs -z localhost:9983)

  for config in "$config_list";
  do
    if [[ $(echo $config | tr -d " ") -eq $config_name ]]; then
      echo "Expected not to find config [$config_name], but it exists"
      return $ASSERT_FAILURE
    fi
  done

  return $ASSERT_SUCCESS
}
