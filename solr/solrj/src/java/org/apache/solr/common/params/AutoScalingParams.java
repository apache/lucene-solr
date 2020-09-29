/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.common.params;

/**
 * Requests parameters for autoscaling.
 *
 * @deprecated to be removed in Solr 9.0 (see SOLR-14656)
 */
public interface AutoScalingParams {

  // parameters
  String DIAGNOSTICS = "diagnostics";
  String SUGGESTIONS = "suggestions";
  String NAME = "name";
  String TRIGGER = "trigger";
  String EVENT = "event";
  String ACTIONS = "actions";
  String WAIT_FOR = "waitFor";
  String LOWER_BOUND = "lowerBound";
  String UPPER_BOUND = "upperBound";
  String STAGE = "stage";
  String CLASS = "class";
  String ENABLED = "enabled";
  String RESUME_AT = "resumeAt";
  String BEFORE_ACTION = "beforeAction";
  String AFTER_ACTION = "afterAction";
  String TIMEOUT = "timeout";
  String COLLECTION = "collection";
  String SHARD = "shard";
  String REPLICA = "replica";
  String NODE = "node";
  String HANDLER = "handler";
  String RATE = "rate";
  String REMOVE_LISTENERS = "removeListeners";
  String ZK_VERSION = "zkVersion";
  String METRIC = "metric";
  String ABOVE = "above";
  String BELOW = "below";
  String PREFERRED_OP = "preferredOperation";
  String REPLICA_TYPE = "replicaType";

  // commands
  String CMD_SET_TRIGGER = "set-trigger";
  String CMD_REMOVE_TRIGGER = "remove-trigger";
  String CMD_SET_LISTENER = "set-listener";
  String CMD_REMOVE_LISTENER = "remove-listener";
  String CMD_SUSPEND_TRIGGER = "suspend-trigger";
  String CMD_RESUME_TRIGGER = "resume-trigger";
  String CMD_SET_POLICY = "set-policy";
  String CMD_REMOVE_POLICY = "remove-policy";
  String CMD_SET_CLUSTER_PREFERENCES = "set-cluster-preferences";
  String CMD_SET_CLUSTER_POLICY = "set-cluster-policy";
  String CMD_SET_PROPERTIES = "set-properties";

  // properties
  String TRIGGER_SCHEDULE_DELAY_SECONDS = "triggerScheduleDelaySeconds";
  String TRIGGER_COOLDOWN_PERIOD_SECONDS = "triggerCooldownPeriodSeconds";
  String TRIGGER_CORE_POOL_SIZE = "triggerCorePoolSize";
  String MAX_COMPUTE_OPERATIONS = "maxComputeOperations";

  @Deprecated
  String ACTION_THROTTLE_PERIOD_SECONDS = "actionThrottlePeriodSeconds";
}
