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

package org.apache.solr.client.solrj.cloud.autoscaling;

import java.util.function.Consumer;

/**
 * A Variable Type used in Autoscaling policy rules
 */
public interface VarType {
  boolean match(Object inputVal, Operand op, Object val, String name, Row row);

  void projectAddReplica(Cell cell, ReplicaInfo ri, Consumer<Row.OperationInfo> opCollector, boolean strictMode);

  void addViolatingReplicas(Suggestion.ViolationCtx ctx);

  default void getSuggestions(Suggestion.SuggestionCtx ctx) {
  }

  default Object computeValue(Policy.Session session, Clause.Condition condition, String collection, String shard, String node) {
    return condition.val;
  }

  int compareViolation(Violation v1, Violation v2);

  default void projectRemoveReplica(Cell cell, ReplicaInfo ri, Consumer<Row.OperationInfo> opCollector) {
  }
}
