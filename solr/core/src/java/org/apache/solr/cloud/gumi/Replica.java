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

package org.apache.solr.cloud.gumi;

/**
 * An instantiation (or one of the copies) of a given {@link Shard} of a given {@link SolrCollection}.
 * Objects of this type are returned by the Solr framework to the plugin, they are not built by the plugin. When the
 * plugin wants to add a replica it goes through {@link WorkOrderFactory#createWorkOrderCreateReplica}).
 * TODO is there an elegant way to have this type also used by the plugin to add replicas? (insisting on elegant)
 */
public interface Replica extends PropertyKeyTarget {
  Shard getShard();

  ReplicaType getType();
  ReplicaState getState();

  // TODO: needed? Different?
  String getReplicaName();
  String getCoreName();
}
