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

package org.apache.solr.cluster.placement;

import java.util.Map;

/**
 * Shard in a {@link SolrCollection}, i.e. a subset of the data indexed in that collection.
 */
public interface Shard extends PropertyValueSource {
  String getShardName();

  SolrCollection getCollection();

  /**
   * <p>The {@link Replica} of this {@link Shard}.
   *
   * <p>The map is from {@link Replica#getReplicaName()} to {@link Replica} instance.
   */
  Map<String, Replica> getReplicas();

  /**
   * The current leader {@link Replica} of this {@link Shard}. Note that by the time this method returns the leader might
   * have changed. Also, if there's no leader for any reason (don't shoot the messenger), this method will return {@code null}.
   */
  Replica getLeader();

  ShardState getState();

  enum ShardState {
    ACTIVE, INACTIVE, CONSTRUCTION, RECOVERY, RECOVERY_FAILED
  }

}
