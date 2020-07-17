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
package org.apache.solr.cloud.api.collections.assign;

import java.util.Locale;

/**
 *
 */
public enum ReplicaType {
  /**
   * Writes updates to transaction log and indexes locally. Replicas of type {@link ReplicaType#NRT} support NRT (soft commits) and RTG.
   * Any {@link ReplicaType#NRT} replica can become a leader. A shard leader will forward updates to all active {@link ReplicaType#NRT} and
   * {@link ReplicaType#TLOG} replicas.
   */
  NRT,
  /**
   * Writes to transaction log, but not to index, uses replication. Any {@link ReplicaType#TLOG} replica can become leader (by first
   * applying all local transaction log elements). If a replica is of type {@link ReplicaType#TLOG} but is also the leader, it will behave
   * as a {@link ReplicaType#NRT}. A shard leader will forward updates to all active {@link ReplicaType#NRT} and {@link ReplicaType#TLOG} replicas.
   */
  TLOG,
  /**
   * Doesn’t index or writes to transaction log. Just replicates from {@link ReplicaType#NRT} or {@link ReplicaType#TLOG} replicas. {@link ReplicaType#PULL}
   * replicas can’t become shard leaders (i.e., if there are only pull replicas in the collection at some point, updates will fail
   * same as if there is no leaders, queries continue to work), so they don’t even participate in elections.
   */
  PULL;

  public static ReplicaType get(String name) {
    return name == null ? ReplicaType.NRT : ReplicaType.valueOf(name.toUpperCase(Locale.ROOT));
  }
}
