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

package org.apache.solr.cluster;

import java.util.Iterator;

/**
 * Shard in a {@link SolrCollection}, i.e. a subset of the data indexed in that collection.
 */
public interface Shard {
  String getShardName();

  /**
   * @return the collection this shard is part of
   */
  SolrCollection getCollection();

  /**
   * Returns the {@link Replica} of the given name for that shard, if such a replica exists.
   * @return {@code null} if the replica does not (or does not yet) exist for the shard.
   */
  Replica getReplica(String name);

  /**
   * @return an iterator over {@link Replica}s already existing for this {@link Shard}.
   */
  Iterator<Replica> iterator();

  /**
   * Allow foreach iteration on replicas such as: {@code for (Replica r : shard.replicas()) {...}}.
   */
  Iterable<Replica> replicas();

  /**
   * @return the current leader {@link Replica} for this {@link Shard}. Note that by the time this method returns the leader might
   * have changed. Also, if there's no leader for any reason (don't shoot the messenger), this method will return {@code null}.
   */
  Replica getLeader();

  ShardState getState();

  enum ShardState {
    ACTIVE, INACTIVE, CONSTRUCTION, RECOVERY, RECOVERY_FAILED
  }
}
