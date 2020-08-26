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

package org.apache.solr.cluster.placement.impl;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;

import com.google.common.collect.Maps;
import org.apache.solr.cluster.placement.PropertyValueSource;
import org.apache.solr.cluster.placement.Replica;
import org.apache.solr.cluster.placement.Shard;
import org.apache.solr.cluster.placement.SolrCollection;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.util.Pair;

class ShardImpl implements Shard {
  private final String shardName;
  private final SolrCollection collection;
  private final ShardState shardState;
  private final Map<String, Replica> replicas;
  private final Replica leader;

  /**
   * Transforms {@link Slice}'s of a {@link org.apache.solr.common.cloud.DocCollection} into a map of {@link Shard}'s,
   * keyed by shard name ({@link Shard#getShardName()}).
   */
  static Map<String, Shard> getShards(SolrCollection solrCollection, Collection<Slice> slices) {
    Map<String, Shard> shards = Maps.newHashMap();

    for (Slice slice : slices) {
      String shardName = slice.getName();
      shards.put(shardName, new ShardImpl(shardName, solrCollection, slice));
    }

    return shards;
  }

  private ShardImpl(String shardName, SolrCollection collection, Slice slice) {
    this.shardName = shardName;
    this.collection = collection;
    this.shardState = translateState(slice.getState());

    Pair<Map<String, Replica>, Replica> pair = ReplicaImpl.getReplicas(slice.getReplicas(), this);
    replicas = pair.first();
    leader = pair.second();
  }

  private ShardState translateState(Slice.State state) {
    switch (state) {
      case ACTIVE: return ShardState.ACTIVE;
      case INACTIVE: return ShardState.INACTIVE;
      case CONSTRUCTION: return ShardState.CONSTRUCTION;
      case RECOVERY: return ShardState.RECOVERY;
      case RECOVERY_FAILED: return ShardState.RECOVERY_FAILED;
      default: throw new RuntimeException("Unexpected " + state);
    }
  }

  @Override
  public String getShardName() {
    return shardName;
  }

  @Override
  public SolrCollection getCollection() {
    return collection;
  }

  @Override
  public Map<String, Replica> getReplicas() {
    return replicas;
  }

  @Override
  public Replica getLeader() {
    return leader;
  }

  @Override
  public ShardState getState() {
    return shardState;
  }

  /**
   * This class implements {@link PropertyValueSource} and will end up as a key in a Map for fetching {@link org.apache.solr.cluster.placement.PropertyKey}'s
   */
  public boolean equals(Object obj) {
    if (obj == null) { return false; }
    if (obj == this) { return true; }
    if (obj.getClass() != getClass()) { return false; }
    ShardImpl other = (ShardImpl) obj;
    return Objects.equals(this.shardName, other.shardName)
        && Objects.equals(this.collection, other.collection)
        && Objects.equals(this.shardState, other.shardState)
        && Objects.equals(this.replicas, other.replicas)
        && Objects.equals(this.leader, other.leader);
  }

  public int hashCode() {
    return Objects.hash(shardName, collection, shardState);
  }
}
