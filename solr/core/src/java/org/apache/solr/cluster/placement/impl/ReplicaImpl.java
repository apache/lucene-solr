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

import com.google.common.collect.Maps;
import org.apache.solr.cluster.placement.Node;
import org.apache.solr.cluster.placement.Replica;
import org.apache.solr.cluster.placement.Shard;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.util.Pair;

class ReplicaImpl implements Replica {
  private final String replicaName;
  private final String coreName;
  private final Shard shard;
  private final ReplicaType replicaType;
  private final ReplicaState replicaState;
  private final Node node;

  /**
   * Transforms {@link org.apache.solr.common.cloud.Replica}'s of a {@link Slice} into a map of {@link Replica}'s,
   * keyed by replica name ({@link Replica#getReplicaName()}). Also returns in the
   */
  static Pair<Map<String, Replica>, Replica> getReplicas(Collection<org.apache.solr.common.cloud.Replica> sliceReplicas, Shard shard) {
    Map<String, Replica> replicas = Maps.newHashMap();
    Replica leader = null;

    for (org.apache.solr.common.cloud.Replica sliceReplica : sliceReplicas) {
      String replicaName = sliceReplica.getName();
      Replica replica = new ReplicaImpl(replicaName, shard, sliceReplica);
      replicas.put(replicaName, replica);

      if (sliceReplica.isLeader()) {
        leader = replica;
      }
    }

    return new Pair<>(replicas, leader);
  }

  private ReplicaImpl(String replicaName, Shard shard, org.apache.solr.common.cloud.Replica sliceReplica) {
    this.replicaName = replicaName;
    this.coreName = sliceReplica.getCoreName();
    this.shard = shard;
    this.replicaType = translateType(sliceReplica.getType());
    this.replicaState = translateState(sliceReplica.getState());
    // Note this node might not be live, and if it is it is a different instance from the Nodes in Cluster, but that's ok.
    this.node = new NodeImpl(sliceReplica.getNodeName());
  }

  private Replica.ReplicaType translateType(org.apache.solr.common.cloud.Replica.Type type) {
    switch (type) {
      case NRT: return Replica.ReplicaType.NRT;
      case TLOG: return Replica.ReplicaType.TLOG;
      case PULL: return Replica.ReplicaType.PULL;
      default: throw new RuntimeException("Unexpected " + type);
    }
  }

  private Replica.ReplicaState translateState(org.apache.solr.common.cloud.Replica.State state) {
    switch (state) {
      case ACTIVE: return Replica.ReplicaState.ACTIVE;
      case DOWN: return Replica.ReplicaState.DOWN;
      case RECOVERING: return Replica.ReplicaState.RECOVERING;
      case RECOVERY_FAILED: return Replica.ReplicaState.RECOVERY_FAILED;
      default: throw new RuntimeException("Unexpected " + state);
    }
  }

  @Override
  public Shard getShard() {
    return shard;
  }

  @Override
  public ReplicaType getType() {
    return replicaType;
  }

  @Override
  public ReplicaState getState() {
    return replicaState;
  }

  @Override
  public String getReplicaName() {
    return replicaName;
  }

  @Override
  public String getCoreName() {
    return coreName;
  }

  @Override
  public Node getNode() {
    return node;
  }

  /**
   * Translating a plugin visible ReplicaType to the internal Solr enum {@link org.apache.solr.common.cloud.Replica.Type}.
   * The obvious approach would have been to add the internal Solr enum value as a parameter in the ReplicaType enum,
   * but that would have leaked an internal SolrCloud implementation class to the plugin API.
   */
  static org.apache.solr.common.cloud.Replica.Type toCloudReplicaType(ReplicaType type) {
    switch (type) {
      case NRT: return org.apache.solr.common.cloud.Replica.Type.NRT;
      case TLOG: return org.apache.solr.common.cloud.Replica.Type.TLOG;
      case PULL: return org.apache.solr.common.cloud.Replica.Type.PULL;
      default: throw new IllegalArgumentException("Unknown " + type);
    }
  }

  // TODO implement hashCode() and equals()
}
