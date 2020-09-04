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

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import com.google.common.collect.Maps;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.cluster.placement.*;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.util.Pair;

class ClusterImpl implements Cluster {
  private final Set<Node> liveNodes;
  private final ClusterState clusterState;

  ClusterImpl(SolrCloudManager solrCloudManager) throws IOException {
    liveNodes = NodeImpl.getNodes(solrCloudManager.getClusterStateProvider().getLiveNodes());
    clusterState = solrCloudManager.getClusterStateProvider().getClusterState();
  }

  @Override
  public Set<Node> getLiveNodes() {
    return liveNodes;
  }

  @Override
  public Optional<SolrCollection> getCollection(String collectionName) {
    return SolrCollectionImpl.createCollectionFacade(clusterState, collectionName);
  }

  /**
   * <p>Returns the set of names of all collections in the cluster. This is a costly method as it potentially builds a
   * large set in memory. Usage is discouraged.
   *
   * <p>Eventually, a similar method allowing efficiently filtering the set of returned collections is desirable. Efficiently
   * implies filter does not have to be applied to each collection but a regular expression or similar is passed in,
   * allowing direct access to qualifying collections.
   */
  public Set<String> getAllCollectionNames() {
    return clusterState.getCollectionsMap().values().stream().map(DocCollection::getName).collect(Collectors.toSet());
  }
}

class NodeImpl implements Node {
  public final String nodeName;

  /**
   * Transforms a collection of node names into a set of {@link Node} instances.
   */
  static Set<Node> getNodes(Collection<String> nodeNames) {
    return nodeNames.stream().map(NodeImpl::new).collect(Collectors.toSet());
  }

  NodeImpl(String nodeName) {
    this.nodeName = nodeName;
  }

  @Override
  public String getName() {
    return nodeName;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(" + getName() + ")";
  }

  /**
   * This class ends up as a key in Maps in {@link org.apache.solr.cluster.placement.AttributeValues}.
   * It is important to implement this method comparing node names given that new instances of {@link Node} are created
   * with names equal to existing instances (See {@link ReplicaImpl} constructor).
   */
  public boolean equals(Object obj) {
    if (obj == null) { return false; }
    if (obj == this) { return true; }
    if (obj.getClass() != getClass()) { return false; }
    NodeImpl other = (NodeImpl) obj;
    return Objects.equals(this.nodeName, other.nodeName);
  }

  public int hashCode() {
    return Objects.hashCode(nodeName);
  }
}


class SolrCollectionImpl implements SolrCollection {
  private final String collectionName;
  /** Map from {@link Shard#getShardName()} to {@link Shard} */
  private final Map<String, Shard> shards;
  private final DocCollection docCollection;

  static Optional<SolrCollection> createCollectionFacade(ClusterState clusterState, String collectionName) {
    DocCollection docCollection = clusterState.getCollectionOrNull(collectionName);

    if (docCollection == null) {
      return Optional.empty();
    } else {
      return Optional.of(new SolrCollectionImpl(docCollection));
    }
  }

  SolrCollectionImpl(DocCollection docCollection) {
    this.collectionName = docCollection.getName();
    this.shards = ShardImpl.getShards(this, docCollection.getSlices());
    this.docCollection = docCollection;
  }

  @Override
  public String getName() {
    return collectionName;
  }

  @Override
  public Map<String, Shard> getShards() {
    return shards;
  }

  @Override
  public String getCustomProperty(String customPropertyName) {
    return docCollection.getStr(customPropertyName);
  }

  /**
   * Multiple instances of this class can be created for the same underlying actual collection. OTOH if the underlying
   * collection does change between the calls, resulting {@link SolrCollection} instances will not be equal. Unsure we
   * need to redefine these methods here anyway, so maybe TODO remove?
   */
  public boolean equals(Object obj) {
    if (obj == null) { return false; }
    if (obj == this) { return true; }
    if (obj.getClass() != getClass()) { return false; }
    SolrCollectionImpl other = (SolrCollectionImpl) obj;
    return Objects.equals(this.collectionName, other.collectionName)
            && Objects.equals(this.shards, other.shards);
  }

  public int hashCode() {
    return Objects.hashCode(collectionName);
  }
}


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

  public boolean equals(Object obj) {
    if (obj == null) { return false; }
    if (obj == this) { return true; }
    if (obj.getClass() != getClass()) { return false; }
    ReplicaImpl other = (ReplicaImpl) obj;
    return Objects.equals(this.replicaName, other.replicaName)
            && Objects.equals(this.coreName, other.coreName)
            && Objects.equals(this.shard, other.shard)
            && Objects.equals(this.replicaType, other.replicaType)
            && Objects.equals(this.replicaState, other.replicaState)
            && Objects.equals(this.node, other.node);
  }

  public int hashCode() {
    return Objects.hash(replicaName, coreName, shard, replicaType, replicaState, node);
  }
}
