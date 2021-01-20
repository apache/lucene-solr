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

import org.apache.solr.cluster.*;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Cluster abstractions independent of any internal SolrCloud abstractions to use in tests (of plugin code).
 */
class ClusterAbstractionsForTest {

  static class ClusterImpl implements Cluster {
    private final Set<Node> liveNodes = new HashSet<>();
    private final Map<String, SolrCollection> collections = new HashMap<>();

    ClusterImpl(Set<Node> liveNodes, Map<String, SolrCollection> collections) {
      this.liveNodes.addAll(liveNodes);
      this.collections.putAll(collections);
    }

    @Override
    public Set<Node> getLiveNodes() {
      return liveNodes;
    }

    @Override
    public SolrCollection getCollection(String collectionName) {
      return collections.get(collectionName);
    }

    @Override
    @Nonnull
    public Iterator<SolrCollection> iterator() {
      return collections.values().iterator();
    }

    @Override
    public Iterable<SolrCollection> collections() {
      return ClusterImpl.this::iterator;
    }
  }


  static class NodeImpl implements Node {
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
     * with names equal to existing instances (See {@link Builders.NodeBuilder#build()}).
     */
    public boolean equals(Object obj) {
      if (obj == null) {
        return false;
      }
      if (obj == this) {
        return true;
      }
      if (obj.getClass() != getClass()) {
        return false;
      }
      NodeImpl other = (NodeImpl) obj;
      return Objects.equals(this.nodeName, other.nodeName);
    }

    public int hashCode() {
      return Objects.hashCode(nodeName);
    }
  }


  static class SolrCollectionImpl implements SolrCollection {
    private final String collectionName;
    /**
     * Map from {@link Shard#getShardName()} to {@link Shard}
     */
    private Map<String, Shard> shards;
    private final Map<String, String> customProperties;

    SolrCollectionImpl(String collectionName, Map<String, String> customProperties) {
      this.collectionName = collectionName;
      this.customProperties = customProperties;
    }

    /**
     * Setting the shards has to happen (in tests) after creating the collection because shards reference the collection
     */
    void setShards(Map<String, Shard> shards) {
      this.shards = shards;
    }

    @Override
    public String getName() {
      return collectionName;
    }

    @Override
    public Shard getShard(String name) {
      return shards.get(name);
    }

    @Override
    @Nonnull
    public Iterator<Shard> iterator() {
      return shards.values().iterator();
    }

    @Override
    public Iterable<Shard> shards() {
      return SolrCollectionImpl.this::iterator;
    }

    @Override
    public Set<String> getShardNames() {
      return shards.keySet();
    }

    @Override
    public String getCustomProperty(String customPropertyName) {
      return customProperties.get(customPropertyName);
    }
  }


  static class ShardImpl implements Shard {
    private final String shardName;
    private final SolrCollection collection;
    private final ShardState shardState;
    private Map<String, Replica> replicas;
    private Replica leader;

    ShardImpl(String shardName, SolrCollection collection, ShardState shardState) {
      this.shardName = shardName;
      this.collection = collection;
      this.shardState = shardState;
    }

    /**
     * Setting the replicas has to happen (in tests) after creating the shard because replicas reference the shard
     */
    void setReplicas(Map<String, Replica> replicas, Replica leader) {
      this.replicas = replicas;
      this.leader = leader;
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
    public Replica getReplica(String name) {
      return replicas.get(name);
    }

    @Override
    @Nonnull
    public Iterator<Replica> iterator() {
      return replicas.values().iterator();
    }

    @Override
    public Iterable<Replica> replicas() {
      return ShardImpl.this::iterator;
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
      if (obj == null) {
        return false;
      }
      if (obj == this) {
        return true;
      }
      if (obj.getClass() != getClass()) {
        return false;
      }
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


  static class ReplicaImpl implements Replica {
    private final String replicaName;
    private final String coreName;
    private final Shard shard;
    private final ReplicaType replicaType;
    private final ReplicaState replicaState;
    private final Node node;

    ReplicaImpl(String replicaName, String coreName, Shard shard, ReplicaType replicaType, ReplicaState replicaState, Node node) {
      this.replicaName = replicaName;
      this.coreName = coreName;
      this.shard = shard;
      this.replicaType = replicaType;
      this.replicaState = replicaState;
      this.node = node;
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

    public boolean equals(Object obj) {
      if (obj == null) {
        return false;
      }
      if (obj == this) {
        return true;
      }
      if (obj.getClass() != getClass()) {
        return false;
      }
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
}
