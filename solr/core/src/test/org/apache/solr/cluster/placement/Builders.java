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
import org.apache.solr.cluster.placement.impl.AttributeFetcherImpl;
import org.apache.solr.cluster.placement.impl.AttributeValuesImpl;
import org.apache.solr.cluster.placement.impl.CollectionMetricsBuilder;
import org.apache.solr.cluster.placement.impl.NodeMetricImpl;
import org.apache.solr.cluster.placement.impl.ReplicaMetricImpl;
import org.apache.solr.common.util.Pair;
import org.junit.Assert;

import java.util.*;

/**
 * Builder classes to make tests using different cluster and node configurations easier to write and to read.
 */
public class Builders {

  public static ClusterBuilder newClusterBuilder() {
    return new ClusterBuilder();
  }

  public static CollectionBuilder newCollectionBuilder(String collectionName) {
    return new CollectionBuilder(collectionName);
  }

  public static class ClusterBuilder {
    /**
     * {@link NodeBuilder} for the live nodes of the cluster.
     */
    private LinkedList<NodeBuilder> nodeBuilders = new LinkedList<>();
    private LinkedList<CollectionBuilder> collectionBuilders = new LinkedList<>();

    public ClusterBuilder initializeLiveNodes(int countNodes) {
      nodeBuilders = new LinkedList<>();
      for (int n = 0; n < countNodes; n++) {
        NodeBuilder nodeBuilder = new NodeBuilder().setNodeName("node_" + n); // Default name, can be changed
        nodeBuilder.setTotalDiskGB(10000.0);
        nodeBuilder.setFreeDiskGB(5000.0);
        nodeBuilders.add(nodeBuilder);
      }
      return this;
    }

    public LinkedList<NodeBuilder> getLiveNodeBuilders() {
      return nodeBuilders;
    }

    public ClusterBuilder addCollection(CollectionBuilder collectionBuilder) {
      collectionBuilders.add(collectionBuilder);
      return this;
    }

    public Cluster build() {
      // TODO if converting all tests to use builders change ClusterImpl ctor to use list of nodes
      return new ClusterAbstractionsForTest.ClusterImpl(new HashSet<>(buildLiveNodes()), buildClusterCollections());
    }

    public List<Node> buildLiveNodes() {
      List<Node> liveNodes = new LinkedList<>();
      for (NodeBuilder nodeBuilder : nodeBuilders) {
        liveNodes.add(nodeBuilder.build());
      }

      return liveNodes;
    }

    Map<String, SolrCollection> buildClusterCollections() {
      Map<String, SolrCollection> clusterCollections = new LinkedHashMap<>();
      for (CollectionBuilder collectionBuilder : collectionBuilders) {
        SolrCollection solrCollection = collectionBuilder.build();
        clusterCollections.put(solrCollection.getName(), solrCollection);
      }

      return clusterCollections;
    }

    public AttributeFetcher buildAttributeFetcher() {
      Map<String, Map<Node, String>> sysprops = new HashMap<>();
      Map<NodeMetric<?>, Map<Node, Object>> metrics = new HashMap<>();
      Map<String, CollectionMetrics> collectionMetrics = new HashMap<>();

      // TODO And a few more missing and will be added...

      // Slight redoing of work twice (building Node instances) but let's favor readability over tricks (I could think
      // of many) to reuse the nodes computed in build() or build the AttributeFetcher at the same time.
      for (NodeBuilder nodeBuilder : nodeBuilders) {
        Node node = nodeBuilder.build();

        if (nodeBuilder.getCoreCount() != null) {
          metrics.computeIfAbsent(NodeMetricImpl.NUM_CORES, n -> new HashMap<>())
              .put(node, nodeBuilder.getCoreCount());
        }
        if (nodeBuilder.getFreeDiskGB() != null) {
          metrics.computeIfAbsent(NodeMetricImpl.FREE_DISK_GB, n -> new HashMap<>())
              .put(node, nodeBuilder.getFreeDiskGB());
        }
        if (nodeBuilder.getTotalDiskGB() != null) {
          metrics.computeIfAbsent(NodeMetricImpl.TOTAL_DISK_GB, n -> new HashMap<>())
              .put(node, nodeBuilder.getTotalDiskGB());
        }
        if (nodeBuilder.getSysprops() != null) {
          nodeBuilder.getSysprops().forEach((name, value) -> {
            sysprops.computeIfAbsent(name, n -> new HashMap<>())
                .put(node, value);
          });
        }
        if (nodeBuilder.getMetrics() != null) {
          nodeBuilder.getMetrics().forEach((name, value) -> {
            metrics.computeIfAbsent(name, n -> new HashMap<>())
                .put(node, value);
          });
        }
      }

      if (!collectionBuilders.isEmpty()) {
        Map<Node, Object> nodeToCoreCount = metrics.computeIfAbsent(NodeMetricImpl.NUM_CORES, n -> new HashMap<>());
        collectionBuilders.forEach(builder -> {
          collectionMetrics.put(builder.collectionName, builder.collectionMetricsBuilder.build());
          SolrCollection collection = builder.build();
          collection.iterator().forEachRemaining(shard ->
              shard.iterator().forEachRemaining(replica -> {
                nodeToCoreCount.compute(replica.getNode(), (node, count) ->
                    (count == null) ? 1 : ((Number) count).intValue() + 1);
              }));
        });
      }

      AttributeValues attributeValues = new AttributeValuesImpl(sysprops, metrics, collectionMetrics);
      return new AttributeFetcherForTest(attributeValues);
    }
  }

  public static class CollectionBuilder {
    private final String collectionName;
    private LinkedList<ShardBuilder> shardBuilders = new LinkedList<>();
    private Map<String, String> customProperties = new HashMap<>();
    int replicaNumber = 0; // global replica numbering for the collection
    private CollectionMetricsBuilder collectionMetricsBuilder = new CollectionMetricsBuilder();

    public CollectionBuilder(String collectionName) {
      this.collectionName = collectionName;
    }

    public CollectionBuilder addCustomProperty(String name, String value) {
      customProperties.put(name, value);
      return this;
    }

    public CollectionMetricsBuilder getCollectionMetricsBuilder() {
      return collectionMetricsBuilder;
    }

    /**
     * @return The internal shards data structure to allow test code to modify the replica distribution to nodes.
     */
    public LinkedList<ShardBuilder> getShardBuilders() {
      return shardBuilders;
    }

    /**
     * Initializes the collection to a specific shard and replica distribution passed in {@code shardsReplicas}.
     * @param shardsReplicas A list of shard descriptions, describing the replicas of that shard.
     *                       Replica description include the replica type and the node on which the replica should be placed.
     *                       Everything is text to make it easy to design specific collections. For example the following value:
     *  <pre>{@code
     *  List.of(
     *    List.of("NRT 0", "TLOG 0", "NRT 3"), // shard 1
     *    List.of("NRT 1", "NRT 3", "TLOG 2")); // shard 2
     *  }</pre>
     *                       Creates a placement that would distribute replicas to nodes (there must be at least 4 nodes)
     *                       in the following way:
     *  <pre>{@code
     *  +--------------+----+----+----+----+
     *  |         Node |  0 |  1 |  2 |  3 |
     *  +----------------------------------+
     *  |   Shard 1:   |    |    |    |    |
     *  |         NRT  |  X |    |    |  X |
     *  |         TLOG |  X |    |    |    |
     *  +----------------------------------+
     *  |   Shard 2:   |    |    |    |    |
     *  |         NRT  |    |  X |    |  X |
     *  |         TLOG |    |    |  X |    |
     *  +--------------+----+----+----+----+
     *  }</pre>
     */
    public CollectionBuilder customCollectionSetup(List<List<String>> shardsReplicas, List<NodeBuilder> liveNodes) {
      shardBuilders = new LinkedList<>();
      int shardNumber = 1; // Shard numbering starts at 1
      for (List<String> replicasOnNodes : shardsReplicas) {
        String shardName = buildShardName(shardNumber++);
        LinkedList<ReplicaBuilder> replicas = new LinkedList<>();
        ReplicaBuilder leader = null;

        for (String replicaNode : replicasOnNodes) {
          // replicaNode is like "TLOG 2" meaning a TLOG replica should be placed on node 2
          String[] splited = replicaNode.split("\\s+");
          Assert.assertEquals(2, splited.length);
          Replica.ReplicaType type = Replica.ReplicaType.valueOf(splited[0]);
          final NodeBuilder node;
          int nodeIndex = Integer.parseInt(splited[1]);
          if (nodeIndex < liveNodes.size()) {
            node = liveNodes.get(nodeIndex);
          } else {
            // The collection can have replicas on non live nodes. Let's create such a node here (that is not known to the
            // cluster). There could be many non live nodes in the collection configuration, they will all reference new
            // instances such as below of a node unknown to cluster, but all will have the same name (so will be equal if
            // tested).
            node = new NodeBuilder().setNodeName("NonLiveNode");
          }
          String replicaName = buildReplicaName(shardName, type);

          ReplicaBuilder replicaBuilder = new ReplicaBuilder();
          replicaBuilder.setReplicaName(replicaName).setCoreName(buildCoreName(replicaName)).setReplicaType(type)
              .setReplicaState(Replica.ReplicaState.ACTIVE).setReplicaNode(node);
          replicas.add(replicaBuilder);

          // No way to specify which replica is the leader. Could be done by adding a "*" to the replica definition for example
          // in the passed shardsReplicas but not implementing this until it is needed :)
          if (leader == null && type != Replica.ReplicaType.PULL) {
            leader = replicaBuilder;
          }
        }

        ShardBuilder shardBuilder = new ShardBuilder();
        shardBuilder.setShardName(shardName).setReplicaBuilders(replicas).setLeader(leader);
        shardBuilders.add(shardBuilder);
      }

      return this;
    }

    /**
     * Initializes shard and replica builders for the collection based on passed parameters. Replicas are assigned round
     * robin to the nodes. The shard leader is the first NRT replica of each shard (or first TLOG is no NRT).
     * Shard and replica configuration can be modified afterwards, the returned builder hierarchy is a convenient starting point.
     * @param countShards number of shards to create
     * @param countNrtReplicas number of NRT replicas per shard
     * @param countTlogReplicas number of TLOG replicas per shard
     * @param countPullReplicas number of PULL replicas per shard
     * @param nodes list of nodes to place replicas on.
     */
    public CollectionBuilder initializeShardsReplicas(int countShards, int countNrtReplicas, int countTlogReplicas,
                                                      int countPullReplicas, List<NodeBuilder> nodes) {
      return initializeShardsReplicas(countShards, countNrtReplicas, countTlogReplicas, countPullReplicas, nodes, null);
    }

    /**
     * Initializes shard and replica builders for the collection based on passed parameters. Replicas are assigned round
     * robin to the nodes. The shard leader is the first NRT replica of each shard (or first TLOG is no NRT).
     * Shard and replica configuration can be modified afterwards, the returned builder hierarchy is a convenient starting point.
     * @param countShards number of shards to create
     * @param countNrtReplicas number of NRT replicas per shard
     * @param countTlogReplicas number of TLOG replicas per shard
     * @param countPullReplicas number of PULL replicas per shard
     * @param nodes list of nodes to place replicas on.
     * @param initialSizeGBPerShard initial replica size (in GB) per shard
     */
    public CollectionBuilder initializeShardsReplicas(int countShards, int countNrtReplicas, int countTlogReplicas,
                                               int countPullReplicas, List<NodeBuilder> nodes,
                                                      List<Integer> initialSizeGBPerShard) {
      Iterator<NodeBuilder> nodeIterator = nodes.iterator();

      shardBuilders = new LinkedList<>();
      if (initialSizeGBPerShard != null && initialSizeGBPerShard.size() != countShards) {
        throw new RuntimeException("list of shard sizes must be the same length as the countShards!");
      }

      for (int shardNumber = 1; shardNumber <= countShards; shardNumber++) {
        String shardName = buildShardName(shardNumber);

        CollectionMetricsBuilder.ShardMetricsBuilder shardMetricsBuilder = new CollectionMetricsBuilder.ShardMetricsBuilder(shardName);

        LinkedList<ReplicaBuilder> replicas = new LinkedList<>();
        ReplicaBuilder leader = null;
        CollectionMetricsBuilder.ReplicaMetricsBuilder leaderMetrics = null;

        // Iterate on requested counts, NRT then TLOG then PULL. Leader chosen as first NRT (or first TLOG if no NRT)
        List<Pair<Replica.ReplicaType, Integer>> replicaTypes = List.of(
            new Pair<>(Replica.ReplicaType.NRT, countNrtReplicas),
            new Pair<>(Replica.ReplicaType.TLOG, countTlogReplicas),
            new Pair<>(Replica.ReplicaType.PULL, countPullReplicas));

        for (Pair<Replica.ReplicaType, Integer> tc : replicaTypes) {
          Replica.ReplicaType type = tc.first();
          int count = tc.second();
          for (int r = 0; r < count; r++) {
            if (!nodeIterator.hasNext()) {
              nodeIterator = nodes.iterator();
            }
            // If the nodes set is empty, this call will fail
            final NodeBuilder node = nodeIterator.next();

            String replicaName = buildReplicaName(shardName, type);

            ReplicaBuilder replicaBuilder = new ReplicaBuilder();
            replicaBuilder.setReplicaName(replicaName).setCoreName(buildCoreName(replicaName)).setReplicaType(type)
                .setReplicaState(Replica.ReplicaState.ACTIVE).setReplicaNode(node);
            replicas.add(replicaBuilder);

            CollectionMetricsBuilder.ReplicaMetricsBuilder replicaMetricsBuilder = new CollectionMetricsBuilder.ReplicaMetricsBuilder(replicaName);
            shardMetricsBuilder.getReplicaMetricsBuilders().put(replicaName, replicaMetricsBuilder);
            if (initialSizeGBPerShard != null) {
              replicaMetricsBuilder.addMetric(ReplicaMetricImpl.INDEX_SIZE_GB, initialSizeGBPerShard.get(shardNumber - 1) * ReplicaMetricImpl.GB);
            }
            if (leader == null && type != Replica.ReplicaType.PULL) {
              leader = replicaBuilder;
              leaderMetrics = replicaMetricsBuilder;
            }
          }
        }

        ShardBuilder shardBuilder = new ShardBuilder();
        shardBuilder.setShardName(shardName).setReplicaBuilders(replicas).setLeader(leader);
        shardMetricsBuilder.setLeaderMetrics(leaderMetrics);
        shardBuilders.add(shardBuilder);
        collectionMetricsBuilder.getShardMetricsBuilders().put(shardName, shardMetricsBuilder);
      }

      return this;
    }

    private String buildShardName(int shardIndex) {
      return "shard" + shardIndex;
    }

    private String buildReplicaName(String shardName, Replica.ReplicaType replicaType) {
      return collectionName + "_" + shardName + "_replica_" + replicaType.getSuffixChar() + replicaNumber++;
    }

    private String buildCoreName(String replicaName) {
      return replicaName + "_c";
    }

    public SolrCollection build() {
      ClusterAbstractionsForTest.SolrCollectionImpl solrCollection = new ClusterAbstractionsForTest.SolrCollectionImpl(collectionName, customProperties);

      final LinkedHashMap<String, Shard> shards = new LinkedHashMap<>();

      for (ShardBuilder shardBuilder : shardBuilders) {
        Shard shard = shardBuilder.build(solrCollection);
        shards.put(shard.getShardName(), shard);
      }

      solrCollection.setShards(shards);
      return solrCollection;
    }
  }

  public static class ShardBuilder {
    private String shardName;
    private LinkedList<ReplicaBuilder> replicaBuilders = new LinkedList<>();
    private ReplicaBuilder leaderReplicaBuilder;

    public ShardBuilder setShardName(String shardName) {
      this.shardName = shardName;
      return this;
    }

    public String getShardName() {
      return shardName;
    }

    public LinkedList<ReplicaBuilder> getReplicaBuilders() {
      return replicaBuilders;
    }

    public ShardBuilder setReplicaBuilders(LinkedList<ReplicaBuilder> replicaBuilders) {
      this.replicaBuilders = replicaBuilders;
      return this;
    }

    public ShardBuilder setLeader(ReplicaBuilder leaderReplicaBuilder) {
      this.leaderReplicaBuilder = leaderReplicaBuilder;
      return this;
    }

    public Shard build(SolrCollection collection) {
      ClusterAbstractionsForTest.ShardImpl shard = new ClusterAbstractionsForTest.ShardImpl(shardName, collection, Shard.ShardState.ACTIVE);

      final LinkedHashMap<String, Replica> replicas = new LinkedHashMap<>();
      Replica leader = null;

      for (ReplicaBuilder replicaBuilder : replicaBuilders) {
        Replica replica = replicaBuilder.build(shard);
        replicas.put(replica.getReplicaName(), replica);

        if (leaderReplicaBuilder == replicaBuilder) {
          leader = replica;
        }
      }

      shard.setReplicas(replicas, leader);
      return shard;
    }
  }

  public static class ReplicaBuilder {
    private String replicaName;
    private String coreName;
    private Replica.ReplicaType replicaType;
    private Replica.ReplicaState replicaState;
    private NodeBuilder replicaNode;
    private Map<ReplicaMetric<?>, Object> metrics;

    public ReplicaBuilder setReplicaName(String replicaName) {
      this.replicaName = replicaName;
      return this;
    }

    public ReplicaBuilder setCoreName(String coreName) {
      this.coreName = coreName;
      return this;
    }

    public Replica.ReplicaType getReplicaType() {
      return replicaType;
    }

    public ReplicaBuilder setReplicaType(Replica.ReplicaType replicaType) {
      this.replicaType = replicaType;
      return this;
    }

    public ReplicaBuilder setReplicaState(Replica.ReplicaState replicaState) {
      this.replicaState = replicaState;
      return this;
    }

    public ReplicaBuilder setReplicaNode(NodeBuilder replicaNode) {
      this.replicaNode = replicaNode;
      return this;
    }

    public ReplicaBuilder setReplicaMetric(ReplicaMetric<?> metric, Object value) {
      if (metrics == null) {
        metrics = new HashMap<>();
      }
      metrics.put(metric, metric.convert(value));
      return this;
    }

    public Replica build(Shard shard) {
      return new ClusterAbstractionsForTest.ReplicaImpl(replicaName, coreName, shard, replicaType, replicaState, replicaNode.build());
    }
  }

  public static class NodeBuilder {
    private String nodeName = null;
    private Integer coreCount = null;
    private Double freeDiskGB = null;
    private Double totalDiskGB = null;
    private Map<String, String> sysprops = null;
    private Map<NodeMetric<?>, Object> metrics = null;

    public NodeBuilder setNodeName(String nodeName) {
      this.nodeName = nodeName;
      return this;
    }

    public NodeBuilder setCoreCount(Integer coreCount) {
      this.coreCount = coreCount;
      return this;
    }

    public NodeBuilder setFreeDiskGB(Double freeDiskGB) {
      this.freeDiskGB = freeDiskGB;
      return this;
    }

    public NodeBuilder setTotalDiskGB(Double totalDiskGB) {
      this.totalDiskGB = totalDiskGB;
      return this;
    }

    public NodeBuilder setSysprop(String key, String value) {
      if (sysprops == null) {
        sysprops = new HashMap<>();
      }
      String name = AttributeFetcherImpl.getSystemPropertySnitchTag(key);
      sysprops.put(name, value);
      return this;
    }

    public NodeBuilder setMetric(NodeMetric<?> metric, Object value) {
      if (metrics == null) {
        metrics = new HashMap<>();
      }
      metrics.put(metric, metric.convert(value));
      return this;
    }

    public Integer getCoreCount() {
      return coreCount;
    }

    public Double getFreeDiskGB() {
      return freeDiskGB;
    }

    public Double getTotalDiskGB() {
      return totalDiskGB;
    }

    public Map<String, String> getSysprops() {
      return sysprops;
    }

    public Map<NodeMetric<?>, Object> getMetrics() {
      return metrics;
    }

    public Node build() {
      // It is ok to build a new instance each time, that instance does the right thing with equals() and hashCode()
      return new ClusterAbstractionsForTest.NodeImpl(nodeName);
    }
  }
}
