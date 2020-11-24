package org.apache.solr.cluster.placement.impl;

import org.apache.solr.cluster.*;
import org.apache.solr.cluster.placement.AttributeFetcher;
import org.apache.solr.cluster.placement.AttributeValues;
import org.apache.solr.common.util.Pair;

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

    static class ClusterBuilder {
        private LinkedList<NodeBuilder> nodeBuilders = new LinkedList<>();
        private LinkedList<CollectionBuilder> collectionBuilders = new LinkedList<>();

        ClusterBuilder initializeNodes(int countNodes) {
            nodeBuilders = new LinkedList<>();
            for (int n = 0; n < countNodes; n++) {
                nodeBuilders.add(new NodeBuilder().setNodeName("node" + n)); // Default name, can be changed
            }
            return this;
        }

        LinkedList<NodeBuilder> getNodeBuilders() {
            return nodeBuilders;
        }

        ClusterBuilder addCollection(CollectionBuilder collectionBuilder) {
            collectionBuilders.add(collectionBuilder);
            return this;
        }

        Cluster build() {
            // TODO if converting all tests to use builders change ClusterImpl ctor to use list of nodes
            return new ClusterAbstractionsForTest.ClusterImpl(new HashSet<>(buildLiveNodes()), buildClusterCollections());
        }

        List<Node> buildLiveNodes() {
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

        AttributeFetcher buildAttributeFetcher() {
            Map<Node, Integer> nodeToCoreCount = new HashMap<>();
            Map<Node, Long> nodeToFreeDisk = new HashMap<>();
            Map<String, Map<Node, String>> sysprops = new HashMap<>();
            Map<String, Map<Node, Double>> metrics = new HashMap<>();

            // TODO And a few more missing and will be added...

            // Slight redoing of work twice (building Node instances) but let's favor readability over tricks (I could think
            // of many) to reuse the nodes computed in build() or build the AttributeFetcher at the same time.
            for (NodeBuilder nodeBuilder : nodeBuilders) {
                Node node = nodeBuilder.build();

                if (nodeBuilder.getCoreCount() != null) {
                    nodeToCoreCount.put(node, nodeBuilder.getCoreCount());
                }
                if (nodeBuilder.getFreeDiskGB() != null) {
                    nodeToFreeDisk.put(node, nodeBuilder.getFreeDiskGB());
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

            AttributeValues attributeValues = new AttributeValuesImpl(nodeToCoreCount, Map.of(), nodeToFreeDisk, Map.of(), Map.of(), Map.of(), sysprops, metrics);
            return new AttributeFetcherForTest(attributeValues);
        }
    }

    static class CollectionBuilder {
        private final String collectionName;
        private LinkedList<ShardBuilder> shardBuilders = new LinkedList<>();
        private Map<String, String> customProperties = new HashMap<>();


        private CollectionBuilder(String collectionName) {
            this.collectionName = collectionName;
        }

        private CollectionBuilder addCustomProperty(String name, String value) {
            customProperties.put(name, value);
            return this;
        }

        /**
         * Initializes shard and replica builders for the collection based on passed parameters. Replicas are assigned round
         * robin to the nodes. The shard leader is the first NRT replica of each shard (or first TLOG is no NRT).
         * Shard and replica configuration can be modified afterwards, the returned builder hierarchy is a convenient starting point.
         */
        CollectionBuilder initializeShardsReplicas(int countShards, int countNrtReplicas, int countTlogReplicas,
                                                   int countPullReplicas, List<NodeBuilder> nodes) {
            Iterator<NodeBuilder> nodeIterator = nodes.iterator();

            shardBuilders = new LinkedList<>();

            for (int s = 0; s < countShards; s++) {
                String shardName = "shard" + (s + 1);

                LinkedList<ReplicaBuilder> replicas = new LinkedList<>();
                ReplicaBuilder leader = null;

                // Iterate on requested counts, NRT then TLOG then PULL. Leader chosen as first NRT (or first TLOG if no NRT)
                List<Pair<Replica.ReplicaType, Integer>> replicaTypes = List.of(
                        new Pair<>(Replica.ReplicaType.NRT, countNrtReplicas),
                        new Pair<>(Replica.ReplicaType.TLOG, countTlogReplicas),
                        new Pair<>(Replica.ReplicaType.PULL, countPullReplicas));

                for (Pair<Replica.ReplicaType, Integer> tc : replicaTypes) {
                    Replica.ReplicaType type = tc.first();
                    int count = tc.second();
                    String replicaPrefix = collectionName + "_" + shardName + "_replica_" + type.getSuffixChar();
                    for (int r = 0; r < count; r++) {
                        String replicaName = replicaPrefix + r;
                        String coreName = replicaName + "_c";
                        if (!nodeIterator.hasNext()) {
                            nodeIterator = nodes.iterator();
                        }
                        // If the nodes set is empty, this call will fail
                        final NodeBuilder node = nodeIterator.next();

                        ReplicaBuilder replicaBuilder = new ReplicaBuilder();
                        replicaBuilder.setReplicaName(replicaName).setCoreName(coreName).setReplicaType(type)
                                .setReplicaState(Replica.ReplicaState.ACTIVE).setReplicaNode(node);
                        replicas.add(replicaBuilder);

                        if (leader == null && type != Replica.ReplicaType.PULL) {
                            leader = replicaBuilder;
                        }
                    }
                }

                ShardBuilder shardBuilder = new ShardBuilder();
                shardBuilder.setShardName(shardName).setReplicaBuilders(replicas).setLeader(leader);
                shardBuilders.add(shardBuilder);
            }

            return this;
        }

        SolrCollection build() {
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

    static class ShardBuilder {
        private String shardName;
        private LinkedList<ReplicaBuilder> replicaBuilders = new LinkedList<>();
        private ReplicaBuilder leaderReplicaBuilder;

        ShardBuilder setShardName(String shardName) {
            this.shardName = shardName;
            return this;
        }

        ShardBuilder setReplicaBuilders(LinkedList<ReplicaBuilder> replicaBuilders) {
            this.replicaBuilders = replicaBuilders;
            return this;
        }

        ShardBuilder setLeader(ReplicaBuilder leaderReplicaBuilder) {
            this.leaderReplicaBuilder = leaderReplicaBuilder;
            return this;
        }

        Shard build(SolrCollection collection) {
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

    static class ReplicaBuilder {
        private String replicaName;
        private String coreName;
        private Replica.ReplicaType replicaType;
        private Replica.ReplicaState replicaState;
        private NodeBuilder replicaNode;

        ReplicaBuilder setReplicaName(String replicaName) {
            this.replicaName = replicaName;
            return this;
        }

        ReplicaBuilder setCoreName(String coreName) {
            this.coreName = coreName;
            return this;
        }

        ReplicaBuilder setReplicaType(Replica.ReplicaType replicaType) {
            this.replicaType = replicaType;
            return this;
        }

        ReplicaBuilder setReplicaState(Replica.ReplicaState replicaState) {
            this.replicaState = replicaState;
            return this;
        }

        ReplicaBuilder setReplicaNode(NodeBuilder replicaNode) {
            this.replicaNode = replicaNode;
            return this;
        }

        Replica build(Shard shard) {
            return new ClusterAbstractionsForTest.ReplicaImpl(replicaName, coreName, shard, replicaType, replicaState, replicaNode.build());
        }
    }

    static class NodeBuilder {
        private String nodeName = null;
        private Integer coreCount = null;
        private Long freeDiskGB = null;
        private Map<String, String> sysprops = null;
        private Map<String, Double> metrics = null;

        NodeBuilder setNodeName(String nodeName) {
            this.nodeName = nodeName;
            return this;
        }

        NodeBuilder setCoreCount(Integer coreCount) {
            this.coreCount = coreCount;
            return this;
        }

        NodeBuilder setFreeDiskGB(Long freeDiskGB) {
            this.freeDiskGB = freeDiskGB;
            return this;
        }

        NodeBuilder setSysprop(String key, String value) {
            if (sysprops == null) {
                sysprops = new HashMap<>();
            }
            String name = AttributeFetcherImpl.getSystemPropertySnitchTag(key);
            sysprops.put(name, value);
            return this;
        }

        NodeBuilder setMetric(AttributeFetcher.NodeMetricRegistry registry, String key, Double value) {
            if (metrics == null) {
                metrics = new HashMap<>();
            }
            String name = AttributeFetcherImpl.getMetricSnitchTag(key, registry);
            metrics.put(name, value);
            return this;
        }

        Integer getCoreCount() {
            return coreCount;
        }

        Long getFreeDiskGB() {
            return freeDiskGB;
        }

        Map<String, String> getSysprops() {
            return sysprops;
        }

        Map<String, Double> getMetrics() {
            return metrics;
        }

        Node build() {
            // It is ok to build a new instance each time, that instance does the right thing with equals() and hashCode()
            return new ClusterAbstractionsForTest.NodeImpl(nodeName);
        }
    }
}
