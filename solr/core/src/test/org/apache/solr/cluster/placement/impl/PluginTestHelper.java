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

import org.apache.solr.cluster.Node;
import org.apache.solr.cluster.Replica;
import org.apache.solr.cluster.Shard;
import org.apache.solr.cluster.SolrCollection;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class PluginTestHelper {

    static ClusterAbstractionsForTest.SolrCollectionImpl createCollection(String name, Map<String, String> properties,
                                           int numShards, int nrtReplicas, int tlogReplicas, int pullReplicas, Set<Node> nodes) {
        ClusterAbstractionsForTest.SolrCollectionImpl solrCollection = new ClusterAbstractionsForTest.SolrCollectionImpl(name, properties);
        Map<String, Shard> shards = createShardsAndReplicas(solrCollection, numShards, nrtReplicas, tlogReplicas, pullReplicas, nodes);
        solrCollection.setShards(shards);
        return solrCollection;
    }

    /**
     * Builds the representation of shards for a collection, based on the number of shards and replicas for each to create.
     * The replicas are allocated to the provided nodes in a round robin way. The leader is set to the last replica of each shard.
     */
    static Map<String, Shard> createShardsAndReplicas(SolrCollection collection, int numShards,
                                                      int nrtReplicas, int tlogReplicas, int pullReplicas,
                                                      Set<Node> nodes) {
        Iterator<Node> nodeIterator = nodes.iterator();

        Map<String, Shard> shards = new HashMap<>();

        for (int s = 0; s < numShards; s++) {
            // "traditional" shard name
            String shardName = "shard" + (s + 1);

            ClusterAbstractionsForTest.ShardImpl shard = new ClusterAbstractionsForTest.ShardImpl(shardName, collection, Shard.ShardState.ACTIVE);

            Map<String, Replica> replicas = new HashMap<>();

            Replica leader = null;
            int totalReplicas = nrtReplicas + tlogReplicas + pullReplicas;
            for (int r = 0; r < totalReplicas; r++) {
                Replica.ReplicaType type;
                String suffix;
                if (r < nrtReplicas) {
                    type = Replica.ReplicaType.NRT;
                    suffix = "n";
                } else if (r < nrtReplicas + tlogReplicas) {
                    type = Replica.ReplicaType.TLOG;
                    suffix = "t";
                } else {
                    type = Replica.ReplicaType.PULL;
                    suffix = "p";
                }
                String replicaName = shardName + "_replica_" + suffix + r;
                String coreName = replicaName + "_c";
                final Node node;
                if (!nodeIterator.hasNext()) {
                    nodeIterator = nodes.iterator();
                }
                // If the nodes set is empty, this call will fail
                node = nodeIterator.next();

                Replica replica = new ClusterAbstractionsForTest.ReplicaImpl(replicaName, coreName, shard, type, Replica.ReplicaState.ACTIVE, node);

                replicas.put(replica.getReplicaName(), replica);
                if (replica.getType() == Replica.ReplicaType.NRT) {
                    leader = replica;
                }
            }

            shard.setReplicas(replicas, leader);

            shards.put(shard.getShardName(), shard);
        }

        return shards;
    }
}
