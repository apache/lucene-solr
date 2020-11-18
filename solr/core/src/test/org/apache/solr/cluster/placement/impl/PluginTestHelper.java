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

    /**
     * Builds the representation of shards for a collection, based on the number of shards and replicas for each to create.
     * The replicas are allocated to the provided nodes in a round robin way. The leader is set to the last replica of each shard.
     */
    static Map<String, Shard> createShardsAndReplicas(SolrCollection collection, int numShards, int numNrtReplicas, Set<Node> nodes) {
        Iterator<Node> nodeIterator = nodes.iterator();

        Map<String, Shard> shards = new HashMap<>();

        for (int s = 0; s < numShards; s++) {
            String shardName = collection.getName() + "_s" + s;

            ClusterAbstractionsForTest.ShardImpl shard = new ClusterAbstractionsForTest.ShardImpl(shardName, collection, Shard.ShardState.ACTIVE);

            Map<String, Replica> replicas = new HashMap<>();
            Replica leader = null;
            for (int r = 0; r < numNrtReplicas; r++) {
                String replicaName = shardName + "_r" + r;
                String coreName = replicaName + "_c";
                final Node node;
                if (!nodeIterator.hasNext()) {
                    nodeIterator = nodes.iterator();
                }
                // If the nodes set is empty, this call will fail
                node = nodeIterator.next();

                Replica replica = new ClusterAbstractionsForTest.ReplicaImpl(replicaName, coreName, shard, Replica.ReplicaType.NRT, Replica.ReplicaState.ACTIVE, node);

                replicas.put(replica.getReplicaName(), replica);
                leader = replica;
            }

            shard.setReplicas(replicas, leader);

            shards.put(shard.getShardName(), shard);
        }

        return shards;
    }
}
