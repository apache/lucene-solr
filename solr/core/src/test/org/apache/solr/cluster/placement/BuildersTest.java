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

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.cluster.Cluster;
import org.apache.solr.cluster.Node;
import org.apache.solr.cluster.Shard;
import org.apache.solr.cluster.SolrCollection;
import org.junit.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.apache.solr.cluster.placement.Builders.*;

/**
 *
 */
public class BuildersTest extends SolrTestCaseJ4 {

  @Test
  public void testClusterBuilder() throws Exception {
    int NUM_NODES = 3;
    int NUM_SHARDS = 2;
    int NUM_NRT_REPLICAS = 2;
    String collectionName = "test";
    ClusterBuilder clusterBuilder = newClusterBuilder()
        .initializeLiveNodes(NUM_NODES);
    CollectionBuilder collectionBuilder = newCollectionBuilder(collectionName)
        .initializeShardsReplicas(NUM_SHARDS,
            NUM_NRT_REPLICAS,
            NUM_NRT_REPLICAS + 1,
            NUM_NRT_REPLICAS + 2,
            clusterBuilder.getLiveNodeBuilders(),
            List.of(10, 20));
    clusterBuilder.addCollection(collectionBuilder);
    Cluster cluster = clusterBuilder.build();
    assertEquals("number of nodes", NUM_NODES, cluster.getLiveNodes().size());
    SolrCollection collection = cluster.getCollection(collectionName);
    assertNotNull("collection", collection);
    assertEquals("shards", 2, collection.getShardNames().size());
    for (String shardName : collection.getShardNames()) {
      Shard shard = collection.getShard(shardName);
      assertNotNull("shard leader", shard.getLeader());
      int[] counts = new int[3];
      shard.iterator().forEachRemaining(r -> {
        switch (r.getType()) {
          case NRT:
            counts[0]++;
            break;
          case TLOG:
            counts[1]++;
            break;
          case PULL:
            counts[2]++;
        }
      });
      assertEquals("numNrt", NUM_NRT_REPLICAS, counts[0]);
      assertEquals("numTlog", NUM_NRT_REPLICAS + 1, counts[1]);
      assertEquals("numPull", NUM_NRT_REPLICAS + 2, counts[2]);
    }
    // AttributeFetcher
    AttributeFetcher attributeFetcher = clusterBuilder.buildAttributeFetcher();
    attributeFetcher
        .fetchFrom(cluster.getLiveNodes())
        .requestNodeCoreCount()
        .requestNodeDiskType()
        .requestNodeFreeDisk()
        .requestNodeTotalDisk()
        .requestCollectionMetrics(collection, Set.of());
    AttributeValues attributeValues = attributeFetcher.fetchAttributes();
    for (Node node : cluster.getLiveNodes()) {
      Optional<Integer> coreCount = attributeValues.getCoresCount(node);
      assertTrue("coreCount present", coreCount.isPresent());
      Optional<AttributeFetcher.DiskHardwareType> diskType = attributeValues.getDiskType(node);
      assertTrue("diskType present", diskType.isPresent());
      Optional<Long> diskOpt = attributeValues.getFreeDisk(node);
      assertTrue("freeDisk", diskOpt.isPresent());
      diskOpt = attributeValues.getTotalDisk(node);
      assertTrue("totalDisk", diskOpt.isPresent());
    }
    Optional<CollectionMetrics> collectionMetricsOpt = attributeValues.getCollectionMetrics(collectionName);
    assertTrue("collectionMetrics present", collectionMetricsOpt.isPresent());
    CollectionMetrics collectionMetrics = collectionMetricsOpt.get();
    for (String shardName : collection.getShardNames()) {
      Optional<ShardMetrics> shardMetricsOpt = collectionMetrics.getShardMetrics(shardName);
      assertTrue("shard metrics", shardMetricsOpt.isPresent());
      ShardMetrics shardMetrics = shardMetricsOpt.get();
      Optional<ReplicaMetrics> replicaMetricsOpt = shardMetrics.getLeaderMetrics();
      assertTrue("leader metrics", replicaMetricsOpt.isPresent());
      ReplicaMetrics leaderMetrics = replicaMetricsOpt.get();
      if (shardName.endsWith("1")) {
        assertEquals("size", 10, leaderMetrics.getReplicaSizeGB());
      } else {
        assertEquals("size", 20, leaderMetrics.getReplicaSizeGB());
      }
      Shard shard = collection.getShard(shardName);
      shard.iterator().forEachRemaining(r -> {
        Optional<ReplicaMetrics> metricsOpt = shardMetrics.getReplicaMetrics(r.getReplicaName());
        assertTrue("replica metrics", metricsOpt.isPresent());
        ReplicaMetrics metrics = metricsOpt.get();
        if (shardName.endsWith("1")) {
          assertEquals("size", 10, metrics.getReplicaSizeGB());
        } else {
          assertEquals("size", 20, metrics.getReplicaSizeGB());
        }
      });
    }
  }
}
