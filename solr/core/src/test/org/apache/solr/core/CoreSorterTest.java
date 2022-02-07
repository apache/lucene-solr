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
package org.apache.solr.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.util.Utils;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.core.CoreSorter.CountsForEachShard;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SolrTestCaseJ4.SuppressSSL
public class CoreSorterTest extends SolrTestCaseJ4 {

  private static final List<CountsForEachShard> inputCounts = Arrays.asList(
      //                     DOWN LIVE  MY
      new CountsForEachShard(1, 3, 1),
      new CountsForEachShard(0, 3, 2),
      new CountsForEachShard(0, 3, 3),
      new CountsForEachShard(0, 3, 4),
      new CountsForEachShard(1, 0, 2),
      new CountsForEachShard(1, 0, 1),
      new CountsForEachShard(2, 5, 1),
      new CountsForEachShard(2, 4, 2),
      new CountsForEachShard(2, 3, 3)
  );

  private static final List<CountsForEachShard> expectedCounts = Arrays.asList(
      new CountsForEachShard(0, 3, 2),
      new CountsForEachShard(0, 3, 3),
      new CountsForEachShard(0, 3, 4),
      new CountsForEachShard(1, 3, 1),
      new CountsForEachShard(2, 5, 1),
      new CountsForEachShard(2, 4, 2),
      new CountsForEachShard(2, 3, 3),
      new CountsForEachShard(1, 0, 1),
      new CountsForEachShard(1, 0, 2)
  );

  @Test
  public void testComparator() {
    for (int i = 0; i < 10; i++) {
      List<CountsForEachShard> copy = new ArrayList<>(inputCounts);
      Collections.shuffle(copy, random());
      Collections.sort(copy, CoreSorter.countsComparator);
      for (int j = 0; j < copy.size(); j++) {
        assertEquals(expectedCounts.get(j), copy.get(j));
      }
    }
  }

  @Test
  @SuppressWarnings({"unchecked"})
  public void integrationTest() {
    assumeWorkingMockito();

    List<CountsForEachShard> perShardCounts = new ArrayList<>(inputCounts);
    Collections.shuffle(perShardCounts, random());

    // compute nodes, some live, some down
    final int maxNodesOfAType = perShardCounts.stream() // not too important how many we have, but lets have plenty
        .mapToInt(c -> c.totalReplicasInLiveNodes + c.totalReplicasInDownNodes + c.myReplicas).max().getAsInt();
    List<String> liveNodes = IntStream.range(0, maxNodesOfAType).mapToObj(i -> "192.168.0." + i + ":8983_").collect(Collectors.toList());
    Collections.shuffle(liveNodes, random());
    String thisNode = liveNodes.get(0);
    List<String> otherLiveNodes = liveNodes.subList(1, liveNodes.size());
    List<String> downNodes = IntStream.range(0, maxNodesOfAType).mapToObj(i -> "192.168.1." + i + ":8983_").collect(Collectors.toList());

    // divide into two collections
    int numCol1 = random().nextInt(perShardCounts.size());
    Map<String,List<CountsForEachShard>> collToCounts = new HashMap<>();
    collToCounts.put("col1", perShardCounts.subList(0, numCol1));
    collToCounts.put("col2", perShardCounts.subList(numCol1, perShardCounts.size()));

    Map<String,DocCollection> collToState = new HashMap<>();
    Map<CountsForEachShard, List<CoreDescriptor>> myCountsToDescs = new HashMap<>();
    for (Map.Entry<String, List<CountsForEachShard>> entry : collToCounts.entrySet()) {
      String collection = entry.getKey();
      List<CountsForEachShard> collCounts = entry.getValue();
      Map<String, Slice> sliceMap = new HashMap<>(collCounts.size());
      for (CountsForEachShard shardCounts : collCounts) {
        String slice = "s" + shardCounts.hashCode();
        List<Replica> replicas = new ArrayList<>();
        for (int myRepNum = 0; myRepNum < shardCounts.myReplicas; myRepNum++) {
          addNewReplica(replicas, collection, slice, Collections.singletonList(thisNode));
          // save this mapping for later
          myCountsToDescs.put(shardCounts, replicas.stream().map(this::newCoreDescriptor).collect(Collectors.toList()));
        }
        for (int myRepNum = 0; myRepNum < shardCounts.totalReplicasInLiveNodes; myRepNum++) {
          addNewReplica(replicas, collection, slice, otherLiveNodes);
        }
        for (int myRepNum = 0; myRepNum < shardCounts.totalReplicasInDownNodes; myRepNum++) {
          addNewReplica(replicas, collection, slice, downNodes);
        }
        Map<String, Replica> replicaMap = replicas.stream().collect(Collectors.toMap(Replica::getName, Function.identity()));
        sliceMap.put(slice, new Slice(slice, replicaMap, map(), collection));
      }
      @SuppressWarnings({"unchecked"})
      DocCollection col = new DocCollection(collection, sliceMap, map(), DocRouter.DEFAULT);
      collToState.put(collection, col);
    }
    // reverse map
    Map<CoreDescriptor, CountsForEachShard> myDescsToCounts = new HashMap<>();
    for (Map.Entry<CountsForEachShard, List<CoreDescriptor>> entry : myCountsToDescs.entrySet()) {
      for (CoreDescriptor descriptor : entry.getValue()) {
        CountsForEachShard prev = myDescsToCounts.put(descriptor, entry.getKey());
        assert prev == null; // sanity check
      }
    }

    assert myCountsToDescs.size() == perShardCounts.size(); // just a sanity check

    CoreContainer mockCC = mock(CoreContainer.class);
    {
      when(mockCC.isZooKeeperAware()).thenReturn(true);

      ZkController mockZKC = mock(ZkController.class);
      when(mockCC.getZkController()).thenReturn(mockZKC);
      {
        ClusterState mockClusterState = mock(ClusterState.class);
        when(mockZKC.getClusterState()).thenReturn(mockClusterState);
        {
          when(mockClusterState.getLiveNodes()).thenReturn(new HashSet<>(liveNodes));
          for (Map.Entry<String, DocCollection> entry : collToState.entrySet()) {
            when(mockClusterState.getCollectionOrNull(entry.getKey())).thenReturn(entry.getValue());
          }
        }
      }

      NodeConfig mockNodeConfig = mock(NodeConfig.class);
      when(mockNodeConfig.getNodeName()).thenReturn(thisNode);
      when(mockCC.getNodeConfig()).thenReturn(mockNodeConfig);

    }

    List<CoreDescriptor> myDescs = new ArrayList<>(myDescsToCounts.keySet());
    for (int i = 0; i < 10; i++) {
      Collections.shuffle(myDescs, random());

      List<CoreDescriptor> resultDescs = CoreSorter.sortCores(mockCC, myDescs);
      // map descriptors back to counts, removing consecutive duplicates
      List<CountsForEachShard> resultCounts = new ArrayList<>();
      CountsForEachShard lastCounts = null;
      for (CoreDescriptor resultDesc : resultDescs) {
        CountsForEachShard counts = myDescsToCounts.get(resultDesc);
        if (counts != lastCounts) {
          resultCounts.add(counts);
        }
        lastCounts = counts;
      }
      assertEquals(expectedCounts, resultCounts);
    }
  }

  private CoreDescriptor newCoreDescriptor(Replica r) {
    @SuppressWarnings({"unchecked"})
    Map<String,String> props = map(
        CoreDescriptor.CORE_SHARD, r.getSlice(),
        CoreDescriptor.CORE_COLLECTION, r.getCollection(),
        CoreDescriptor.CORE_NODE_NAME, r.getNodeName()
    );
    return new CoreDescriptor(r.getCoreName(), TEST_PATH(), props , null, mock(ZkController.class));
  }

  protected Replica addNewReplica(List<Replica> replicaList, String collection, String slice, List<String> possibleNodes) {
    String replica = "r" + replicaList.size();
    String node = possibleNodes.get(random().nextInt(possibleNodes.size())); // place on a random node
    @SuppressWarnings({"unchecked"})
    Replica r = new Replica(replica, map("core", replica, "node_name", node, ZkStateReader.BASE_URL_PROP, Utils.getBaseUrlForNodeName(node, "http")), collection, slice);
    replicaList.add(r);
    return r;
  }

}
