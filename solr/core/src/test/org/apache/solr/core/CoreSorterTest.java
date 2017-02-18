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
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreSorter.CountsForEachShard;
import org.apache.solr.util.MockCoreContainer;

import static java.util.stream.Collectors.toList;
import static org.apache.solr.core.CoreSorter.getShardName;
import static org.mockito.Mockito.*;

public class CoreSorterTest extends SolrTestCaseJ4 {
  Map<String, Boolean> nodes = new LinkedHashMap<>();
  Set<String> liveNodes = new HashSet<>();

  public void testComparator() {
    List<CountsForEachShard> l = new ArrayList<>();
    //                           DOWN LIVE  MY
    l.add(new CountsForEachShard(1,     3,  1));
    l.add(new CountsForEachShard(0,     3,  2));
    l.add(new CountsForEachShard(0,     3,  3));
    l.add(new CountsForEachShard(0,     3,  4));
    l.add(new CountsForEachShard(1,     0,  2));
    l.add(new CountsForEachShard(1,     0,  1));
    l.add(new CountsForEachShard(2,     5,  1));
    l.add(new CountsForEachShard(2,     4,  2));
    l.add(new CountsForEachShard(2,     3,  3));

    List<CountsForEachShard> expected = Arrays.asList(
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

    for (int i = 0; i < 10; i++) {
      List<CountsForEachShard> copy = new ArrayList<>(l);
      Collections.shuffle(copy, random());
      Collections.sort(copy, CoreSorter.countsComparator);
      for (int j = 0; j < copy.size(); j++) {
        assertEquals(expected.get(j), copy.get(j));
      }
    }
  }

  public void testSort() throws Exception {
    CoreContainer mockCC = getMockContainer();
    MockCoreSorter coreSorter = (MockCoreSorter) new MockCoreSorter().init(mockCC);
    List<CoreDescriptor> copy = new ArrayList<>(coreSorter.getLocalCores());
    Collections.sort(copy, coreSorter::compare);
    List<CountsForEachShard> l = copy.stream()
        .map(CoreDescriptor::getCloudDescriptor)
        .map(it -> coreSorter.shardsVsReplicaCounts.get(getShardName(it)))
        .collect(toList());
    for (int i = 1; i < l.size(); i++) {
      CountsForEachShard curr = l.get(i);
      CountsForEachShard prev = l.get(i-1);
      assertTrue(CoreSorter.countsComparator.compare(prev, curr) < 1);
    }

    for (CountsForEachShard c : l) {
      System.out.println(c);
    }
  }

  private CoreContainer getMockContainer() {
    CoreContainer mockCC = mock(CoreContainer.class);
    ZkController mockZKC = mock(ZkController.class);
    ClusterState mockClusterState = mock(ClusterState.class);
    when(mockCC.isZooKeeperAware()).thenReturn(true);
    when(mockCC.getZkController()).thenReturn(mockZKC);
    when(mockClusterState.getLiveNodes()).thenReturn(liveNodes);
    when(mockZKC.getClusterState()).thenReturn(mockClusterState);
    return mockCC;
  }

  static class ReplicaInfo {
    final int coll, slice, replica;
    final String replicaName;
    CloudDescriptor cd;

    ReplicaInfo(int coll, int slice, int replica) {
      this.coll = coll;
      this.slice = slice;
      this.replica = replica;
      replicaName = "coll_" + coll + "_" + slice + "_" + replica;
      Properties p = new Properties();
      p.setProperty(CoreDescriptor.CORE_SHARD, "shard_" + slice);
      p.setProperty(CoreDescriptor.CORE_COLLECTION, "coll_" + slice);
      p.setProperty(CoreDescriptor.CORE_NODE_NAME, replicaName);
      cd = new CloudDescriptor(replicaName, p, null);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof ReplicaInfo) {
        ReplicaInfo replicaInfo = (ReplicaInfo) obj;
        return replicaInfo.replicaName.equals(replicaName);
      }
      return false;
    }


    @Override
    public int hashCode() {
      return replicaName.hashCode();
    }

    CloudDescriptor getCloudDescriptor() {
      return cd;

    }

    public Replica getReplica(String node) {
      return new Replica(replicaName, Utils.makeMap("core", replicaName, "node_name", node));
    }

    public boolean equals(String coll, String slice) {
      return cd.getCollectionName().equals(coll) && slice.equals(cd.getShardId());
    }
  }


  class MockCoreSorter extends CoreSorter {
    int numColls = 1 + random().nextInt(3);
    int numReplicas = 2 + random().nextInt(2);
    int numShards = 50 + random().nextInt(10);
    String myNodeName;
    Collection<CloudDescriptor> myCores = new ArrayList<>();
    List<CoreDescriptor> localCores = new ArrayList<>();

    Map<ReplicaInfo, String> replicaPositions = new LinkedHashMap<>();//replicaname vs. nodename

    public MockCoreSorter() {
      int totalNodes = 50 + random().nextInt(10);
      int myNode = random().nextInt(totalNodes);
      List<String> nodeNames = new ArrayList<>();
      for (int i = 0; i < totalNodes; i++) {
        String s = "192.168.1." + i + ":8983_solr";
        if (i == myNode) myNodeName = s;
        boolean on = random().nextInt(100) < 70;
        nodes.put(s,
            on);//70% chance that the node is up;
        nodeNames.add(s);
        if(on) liveNodes.add(s);
      }

      for (int i = 0; i < numColls; i++) {
        for (int j = 0; j < numShards; j++) {
          for (int k = 0; k < numReplicas; k++) {
            ReplicaInfo ri = new ReplicaInfo(i, j, k);
            replicaPositions.put(ri, nodeNames.get(random().nextInt(totalNodes)));
          }
        }
      }

      for (Map.Entry<ReplicaInfo, String> e : replicaPositions.entrySet()) {
        if (e.getValue().equals(myNodeName)) {
          myCores.add(e.getKey().getCloudDescriptor());
          localCores.add(new MockCoreContainer.MockCoreDescriptor() {
            @Override
            public CloudDescriptor getCloudDescriptor() {
              return e.getKey().getCloudDescriptor();
            }
          });
        }
      }
    }

    @Override
    String getNodeName() {
      return myNodeName;
    }

    @Override
    Collection<CloudDescriptor> getCloudDescriptors() {
      return myCores;

    }

    public List<CoreDescriptor> getLocalCores() {
      return localCores;
    }

    @Override
    Collection<Replica> getReplicas(ClusterState cs, String coll, String slice) {
      List<Replica> r = new ArrayList<>();
      for (Map.Entry<ReplicaInfo, String> e : replicaPositions.entrySet()) {
        if (e.getKey().equals(coll, slice)) {
          r.add(e.getKey().getReplica(e.getValue()));
        }
      }
      return r;
    }
  }


}
