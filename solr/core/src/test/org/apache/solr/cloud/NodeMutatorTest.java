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
package org.apache.solr.cloud;

import java.io.IOException;
import java.util.List;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.SolrTestCaseJ4Test;
import org.apache.solr.cloud.overseer.NodeMutator;
import org.apache.solr.cloud.overseer.ZkWriteCommand;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.junit.Test;

@SolrTestCaseJ4.SuppressSSL // tests compare for http:
public class NodeMutatorTest extends SolrTestCaseJ4Test {

  private static final String NODE3 = "baseUrl3:8985_";
  private static final String NODE3_URL = "http://baseUrl3:8985";

  private static final String NODE2 = "baseUrl2:8984_";
  private static final String NODE2_URL = "http://baseUrl2:8984";

  private static final String NODE1 = "baseUrl1:8983_";
  private static final String NODE1_URL = "http://baseUrl1:8983";

  @Test
  public void downNodeReportsAllImpactedCollectionsAndNothingElse() throws IOException {
    NodeMutator nm = new NodeMutator(null);

    //We use 2 nodes with maxShardsPerNode as 1
    //Collection1: 2 shards X 1 replica = replica1 on node1 and replica2 on node2
    //Collection2: 1 shard X 1 replica = replica1 on node2
    ZkStateReader reader = ClusterStateMockUtil.buildClusterState("csrr2rDcsr2", 1, 1, NODE1, NODE2);
    ClusterState clusterState = reader.getClusterState();
    assertEquals(clusterState.getCollection("collection1").getReplica("replica1").getBaseUrl(), NODE1_URL);
    assertEquals(clusterState.getCollection("collection1").getReplica("replica2").getBaseUrl(), NODE2_URL);
    assertEquals(clusterState.getCollection("collection2").getReplica("replica4").getBaseUrl(), NODE2_URL);

    ZkNodeProps props = new ZkNodeProps(ZkStateReader.NODE_NAME_PROP, NODE1);
    List<ZkWriteCommand> writes = nm.downNode(clusterState, props);
    assertEquals(writes.size(), 1);
    assertEquals(writes.get(0).name, "collection1");
    assertEquals(writes.get(0).collection.getReplica("replica1").getState(), Replica.State.DOWN);
    assertEquals(writes.get(0).collection.getReplica("replica2").getState(), Replica.State.ACTIVE);
    reader.close();

    //We use 3 nodes with maxShardsPerNode as 1
    //Collection1: 2 shards X 1 replica = replica1 on node1 and replica2 on node2
    //Collection2: 1 shard X 1 replica = replica1 on node2
    //Collection3: 1 shard X 3 replica = replica1 on node1 , replica2 on node2, replica3 on node3
    reader = ClusterStateMockUtil.buildClusterState("csrr2rDcsr2csr1r2r3", 1, 1, NODE1, NODE2, NODE3);
    clusterState = reader.getClusterState();
    assertEquals(clusterState.getCollection("collection1").getReplica("replica1").getBaseUrl(), NODE1_URL);
    assertEquals(clusterState.getCollection("collection1").getReplica("replica2").getBaseUrl(), NODE2_URL);

    assertEquals(clusterState.getCollection("collection2").getReplica("replica4").getBaseUrl(), NODE2_URL);

    assertEquals(clusterState.getCollection("collection3").getReplica("replica5").getBaseUrl(), NODE1_URL);
    assertEquals(clusterState.getCollection("collection3").getReplica("replica6").getBaseUrl(), NODE2_URL);
    assertEquals(clusterState.getCollection("collection3").getReplica("replica7").getBaseUrl(), NODE3_URL);

    writes = nm.downNode(clusterState, props);
    assertEquals(writes.size(), 2);
    for (ZkWriteCommand write : writes) {
      if (write.name.equals("collection1")) {
        assertEquals(write.collection.getReplica("replica1").getState(), Replica.State.DOWN);
        assertEquals(write.collection.getReplica("replica2").getState(), Replica.State.ACTIVE);
      } else if (write.name.equals("collection3")) {
        assertEquals(write.collection.getReplica("replica5").getState(), Replica.State.DOWN);
        assertEquals(write.collection.getReplica("replica6").getState(), Replica.State.ACTIVE);
        assertEquals(write.collection.getReplica("replica7").getState(), Replica.State.ACTIVE);
      } else {
        fail("No other collection needs to be changed");
      }
    }
    reader.close();
  }
}
