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

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.junit.Test;

/**
 * Tests for {@link ClusterStateMockUtil}
 */
@SolrTestCaseJ4.SuppressSSL // tests expect http scheme
public class ClusterStateMockUtilTest extends SolrTestCaseJ4 {

  @Test
  public void testBuildClusterState_Simple() {
    try (ZkStateReader zkStateReader = ClusterStateMockUtil.buildClusterState("csr", "baseUrl1:8983_")) {
      ClusterState clusterState = zkStateReader.getClusterState();
      assertNotNull(clusterState);
      assertEquals(1, clusterState.getCollectionStates().size());
      DocCollection collection1 = clusterState.getCollectionOrNull("collection1");
      assertNotNull(collection1);
      assertEquals(DocRouter.DEFAULT, collection1.getRouter());
      assertEquals(1, collection1.getActiveSlices().size());
      assertEquals(1, collection1.getSlices().size());
      Slice slice1 = collection1.getSlice("slice1");
      assertNotNull(slice1);
      assertEquals(1, slice1.getReplicas().size());
      Replica replica1 = slice1.getReplica("replica1");
      assertNotNull(replica1);
      assertEquals("baseUrl1:8983_", replica1.getNodeName());
      assertEquals("slice1_replica1", replica1.getCoreName());
      assertEquals("http://baseUrl1:8983", replica1.getBaseUrl());
      assertEquals("http://baseUrl1:8983/slice1_replica1/", replica1.getCoreUrl());
      assertEquals(Replica.State.ACTIVE, replica1.getState());
      assertEquals(Replica.Type.NRT, replica1.getType());
    }
  }

  @Test
  public void testBuildClusterState_ReplicaTypes() {
    try (ZkStateReader zkStateReader = ClusterStateMockUtil.buildClusterState("csntp", "baseUrl1:8983_")) {
      ClusterState clusterState = zkStateReader.getClusterState();
      assertNotNull(clusterState);
      assertEquals(1, clusterState.getCollectionStates().size());
      DocCollection collection1 = clusterState.getCollectionOrNull("collection1");
      assertNotNull(collection1);
      assertEquals(DocRouter.DEFAULT, collection1.getRouter());
      assertEquals(1, collection1.getActiveSlices().size());
      assertEquals(1, collection1.getSlices().size());
      Slice slice1 = collection1.getSlice("slice1");
      assertNotNull(slice1);
      assertEquals(3, slice1.getReplicas().size());
      assertEquals(1, slice1.getReplicas(replica -> replica.getType() == Replica.Type.NRT).size());
      assertEquals(1, slice1.getReplicas(replica -> replica.getType() == Replica.Type.TLOG).size());
      assertEquals(1, slice1.getReplicas(replica -> replica.getType() == Replica.Type.PULL).size());
    }
  }

  @Test
  public void testBuildClusterState_ReplicaStateAndType() {
    try (ZkStateReader zkStateReader = ClusterStateMockUtil.buildClusterState("csrStRpDnF", "baseUrl1:8983_")) {
      ClusterState clusterState = zkStateReader.getClusterState();
      assertNotNull(clusterState);
      assertEquals(1, clusterState.getCollectionStates().size());
      DocCollection collection1 = clusterState.getCollectionOrNull("collection1");
      assertNotNull(collection1);
      assertEquals(DocRouter.DEFAULT, collection1.getRouter());
      assertEquals(1, collection1.getActiveSlices().size());
      assertEquals(1, collection1.getSlices().size());
      Slice slice1 = collection1.getSlice("slice1");
      assertNotNull(slice1);
      assertEquals(4, slice1.getReplicas().size());
      assertEquals(1, slice1.getReplicas(replica -> replica.getType() == Replica.Type.NRT && replica.getState() == Replica.State.ACTIVE).size());
      assertEquals(1, slice1.getReplicas(replica -> replica.getType() == Replica.Type.NRT && replica.getState() == Replica.State.RECOVERY_FAILED).size());
      assertEquals(1, slice1.getReplicas(replica -> replica.getType() == Replica.Type.TLOG && replica.getState() == Replica.State.RECOVERING).size());
      assertEquals(1, slice1.getReplicas(replica -> replica.getType() == Replica.Type.PULL && replica.getState() == Replica.State.DOWN).size());
    }
  }
}