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

import java.util.ArrayList;
import java.util.List;

import org.apache.solr.SolrTestCaseJ4;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.solr.cloud.ClusterStateMockUtil.buildClusterState;

public class SharedFSAutoReplicaFailoverUtilsTest extends SolrTestCaseJ4 {
  private static final String NODE6 = "baseUrl6_";
  private static final String NODE6_URL = "http://baseUrl6";
  
  private static final String NODE5 = "baseUrl5_";
  private static final String NODE5_URL = "http://baseUrl5";
  
  private static final String NODE4 = "baseUrl4_";
  private static final String NODE4_URL = "http://baseUrl4";

  private static final String NODE3 = "baseUrl3_";
  private static final String NODE3_URL = "http://baseUrl3";

  private static final String NODE2 = "baseUrl2_";
  private static final String NODE2_URL = "http://baseUrl2";

  private static final String NODE1 = "baseUrl1_";
  private static final String NODE1_URL = "http://baseUrl1";
  
  private List<ClusterStateMockUtil.Result> results;
  
  @Before
  public void setUp() throws Exception {
    super.setUp();
    results = new ArrayList<>();
  }
  
  @After
  public void tearDown() throws Exception {
    super.tearDown();
    for (ClusterStateMockUtil.Result result : results) {
      result.close();
    }
  }
  
  @Test
  public void testGetBestCreateUrlBasics() {
    ClusterStateMockUtil.Result result = buildClusterState(results, "csr1R*r2", NODE1);
    String createUrl = OverseerAutoReplicaFailoverThread.getBestCreateUrl(result.reader, result.badReplica, null);
    assertNull("Should be no live node to failover to", createUrl);
    
    result = buildClusterState(results, "csr1R*r2", NODE1, NODE2);
    createUrl = OverseerAutoReplicaFailoverThread.getBestCreateUrl(result.reader, result.badReplica, null);
    assertNull("Only failover candidate node already has a replica", createUrl);
    
    result = buildClusterState(results, "csr1R*r2sr3", NODE1, NODE2, NODE3);
    createUrl = OverseerAutoReplicaFailoverThread.getBestCreateUrl(result.reader, result.badReplica, null);
    assertEquals("Node3 does not have a replica from the bad slice and should be the best choice", NODE3_URL, createUrl);

    result = buildClusterState(results, "csr1R*r2Fsr3r4r5", NODE1, NODE2, NODE3);
    createUrl = OverseerAutoReplicaFailoverThread.getBestCreateUrl(result.reader, result.badReplica, null);
    assertTrue(createUrl.equals(NODE3_URL));

    result = buildClusterState(results, "csr1*r2r3sr3r3sr4", NODE1, NODE2, NODE3, NODE4);
    createUrl = OverseerAutoReplicaFailoverThread.getBestCreateUrl(result.reader, result.badReplica, null);
    assertEquals(NODE4_URL, createUrl);
    
    result = buildClusterState(results, "csr1*r2sr3r3sr4sr4", NODE1, NODE2, NODE3, NODE4);
    createUrl = OverseerAutoReplicaFailoverThread.getBestCreateUrl(result.reader, result.badReplica, null);
    assertTrue(createUrl.equals(NODE3_URL) || createUrl.equals(NODE4_URL));
  }

  @Test
  public void testGetBestCreateUrlMultipleCollections() throws Exception {

    ClusterStateMockUtil.Result result = buildClusterState(results, "csr*r2csr2", NODE1);
    String createUrl = OverseerAutoReplicaFailoverThread.getBestCreateUrl(result.reader, result.badReplica, null);
    assertNull(createUrl);

    result = buildClusterState(results, "csr*r2csr2", NODE1);
    createUrl = OverseerAutoReplicaFailoverThread.getBestCreateUrl(result.reader, result.badReplica, null);
    assertNull(createUrl);

    result = buildClusterState(results, "csr*r2csr2", NODE1, NODE2);
    createUrl = OverseerAutoReplicaFailoverThread.getBestCreateUrl(result.reader, result.badReplica, null);
    assertNull(createUrl);
  }

  @Test
  public void testGetBestCreateUrlMultipleCollections2() {
    
    ClusterStateMockUtil.Result result = buildClusterState(results, "csr*r2sr3cr2", NODE1);
    String createUrl = OverseerAutoReplicaFailoverThread.getBestCreateUrl(result.reader, result.badReplica, null);
    assertNull(createUrl);

    result = buildClusterState(results, "csr*r2sr3cr2", NODE1, NODE2, NODE3);
    createUrl = OverseerAutoReplicaFailoverThread.getBestCreateUrl(result.reader, result.badReplica, null);
    assertEquals(NODE3_URL, createUrl);
  }
  
  
  @Test
  public void testGetBestCreateUrlMultipleCollections3() {
    ClusterStateMockUtil.Result result = buildClusterState(results, "csr5r1sr4r2sr3r6csr2*r6sr5r3sr4r3", NODE1, NODE4, NODE5, NODE6);
    String createUrl = OverseerAutoReplicaFailoverThread.getBestCreateUrl(result.reader, result.badReplica, null);
    assertEquals(NODE1_URL, createUrl);
  }
  
  @Test
  public void testGetBestCreateUrlMultipleCollections4() {
    ClusterStateMockUtil.Result result = buildClusterState(results, "csr1r4sr3r5sr2r6csr5r6sr4r6sr5*r4", NODE6);
    String createUrl = OverseerAutoReplicaFailoverThread.getBestCreateUrl(result.reader, result.badReplica, null);
    assertEquals(NODE6_URL, createUrl);
  }
  
  @Test
  public void testFailOverToEmptySolrInstance() {
    ClusterStateMockUtil.Result result = buildClusterState(results, "csr1*r1sr1csr1", NODE2);
    String createUrl = OverseerAutoReplicaFailoverThread.getBestCreateUrl(result.reader, result.badReplica, null);
    assertEquals(NODE2_URL, createUrl);
  }
  
  @Test
  public void testFavorForeignSlices() {
    ClusterStateMockUtil.Result result = buildClusterState(results, "csr*sr2csr3r3", NODE2, NODE3);
    String createUrl = OverseerAutoReplicaFailoverThread.getBestCreateUrl(result.reader, result.badReplica, null);
    assertEquals(NODE3_URL, createUrl);
    
    result = buildClusterState(results, "csr*sr2csr3r3r3r3r3r3r3", NODE2, NODE3);
    createUrl = OverseerAutoReplicaFailoverThread.getBestCreateUrl(result.reader, result.badReplica, null);
    assertEquals(NODE2_URL, createUrl);
  }

  @Test
  public void testCollectionMaxNodesPerShard() {
    ClusterStateMockUtil.Result result = buildClusterState(results, "csr*sr2", 1, 1, NODE2);
    String createUrl = OverseerAutoReplicaFailoverThread.getBestCreateUrl(result.reader, result.badReplica, null);
    assertNull(createUrl);

    result = buildClusterState(results, "csr*sr2", 1, 2, NODE2);
    createUrl = OverseerAutoReplicaFailoverThread.getBestCreateUrl(result.reader, result.badReplica, null);
    assertEquals(NODE2_URL, createUrl);

    result = buildClusterState(results, "csr*csr2r2", 1, 1, NODE2);
    createUrl = OverseerAutoReplicaFailoverThread.getBestCreateUrl(result.reader, result.badReplica, null);
    assertEquals(NODE2_URL, createUrl);
  }

  @Test
  public void testMaxCoresPerNode() {
    ClusterStateMockUtil.Result result = buildClusterState(results, "csr*sr2", 1, 1, NODE2);
    String createUrl = OverseerAutoReplicaFailoverThread.getBestCreateUrl(result.reader, result.badReplica, 1);
    assertNull(createUrl);

    createUrl = OverseerAutoReplicaFailoverThread.getBestCreateUrl(result.reader, result.badReplica, 2);
    assertNull(createUrl);

    result = buildClusterState(results, "csr*sr2", 1, 2, NODE2);
    createUrl = OverseerAutoReplicaFailoverThread.getBestCreateUrl(result.reader, result.badReplica, 2);
    assertEquals(NODE2_URL, createUrl);

    result = buildClusterState(results, "csr*sr2sr3sr4", 1, 1, NODE2, NODE3, NODE4);
    createUrl = OverseerAutoReplicaFailoverThread.getBestCreateUrl(result.reader, result.badReplica, 1);
    assertNull(createUrl);

    createUrl = OverseerAutoReplicaFailoverThread.getBestCreateUrl(result.reader, result.badReplica, 2);
    assertNull(createUrl);

    result = buildClusterState(results, "csr*sr2sr3sr4", 1, 2, NODE2, NODE3, NODE4);
    createUrl = OverseerAutoReplicaFailoverThread.getBestCreateUrl(result.reader, result.badReplica, 2);
    assertTrue(createUrl.equals(NODE3_URL) || createUrl.equals(NODE4_URL));
  }
}
