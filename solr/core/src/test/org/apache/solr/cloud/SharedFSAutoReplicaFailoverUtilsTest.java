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

import java.io.Closeable;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.cloud.OverseerAutoReplicaFailoverThread.DownReplica;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.Utils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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

  private final static Pattern BLUEPRINT = Pattern.compile("([a-z])(\\d+)?(?:(['A','R','D','F']))?(\\*)?");

  private int buildNumber = 1;
  
  private List<Result> results;
  
  @Before
  public void setUp() throws Exception {
    super.setUp();
    results = new ArrayList<Result>();
  }
  
  @After
  public void tearDown() throws Exception {
    super.tearDown();
    for (Result result : results) {
      result.close();
    }
  }
  
  @Test
  public void testGetBestCreateUrlBasics() {
    Result result = buildClusterState("csr1R*r2", NODE1);
    String createUrl = OverseerAutoReplicaFailoverThread.getBestCreateUrl(result.reader, result.badReplica);
    assertNull("Should be no live node to failover to", createUrl);
    
    result = buildClusterState("csr1R*r2", NODE1, NODE2);
    createUrl = OverseerAutoReplicaFailoverThread.getBestCreateUrl(result.reader, result.badReplica);
    assertNull("Only failover candidate node already has a replica", createUrl);
    
    result = buildClusterState("csr1R*r2sr3", NODE1, NODE2, NODE3);
    createUrl = OverseerAutoReplicaFailoverThread.getBestCreateUrl(result.reader, result.badReplica);
    assertEquals("Node3 does not have a replica from the bad slice and should be the best choice", NODE3_URL, createUrl);
    
    result = buildClusterState("csr1R*r2-4sr3r4r5", NODE1, NODE2, NODE3);
    createUrl = OverseerAutoReplicaFailoverThread.getBestCreateUrl(result.reader, result.badReplica);
    assertTrue(createUrl.equals(NODE2_URL) || createUrl.equals(NODE3_URL));
    
    result = buildClusterState("csr1*r2r3sr3r3sr4", NODE1, NODE2, NODE3, NODE4);
    createUrl = OverseerAutoReplicaFailoverThread.getBestCreateUrl(result.reader, result.badReplica);
    assertEquals(NODE4_URL, createUrl);
    
    result = buildClusterState("csr1*r2sr3r3sr4sr4", NODE1, NODE2, NODE3, NODE4);
    createUrl = OverseerAutoReplicaFailoverThread.getBestCreateUrl(result.reader, result.badReplica);
    assertTrue(createUrl.equals(NODE3_URL) || createUrl.equals(NODE4_URL));
  }
  
  
  private static class Result implements Closeable {
    DownReplica badReplica;
    ZkStateReader reader;
    
    @Override
    public void close() throws IOException {
      reader.close();
    }
  }

  @Test
  public void testGetBestCreateUrlMultipleCollections() throws Exception {

    Result result = buildClusterState("csr*r2csr2", NODE1);
    String createUrl = OverseerAutoReplicaFailoverThread.getBestCreateUrl(result.reader, result.badReplica);
    assertEquals(null, createUrl);
    
    result = buildClusterState("csr*r2csr2", NODE1);
    createUrl = OverseerAutoReplicaFailoverThread.getBestCreateUrl(result.reader, result.badReplica);
    assertEquals(null, createUrl);
    
    result = buildClusterState("csr*r2csr2", NODE1, NODE2);
    createUrl = OverseerAutoReplicaFailoverThread.getBestCreateUrl(result.reader, result.badReplica);
    assertEquals(null, createUrl);
  }
  
  @Test
  public void testGetBestCreateUrlMultipleCollections2() {
    
    Result result = buildClusterState("csr*r2sr3cr2", NODE1);
    String createUrl = OverseerAutoReplicaFailoverThread.getBestCreateUrl(result.reader, result.badReplica);
    assertEquals(null, createUrl);
    
    result = buildClusterState("csr*r2sr3cr2", NODE1, NODE2, NODE3);
    createUrl = OverseerAutoReplicaFailoverThread.getBestCreateUrl(result.reader, result.badReplica);
    assertEquals(NODE3_URL, createUrl);
  }
  
  
  @Test
  public void testGetBestCreateUrlMultipleCollections3() {
    Result result = buildClusterState("csr5r1sr4r2sr3r6csr2*r6sr5r3sr4r3", NODE1, NODE4, NODE5, NODE6);
    String createUrl = OverseerAutoReplicaFailoverThread.getBestCreateUrl(result.reader, result.badReplica);
    assertEquals(NODE1_URL, createUrl);
  }
  
  @Test
  public void testGetBestCreateUrlMultipleCollections4() {
    Result result = buildClusterState("csr1r4sr3r5sr2r6csr5r6sr4r6sr5*r4", NODE6);
    String createUrl = OverseerAutoReplicaFailoverThread.getBestCreateUrl(result.reader, result.badReplica);
    assertEquals(NODE6_URL, createUrl);
  }
  
  @Test
  public void testFailOverToEmptySolrInstance() {
    Result result = buildClusterState("csr1*r1sr1csr1", NODE2);
    String createUrl = OverseerAutoReplicaFailoverThread.getBestCreateUrl(result.reader, result.badReplica);
    assertEquals(NODE2_URL, createUrl);
  }
  
  @Test
  public void testFavorForeignSlices() {
    Result result = buildClusterState("csr*sr2csr3r3", NODE2, NODE3);
    String createUrl = OverseerAutoReplicaFailoverThread.getBestCreateUrl(result.reader, result.badReplica);
    assertEquals(NODE3_URL, createUrl);
    
    result = buildClusterState("csr*sr2csr3r3r3r3r3r3r3", NODE2, NODE3);
    createUrl = OverseerAutoReplicaFailoverThread.getBestCreateUrl(result.reader, result.badReplica);
    assertEquals(NODE2_URL, createUrl);
  }
  
  @Test
  public void testCollectionMaxNodesPerShard() {
    Result result = buildClusterState("csr*sr2", 1, 1, NODE2);
    String createUrl = OverseerAutoReplicaFailoverThread.getBestCreateUrl(result.reader, result.badReplica);
    assertEquals(null, createUrl);
    
    result = buildClusterState("csr*sr2", 1, 2, NODE2);
    createUrl = OverseerAutoReplicaFailoverThread.getBestCreateUrl(result.reader, result.badReplica);
    assertEquals(NODE2_URL, createUrl);
    
    result = buildClusterState("csr*csr2r2", 1, 1, NODE2);
    createUrl = OverseerAutoReplicaFailoverThread.getBestCreateUrl(result.reader, result.badReplica);
    assertEquals(NODE2_URL, createUrl);
  }
  
  private Result buildClusterState(String string, String ... liveNodes) {
    return buildClusterState(string, 1, liveNodes);
  }
  
  private Result buildClusterState(String string, int replicationFactor, String ... liveNodes) {
    return buildClusterState(string, replicationFactor, 10, liveNodes);
  }
  
  /**
   * This method lets you construct a complex ClusterState object by using simple strings of letters.
   * 
   * c = collection, s = slice, r = replica, \d = node number (r2 means the replica is on node 2), 
   * state = [A,R,D,F], * = replica to replace, binds to the left.
   * 
   * For example:
   * csrr2rD*sr2csr
   * 
   * Creates:
   * 
   * 'csrr2rD*'
   * A collection, a shard, a replica on node 1 (the default) that is active (the default), a replica on node 2, and a replica on node 1
   * that has a state of down and is the replica we will be looking to put somewhere else (the *).
   * 
   * 'sr2'
   * Then, another shard that has a replica on node 2.
   * 
   * 'csr'
   * Then, another collection that has a shard with a single active replica on node 1.
   * 
   * Result:
   *        {
   *         "collection2":{
   *           "maxShardsPerNode":"1",
   *           "replicationFactor":"1",
   *           "shards":{"slice1":{
   *               "state":"active",
   *               "replicas":{"replica5":{
   *                   "state":"active",
   *                   "node_name":"baseUrl1_",
   *                   "base_url":"http://baseUrl1"}}}}},
   *         "collection1":{
   *           "maxShardsPerNode":"1",
   *           "replicationFactor":"1",
   *           "shards":{
   *             "slice1":{
   *               "state":"active",
   *               "replicas":{
   *                 "replica3 (bad)":{
   *                   "state":"down",
   *                   "node_name":"baseUrl1_",
   *                   "base_url":"http://baseUrl1"},
   *                 "replica2":{
   *                   "state":"active",
   *                   "node_name":"baseUrl2_",
   *                   "base_url":"http://baseUrl2"},
   *                 "replica1":{
   *                   "state":"active",
   *                   "node_name":"baseUrl1_",
   *                   "base_url":"http://baseUrl1"}}},
   *             "slice2":{
   *               "state":"active",
   *               "replicas":{"replica4":{
   *                   "state":"active",
   *                   "node_name":"baseUrl2_",
   *                   "base_url":"http://baseUrl2"}}}}}}
   * 
   */
  @SuppressWarnings("resource")
  private Result buildClusterState(String clusterDescription, int replicationFactor, int maxShardsPerNode, String ... liveNodes) {
    Result result = new Result();
    
    Map<String,Slice> slices = null;
    Map<String,Replica> replicas = null;
    Map<String,Object> collectionProps = new HashMap<>();
    collectionProps.put(ZkStateReader.MAX_SHARDS_PER_NODE, Integer.toString(maxShardsPerNode));
    collectionProps.put(ZkStateReader.REPLICATION_FACTOR, Integer.toString(replicationFactor));
    Map<String,DocCollection> collectionStates = new HashMap<>();
    DocCollection docCollection = null;
    Slice slice = null;
    int replicaCount = 1;
    
    Matcher m = BLUEPRINT.matcher(clusterDescription);
    while (m.find()) {
      Replica replica;
      switch (m.group(1)) {
        case "c":
          slices = new HashMap<>();
          docCollection = new DocCollection("collection" + (collectionStates.size() + 1), slices, collectionProps, null);
          collectionStates.put(docCollection.getName(), docCollection);
          break;
        case "s":
          replicas = new HashMap<>();
          slice = new Slice("slice" + (slices.size() + 1), replicas, null);
          slices.put(slice.getName(), slice);
          break;
        case "r":
          Map<String,Object> replicaPropMap = new HashMap<>();
          String node;

          node = m.group(2);
          
          if (node == null || node.trim().length() == 0) {
            node = "1";
          }
          
          Replica.State state = Replica.State.ACTIVE;
          String stateCode = m.group(3);

          if (stateCode != null) {
            switch (stateCode.charAt(0)) {
              case 'S':
                state = Replica.State.ACTIVE;
                break;
              case 'R':
                state = Replica.State.RECOVERING;
                break;
              case 'D':
                state = Replica.State.DOWN;
                break;
              case 'F':
                state = Replica.State.RECOVERY_FAILED;
                break;
              default:
                throw new IllegalArgumentException(
                    "Unexpected state for replica: " + stateCode);
            }
          }
          
          String nodeName = "baseUrl" + node + "_";
          String replicaName = "replica" + replicaCount++;
          
          if ("*".equals(m.group(4))) {
            replicaName += " (bad)";
          }
          
          replicaPropMap.put(ZkStateReader.NODE_NAME_PROP, nodeName);
          replicaPropMap.put(ZkStateReader.BASE_URL_PROP, "http://baseUrl" + node);
          replicaPropMap.put(ZkStateReader.STATE_PROP, state.toString());
          
          replica = new Replica(replicaName, replicaPropMap);
          
          if ("*".equals(m.group(4))) {
            result.badReplica = new DownReplica();
            result.badReplica.replica = replica;
            result.badReplica.slice = slice;
            result.badReplica.collection = docCollection;
          }
          
          replicas.put(replica.getName(), replica);
          break;
        default:
          break;
      }
    }
  
    // trunk briefly had clusterstate taking a zkreader :( this was required to work around that - leaving
    // until that issue is resolved.
    MockZkStateReader reader = new MockZkStateReader(null, collectionStates.keySet());
    ClusterState clusterState = new ClusterState(1, new HashSet<String>(Arrays.asList(liveNodes)), collectionStates);
    reader = new MockZkStateReader(clusterState, collectionStates.keySet());
    
    String json;
    try {
      json = new String(Utils.toJSON(clusterState), "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException("Unexpected");
    }
    System.err.println("build:" + buildNumber++);
    System.err.println(json);
    
    assert result.badReplica != null : "Is there no bad replica?";
    assert result.badReplica.slice != null : "Is there no bad replica?";
    
    result.reader = reader;
    
    results.add(result);

    return result;
  }
}
