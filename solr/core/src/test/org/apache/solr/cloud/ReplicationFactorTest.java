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

import java.io.File;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.util.NamedList;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Tests a client application's ability to get replication factor
 * information back from the cluster after an add or update.
 */
@Slow
@SuppressSSL(bugUrl = "https://issues.apache.org/jira/browse/SOLR-5776")
@LuceneTestCase.BadApple(bugUrl = "https://issues.apache.org/jira/browse/SOLR-6944")
public class ReplicationFactorTest extends AbstractFullDistribZkTestBase {
  
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public ReplicationFactorTest() {
    super();
    sliceCount = 3;
    fixShardCount(3);
  }
  
  /**
   * Overrides the parent implementation so that we can configure a socket proxy
   * to sit infront of each Jetty server, which gives us the ability to simulate
   * network partitions without having to fuss with IPTables (which is not very
   * cross platform friendly).
   */
  @Override
  public JettySolrRunner createJetty(File solrHome, String dataDir,
      String shardList, String solrConfigOverride, String schemaOverride, Replica.Type replicaType)
      throws Exception {

    return createProxiedJetty(solrHome, dataDir, shardList, solrConfigOverride, schemaOverride, replicaType);
  }
  
  @Test
  public void test() throws Exception {
    log.info("replication factor test running");
    waitForThingsToLevelOut(30000);
    
    // test a 1x3 collection
    log.info("Testing replication factor handling for repfacttest_c8n_1x3");
    testRf3();

    waitForThingsToLevelOut(30000);

    // test handling when not using direct updates
    log.info("Now testing replication factor handling for repfacttest_c8n_2x2");
    testRf2NotUsingDirectUpdates();
        
    waitForThingsToLevelOut(30000);
    log.info("replication factor testing complete! final clusterState is: "+
        cloudClient.getZkStateReader().getClusterState());    
  }
  
  protected void testRf2NotUsingDirectUpdates() throws Exception {
    int numShards = 2;
    int replicationFactor = 2;
    int maxShardsPerNode = 1;
    String testCollectionName = "repfacttest_c8n_2x2";
    String shardId = "shard1";
    int minRf = 2;
    
    CollectionAdminResponse resp = createCollection(testCollectionName, "conf1", numShards, replicationFactor, maxShardsPerNode);
    
    if (resp.getResponse().get("failure") != null) {
      CollectionAdminRequest.deleteCollection(testCollectionName).process(cloudClient);
      
      resp = createCollection(testCollectionName, "conf1", numShards, replicationFactor, maxShardsPerNode);
      
      if (resp.getResponse().get("failure") != null) {
        fail("Could not create " + testCollectionName);
      }
    }
    
    cloudClient.setDefaultCollection(testCollectionName);
    
    List<Replica> replicas = 
        ensureAllReplicasAreActive(testCollectionName, shardId, numShards, replicationFactor, 30);
    assertTrue("Expected active 1 replicas for "+testCollectionName, replicas.size() == 1);
                
    List<SolrInputDocument> batch = new ArrayList<SolrInputDocument>(10);
    for (int i=0; i < 15; i++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField(id, String.valueOf(i));
      doc.addField("a_t", "hello" + i);
      batch.add(doc);
    }
    
    // send directly to the leader using HttpSolrServer instead of CloudSolrServer (to test support for non-direct updates)
    UpdateRequest up = new UpdateRequest();
    up.setParam(UpdateRequest.MIN_REPFACT, String.valueOf(minRf));
    up.add(batch);

    Replica leader = cloudClient.getZkStateReader().getLeaderRetry(testCollectionName, shardId);
    sendNonDirectUpdateRequestReplicaWithRetry(leader, up, 2, testCollectionName);    
    sendNonDirectUpdateRequestReplicaWithRetry(replicas.get(0), up, 2, testCollectionName);    
    
    // so now kill the replica of shard2 and verify the achieved rf is only 1
    List<Replica> shard2Replicas = 
        ensureAllReplicasAreActive(testCollectionName, "shard2", numShards, replicationFactor, 30);
    assertTrue("Expected active 1 replicas for "+testCollectionName, replicas.size() == 1);
    
    getProxyForReplica(shard2Replicas.get(0)).close();

    Thread.sleep(2000);
    
    // shard1 will have rf=2 but shard2 will only have rf=1
    sendNonDirectUpdateRequestReplicaWithRetry(leader, up, 1, testCollectionName);    
    sendNonDirectUpdateRequestReplicaWithRetry(replicas.get(0), up, 1, testCollectionName);
    
    // heal the partition
    getProxyForReplica(shard2Replicas.get(0)).reopen();
    
    Thread.sleep(2000);
  }
  

  protected void sendNonDirectUpdateRequestReplicaWithRetry(Replica replica, UpdateRequest up, int expectedRf, String collection) throws Exception {
    try {
      sendNonDirectUpdateRequestReplica(replica, up, expectedRf, collection);
    } catch (Exception e) {
      sendNonDirectUpdateRequestReplica(replica, up, expectedRf, collection);
    }
  }
  
  @SuppressWarnings("rawtypes")
  protected void sendNonDirectUpdateRequestReplica(Replica replica, UpdateRequest up, int expectedRf, String collection) throws Exception {

    ZkCoreNodeProps zkProps = new ZkCoreNodeProps(replica);
    String url = zkProps.getBaseUrl() + "/" + collection;

    try (HttpSolrClient solrServer = getHttpSolrClient(url)) {
      NamedList resp = solrServer.request(up);
      NamedList hdr = (NamedList) resp.get("responseHeader");
      Integer batchRf = (Integer)hdr.get(UpdateRequest.REPFACT);
      assertTrue("Expected rf="+expectedRf+" for batch but got "+
        batchRf+"; clusterState: "+printClusterStateInfo(), batchRf == expectedRf);      
    }
  }
    
  protected void testRf3() throws Exception {
    int numShards = 1;
    int replicationFactor = 3;
    int maxShardsPerNode = 1;
    String testCollectionName = "repfacttest_c8n_1x3";
    String shardId = "shard1";
    int minRf = 2;
    
    createCollection(testCollectionName, "conf1", numShards, replicationFactor, maxShardsPerNode);
    cloudClient.setDefaultCollection(testCollectionName);
    
    List<Replica> replicas = 
        ensureAllReplicasAreActive(testCollectionName, shardId, numShards, replicationFactor, 30);
    assertTrue("Expected 2 active replicas for "+testCollectionName, replicas.size() == 2);
                
    int rf = sendDoc(1, minRf);
    assertRf(3, "all replicas should be active", rf);
        
    getProxyForReplica(replicas.get(0)).close();
    
    rf = sendDoc(2, minRf);
    assertRf(2, "one replica should be down", rf);

    getProxyForReplica(replicas.get(1)).close();    

    rf = sendDoc(3, minRf);
    assertRf(1, "both replicas should be down", rf);
    
    // heal the partitions
    getProxyForReplica(replicas.get(0)).reopen();    
    getProxyForReplica(replicas.get(1)).reopen();
    
    Thread.sleep(2000); // give time for the healed partition to get propagated
    
    ensureAllReplicasAreActive(testCollectionName, shardId, numShards, replicationFactor, 30);
    
    rf = sendDoc(4, minRf);
    assertRf(3, "partitions to replicas have been healed", rf);
    
    // now send a batch
    List<SolrInputDocument> batch = new ArrayList<SolrInputDocument>(10);
    for (int i=5; i < 15; i++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField(id, String.valueOf(i));
      doc.addField("a_t", "hello" + i);
      batch.add(doc);
    }
    
    int batchRf = sendDocsWithRetry(batch, minRf, 5, 1);
    assertRf(3, "batch should have succeeded on all replicas", batchRf);
    
    // add some chaos to the batch
    getProxyForReplica(replicas.get(0)).close();
    
    // now send a batch
    batch = new ArrayList<SolrInputDocument>(10);
    for (int i=15; i < 30; i++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField(id, String.valueOf(i));
      doc.addField("a_t", "hello" + i);
      batch.add(doc);
    }

    batchRf = sendDocsWithRetry(batch, minRf, 5, 1);
    assertRf(2, "batch should have succeeded on 2 replicas (only one replica should be down)", batchRf);

    // close the 2nd replica, and send a 3rd batch with expected achieved rf=1
    getProxyForReplica(replicas.get(1)).close();
    
    batch = new ArrayList<SolrInputDocument>(10);
    for (int i=30; i < 45; i++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField(id, String.valueOf(i));
      doc.addField("a_t", "hello" + i);
      batch.add(doc);
    }

    batchRf = sendDocsWithRetry(batch, minRf, 5, 1);
    assertRf(1, "batch should have succeeded on the leader only (both replicas should be down)", batchRf);

    getProxyForReplica(replicas.get(0)).reopen();        
    getProxyForReplica(replicas.get(1)).reopen();
    
    Thread.sleep(2000);
    ensureAllReplicasAreActive(testCollectionName, shardId, numShards, replicationFactor, 30);    
  } 

  protected int sendDoc(int docId, int minRf) throws Exception {
    UpdateRequest up = new UpdateRequest();
    up.setParam(UpdateRequest.MIN_REPFACT, String.valueOf(minRf));
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField(id, String.valueOf(docId));
    doc.addField("a_t", "hello" + docId);
    up.add(doc);
    return cloudClient.getMinAchievedReplicationFactor(cloudClient.getDefaultCollection(), cloudClient.request(up));
  }
  
  protected void assertRf(int expected, String explain, int actual) throws Exception {
    if (actual != expected) {
      String assertionFailedMessage = 
          String.format(Locale.ENGLISH, "Expected rf=%d because %s but got %d", expected, explain, actual);
      fail(assertionFailedMessage+"; clusterState: "+printClusterStateInfo());
    }    
  }  
}
