package org.apache.solr.cloud;

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

import java.io.File;
import java.net.ServerSocket;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.JSONTestUtil;
import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.NamedList;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simulates HTTP partitions between a leader and replica but the replica does
 * not lose its ZooKeeper connection.
 */
@Slow
@SuppressSSL(bugUrl = "https://issues.apache.org/jira/browse/SOLR-5776")
public class HttpPartitionTest extends AbstractFullDistribZkTestBase {
  
  private static final transient Logger log = 
      LoggerFactory.getLogger(HttpPartitionTest.class);
  
  // To prevent the test assertions firing too fast before cluster state
  // recognizes (and propagates) partitions
  private static final long sleepMsBeforeHealPartition = 2000L;
  
  private static final int maxWaitSecsToSeeAllActive = 30;
  
  private Map<URI,SocketProxy> proxies = new HashMap<URI,SocketProxy>();
  
  public HttpPartitionTest() {
    super();
    sliceCount = 2;
    shardCount = 2;
  }
  
  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    System.setProperty("numShards", Integer.toString(sliceCount));
  }
  
  @Override
  @After
  public void tearDown() throws Exception {    
    System.clearProperty("numShards");
    
    try {
      super.tearDown();
    } catch (Exception exc) {}
    
    resetExceptionIgnores();
    
    // close socket proxies after super.tearDown
    if (!proxies.isEmpty()) {
      for (SocketProxy proxy : proxies.values()) {
        proxy.close();
      }
    }    
  }
  
  /**
   * Overrides the parent implementation so that we can configure a socket proxy
   * to sit infront of each Jetty server, which gives us the ability to simulate
   * network partitions without having to fuss with IPTables (which is not very
   * cross platform friendly).
   */
  @Override
  public JettySolrRunner createJetty(File solrHome, String dataDir,
      String shardList, String solrConfigOverride, String schemaOverride)
      throws Exception {
    
    JettySolrRunner jetty = new JettySolrRunner(solrHome.getPath(), context,
        0, solrConfigOverride, schemaOverride, false,
        getExtraServlets(), sslConfig, getExtraRequestFilters());
    jetty.setShards(shardList);
    jetty.setDataDir(getDataDir(dataDir));      
    
    // setup to proxy Http requests to this server unless it is the control
    // server
    int proxyPort = getNextAvailablePort();
    jetty.setProxyPort(proxyPort);        
    jetty.start();        
    
    // create a socket proxy for the jetty server ...
    SocketProxy proxy = new SocketProxy(proxyPort, jetty.getBaseUrl().toURI());
    proxies.put(proxy.getUrl(), proxy);
    
    return jetty;
  }
  
  protected int getNextAvailablePort() throws Exception {    
    int port = -1;
    try (ServerSocket s = new ServerSocket(0)) {
      port = s.getLocalPort();
    }
    return port;
  }
   
  @Override
  public void doTest() throws Exception {
    waitForThingsToLevelOut(30000);
    
    // test a 1x2 collection
    testRf2();

    // now do similar for a 1x3 collection while taking 2 replicas on-and-off
    // each time
    testRf3();

    // kill a leader and make sure recovery occurs as expected
    testRf3WithLeaderFailover();    
  }
  
  protected void testRf2() throws Exception {
    // create a collection that has 1 shard but 2 replicas
    String testCollectionName = "c8n_1x2";
    createCollection(testCollectionName, 1, 2, 1);
    cloudClient.setDefaultCollection(testCollectionName);
    
    sendDoc(1);
    
    Replica notLeader = 
        ensureAllReplicasAreActive(testCollectionName, "shard1", 1, 2, maxWaitSecsToSeeAllActive).get(0);
    
    // ok, now introduce a network partition between the leader and the replica
    SocketProxy proxy = getProxyForReplica(notLeader);
    
    proxy.close();
    
    // indexing during a partition
    sendDoc(2);
    
    // Have the partition last at least 1 sec
    // While this gives the impression that recovery is timing related, this is
    // really only
    // to give time for the state to be written to ZK before the test completes.
    // In other words,
    // without a brief pause, the test finishes so quickly that it doesn't give
    // time for the recovery process to kick-in
    Thread.sleep(sleepMsBeforeHealPartition);
    
    proxy.reopen();
    
    List<Replica> notLeaders = 
        ensureAllReplicasAreActive(testCollectionName, "shard1", 1, 2, maxWaitSecsToSeeAllActive);
    
    sendDoc(3);
    
    // sent 3 docs in so far, verify they are on the leader and replica
    assertDocsExistInAllReplicas(notLeaders, testCollectionName, 1, 3);
        
    // now up the stakes and do more docs
    int numDocs = 1000;
    boolean hasPartition = false;
    for (int d = 0; d < numDocs; d++) {
      // create / restore partition every 100 docs
      if (d % 100 == 0) {
        if (hasPartition) {
          proxy.reopen();
          hasPartition = false;
        } else {
          if (d >= 100) {
            proxy.close();
            hasPartition = true;
            Thread.sleep(sleepMsBeforeHealPartition);
          }
        }
      }
      sendDoc(d + 4); // 4 is offset as we've already indexed 1-3
    }
    
    // restore connectivity if lost
    if (hasPartition) {
      proxy.reopen();
    }
    
    notLeaders = ensureAllReplicasAreActive(testCollectionName, "shard1", 1, 2, maxWaitSecsToSeeAllActive);
    
    // verify all docs received
    assertDocsExistInAllReplicas(notLeaders, testCollectionName, 1, numDocs + 3);
  }
  
  protected void testRf3() throws Exception {
    // create a collection that has 1 shard but 2 replicas
    String testCollectionName = "c8n_1x3";
    createCollection(testCollectionName, 1, 3, 1);
    cloudClient.setDefaultCollection(testCollectionName);
    
    sendDoc(1);
    
    List<Replica> notLeaders = 
        ensureAllReplicasAreActive(testCollectionName, "shard1", 1, 3, maxWaitSecsToSeeAllActive);
    assertTrue("Expected 2 replicas for collection " + testCollectionName
        + " but found " + notLeaders.size() + "; clusterState: "
        + printClusterStateInfo(),
        notLeaders.size() == 2);
    
    sendDoc(1);
    
    // ok, now introduce a network partition between the leader and the replica
    SocketProxy proxy0 = getProxyForReplica(notLeaders.get(0));
    
    proxy0.close();
    
    // indexing during a partition
    sendDoc(2);
    
    Thread.sleep(sleepMsBeforeHealPartition);
    
    proxy0.reopen();
    
    SocketProxy proxy1 = getProxyForReplica(notLeaders.get(1));
    
    proxy1.close();
    
    sendDoc(3);
    
    Thread.sleep(sleepMsBeforeHealPartition);
    proxy1.reopen();
    
    // sent 4 docs in so far, verify they are on the leader and replica
    notLeaders = ensureAllReplicasAreActive(testCollectionName, "shard1", 1, 3, maxWaitSecsToSeeAllActive); 
    
    sendDoc(4);
    
    assertDocsExistInAllReplicas(notLeaders, testCollectionName, 1, 4);    
  }
  
  protected void testRf3WithLeaderFailover() throws Exception {
    // now let's create a partition in one of the replicas and outright
    // kill the leader ... see what happens
    // create a collection that has 1 shard but 3 replicas
    String testCollectionName = "c8n_1x3_lf"; // _lf is leader fails
    createCollection(testCollectionName, 1, 3, 1);
    cloudClient.setDefaultCollection(testCollectionName);
    
    sendDoc(1);
    
    List<Replica> notLeaders = 
        ensureAllReplicasAreActive(testCollectionName, "shard1", 1, 3, maxWaitSecsToSeeAllActive);
    assertTrue("Expected 2 replicas for collection " + testCollectionName
        + " but found " + notLeaders.size() + "; clusterState: "
        + printClusterStateInfo(),
        notLeaders.size() == 2);
        
    sendDoc(1);
    
    // ok, now introduce a network partition between the leader and the replica
    SocketProxy proxy0 = null;
    proxy0 = getProxyForReplica(notLeaders.get(0));
    
    proxy0.close();
    
    // indexing during a partition
    sendDoc(2);
    
    Thread.sleep(sleepMsBeforeHealPartition);
    
    proxy0.reopen();
    
    SocketProxy proxy1 = getProxyForReplica(notLeaders.get(1));
    
    proxy1.close();
    
    sendDoc(3);
    
    Thread.sleep(sleepMsBeforeHealPartition);
    proxy1.reopen();
    
    // sent 4 docs in so far, verify they are on the leader and replica
    notLeaders = ensureAllReplicasAreActive(testCollectionName, "shard1", 1, 3, maxWaitSecsToSeeAllActive); 
    
    sendDoc(4);
    
    assertDocsExistInAllReplicas(notLeaders, testCollectionName, 1, 4);    
        
    Replica leader = 
        cloudClient.getZkStateReader().getLeaderRetry(testCollectionName, "shard1");
    String leaderNode = leader.getNodeName();
    assertNotNull("Could not find leader for shard1 of "+
      testCollectionName+"; clusterState: "+printClusterStateInfo(), leader);
    JettySolrRunner leaderJetty = getJettyOnPort(getReplicaPort(leader));
    
    // since maxShardsPerNode is 1, we're safe to kill the leader
    notLeaders = ensureAllReplicasAreActive(testCollectionName, "shard1", 1, 3, maxWaitSecsToSeeAllActive);    
    proxy0 = getProxyForReplica(notLeaders.get(0));
    proxy0.close();
        
    // indexing during a partition
    // doc should be on leader and 1 replica
    sendDoc(5);
    
    Thread.sleep(sleepMsBeforeHealPartition);
    
    String shouldNotBeNewLeaderNode = notLeaders.get(0).getNodeName();

    // kill the leader
    leaderJetty.stop();
    
    if (leaderJetty.isRunning())
      fail("Failed to stop the leader on "+leaderNode);
        
    SocketProxy oldLeaderProxy = getProxyForReplica(leader);
    if (oldLeaderProxy != null) {
      oldLeaderProxy.close();      
    } else {
      log.warn("No SocketProxy found for old leader node "+leaderNode);      
    }

    Thread.sleep(10000); // give chance for new leader to be elected.
    
    Replica newLeader = 
        cloudClient.getZkStateReader().getLeaderRetry(testCollectionName, "shard1", 60000);
        
    assertNotNull("No new leader was elected after 60 seconds; clusterState: "+
      printClusterStateInfo(),newLeader);
        
    assertTrue("Expected node "+shouldNotBeNewLeaderNode+
        " to NOT be the new leader b/c it was out-of-sync with the old leader! ClusterState: "+
        printClusterStateInfo(), 
        !shouldNotBeNewLeaderNode.equals(newLeader.getNodeName()));
    
    proxy0.reopen();
    
    Thread.sleep(10000L);
    
    cloudClient.getZkStateReader().updateClusterState(true);
    
    List<Replica> activeReps = getActiveOrRecoveringReplicas(testCollectionName, "shard1");
    assertTrue("Expected 2 of 3 replicas to be active but only found "+
      activeReps.size()+"; "+activeReps+"; clusterState: "+printClusterStateInfo(), 
      activeReps.size() == 2);
        
    sendDoc(6);
    
    assertDocsExistInAllReplicas(activeReps, testCollectionName, 1, 6);
  }
    
  protected List<Replica> getActiveOrRecoveringReplicas(String testCollectionName, String shardId) throws Exception {    
    Map<String,Replica> activeReplicas = new HashMap<String,Replica>();    
    ZkStateReader zkr = cloudClient.getZkStateReader();
    ClusterState cs = zkr.getClusterState();
    cs = zkr.getClusterState();
    assertNotNull(cs);
    for (Slice shard : cs.getActiveSlices(testCollectionName)) {
      if (shard.getName().equals(shardId)) {
        for (Replica replica : shard.getReplicas()) {
          String replicaState = replica.getStr(ZkStateReader.STATE_PROP);
          if (ZkStateReader.ACTIVE.equals(replicaState) || ZkStateReader.RECOVERING.equals(replicaState)) {
            activeReplicas.put(replica.getName(), replica);
          }
        }
      }
    }        
    List<Replica> replicas = new ArrayList<Replica>();
    replicas.addAll(activeReplicas.values());
    return replicas;
  }  
  
  protected SocketProxy getProxyForReplica(Replica replica) throws Exception {
    String replicaBaseUrl = replica.getStr(ZkStateReader.BASE_URL_PROP);
    assertNotNull(replicaBaseUrl);
    URL baseUrl = new URL(replicaBaseUrl);
    
    SocketProxy proxy = proxies.get(baseUrl.toURI());
    if (proxy == null && !baseUrl.toExternalForm().endsWith("/")) {
      baseUrl = new URL(baseUrl.toExternalForm() + "/");
      proxy = proxies.get(baseUrl.toURI());
    }
    assertNotNull("No proxy found for " + baseUrl + "!", proxy);
    return proxy;
  }
  
  protected void assertDocsExistInAllReplicas(List<Replica> notLeaders,
      String testCollectionName, int firstDocId, int lastDocId)
      throws Exception {
    Replica leader = 
        cloudClient.getZkStateReader().getLeaderRetry(testCollectionName, "shard1", 10000);
    HttpSolrServer leaderSolr = getHttpSolrServer(leader, testCollectionName);
    List<HttpSolrServer> replicas = 
        new ArrayList<HttpSolrServer>(notLeaders.size());
    
    for (Replica r : notLeaders) {
      replicas.add(getHttpSolrServer(r, testCollectionName));
    }
    try {
      for (int d = firstDocId; d <= lastDocId; d++) {
        String docId = String.valueOf(d);
        assertDocExists(leaderSolr, testCollectionName, docId);
        for (HttpSolrServer replicaSolr : replicas) {
          assertDocExists(replicaSolr, testCollectionName, docId);
        }
      }
    } finally {
      if (leaderSolr != null) {
        leaderSolr.shutdown();
      }
      for (HttpSolrServer replicaSolr : replicas) {
        replicaSolr.shutdown();
      }
    }
  }
  
  protected HttpSolrServer getHttpSolrServer(Replica replica, String coll) throws Exception {
    ZkCoreNodeProps zkProps = new ZkCoreNodeProps(replica);
    String url = zkProps.getBaseUrl() + "/" + coll;
    return new HttpSolrServer(url);
  }
  
  protected void sendDoc(int docId) throws Exception {
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField(id, String.valueOf(docId));
    doc.addField("a_t", "hello" + docId);
    cloudClient.add(doc);
  }
  
  /**
   * Query the real-time get handler for a specific doc by ID to verify it
   * exists in the provided server.
   */
  @SuppressWarnings("rawtypes")
  protected void assertDocExists(HttpSolrServer solr, String coll, String docId) throws Exception {
    QueryRequest qr = new QueryRequest(params("qt", "/get", "id", docId));
    NamedList rsp = solr.request(qr);
    String match = 
        JSONTestUtil.matchObj("/id", rsp.get("doc"), new Integer(docId));
    assertTrue("Doc with id=" + docId + " not found in " + solr.getBaseURL()
        + " due to: " + match, match == null);
  }
  
  protected JettySolrRunner getJettyOnPort(int port) {
    JettySolrRunner theJetty = null;
    for (JettySolrRunner jetty : jettys) {
      if (port == jetty.getLocalPort()) {
        theJetty = jetty;
        break;
      }
    }    
    
    if (theJetty == null) {
      if (controlJetty.getLocalPort() == port) {
        theJetty = controlJetty;
      }
    }
    
    if (theJetty == null)
      fail("Not able to find JettySolrRunner for port: "+port);
    
    return theJetty;
  }
  
  protected int getReplicaPort(Replica replica) {
    String replicaNode = replica.getNodeName();    
    String tmp = replicaNode.substring(replicaNode.indexOf(':')+1);
    if (tmp.indexOf('_') != -1)
      tmp = tmp.substring(0,tmp.indexOf('_'));
    return Integer.parseInt(tmp);    
  }  
}
