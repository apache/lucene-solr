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
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.solr.JSONTestUtil;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.cloud.SocketProxy;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.NamedList;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LeaderVoteWaitTimeoutTest extends SolrCloudTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final int NODE_COUNT = 4;

  private static Map<JettySolrRunner, SocketProxy> proxies;
  private static Map<URI, JettySolrRunner> jettys;

  @BeforeClass
  public static void setupCluster() throws Exception {
    System.setProperty("solr.directoryFactory", "solr.StandardDirectoryFactory");
    System.setProperty("solr.ulog.numRecordsToKeep", "1000");
    System.setProperty("leaderVoteWait", "2000");
    System.setProperty("distribUpdateSoTimeout", "5000");
    System.setProperty("distribUpdateConnTimeout", "5000");
    System.setProperty("solr.httpclient.retries", "0");
    System.setProperty("solr.retries.on.forward", "0");
    System.setProperty("solr.retries.to.followers", "0"); 
  }

  @AfterClass
  public static void tearDownCluster() throws Exception {
    proxies = null;
    jettys = null;
    System.clearProperty("solr.directoryFactory");
    System.clearProperty("solr.ulog.numRecordsToKeep");
    System.clearProperty("leaderVoteWait");
    System.clearProperty("distribUpdateSoTimeout");
    System.clearProperty("distribUpdateConnTimeout");
  }

  @Before
  public void setupTest() throws Exception {
    configureCluster(NODE_COUNT)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();

    // Add proxies
    proxies = new HashMap<>(cluster.getJettySolrRunners().size());
    jettys = new HashMap<>();
    for (JettySolrRunner jetty : cluster.getJettySolrRunners()) {
      SocketProxy proxy = new SocketProxy();
      jetty.setProxyPort(proxy.getListenPort());
      cluster.stopJettySolrRunner(jetty);// TODO: Can we avoid this restart
      cluster.startJettySolrRunner(jetty);
      proxy.open(new URI(jetty.getBaseUrl()));
      if (log.isInfoEnabled()) {
        log.info("Adding proxy for URL: {}. Proxy {}", jetty.getBaseUrl(), proxy.getUrl());
      }
      proxies.put(jetty, proxy);
      jettys.put(proxy.getUrl(), jetty);
    }
  }
  
  @After
  public void tearDown() throws Exception {
    if (null != proxies) {
      for (SocketProxy proxy : proxies.values()) {
        proxy.close();
      }
    }
    shutdownCluster();
    super.tearDown();
  }

  @Test
  @Nightly
  public void basicTest() throws Exception {
    final String collectionName = "basicTest";
    CollectionAdminRequest.createCollection(collectionName, 1, 1)
        .setCreateNodeSet(cluster.getJettySolrRunner(0).getNodeName())
        .process(cluster.getSolrClient());

    cluster.getSolrClient().add(collectionName, new SolrInputDocument("id", "1"));
    cluster.getSolrClient().add(collectionName, new SolrInputDocument("id", "2"));
    cluster.getSolrClient().commit(collectionName);

    try (ZkShardTerms zkShardTerms = new ZkShardTerms(collectionName, "s1", cluster.getZkClient())) {
      assertEquals(1, zkShardTerms.getTerms().size());
      assertEquals(1L, zkShardTerms.getHighestTerm());
    }

    String nodeName = cluster.getJettySolrRunner(0).getCoreContainer().getZkController().getNodeName();
    
    JettySolrRunner j = cluster.getJettySolrRunner(0);
    j.stop();
    cluster.waitForJettyToStop(j);
    
    cluster.getSolrClient().getZkStateReader().waitForState(collectionName, 10, TimeUnit.SECONDS, (liveNodes, collectionState) -> !liveNodes.contains(nodeName));

    CollectionAdminRequest.addReplicaToShard(collectionName, "s1")
        .setNode(cluster.getJettySolrRunner(1).getNodeName())
        .process(cluster.getSolrClient());

    waitForState("Timeout waiting for replica win the election", collectionName, (liveNodes, collectionState) -> {
      Replica newLeader = collectionState.getSlice("s1").getLeader();
      if (newLeader == null) {
        return false;
      }
      return newLeader.getNodeName().equals(cluster.getJettySolrRunner(1).getNodeName());
    });

    try (ZkShardTerms zkShardTerms = new ZkShardTerms(collectionName, "s1", cluster.getZkClient())) {
      Replica newLeader = getCollectionState(collectionName).getSlice("s1").getLeader();
      assertEquals(2, zkShardTerms.getTerms().size());
      assertEquals(1L, zkShardTerms.getTerm(newLeader.getName()));
    }

    cluster.getJettySolrRunner(0).start();
    
    cluster.waitForAllNodes(30);
    try {
      CollectionAdminRequest.deleteCollection(collectionName).process(cluster.getSolrClient()); // connection may be off from pool
    } catch (Exception e) {
      CollectionAdminRequest.deleteCollection(collectionName).process(cluster.getSolrClient());
    }
  }

  @Test
  @Nightly
  public void testMostInSyncReplicasCanWinElection() throws Exception {
    final String collectionName = "collection1";
    CollectionAdminRequest.createCollection(collectionName, 1, 3)
        .setCreateNodeSet(ZkStateReader.CREATE_NODE_SET_EMPTY)
        .process(cluster.getSolrClient());
    CollectionAdminRequest.addReplicaToShard(collectionName, "s1")
        .setNode(cluster.getJettySolrRunner(0).getNodeName())
        .process(cluster.getSolrClient());

    cluster.waitForActiveCollection(collectionName, 1, 1);
    Replica leader = getCollectionState(collectionName).getSlice("s1").getLeader();

    // this replica will ahead of election queue
    CollectionAdminRequest.addReplicaToShard(collectionName, "s1")
        .setNode(cluster.getJettySolrRunner(1).getNodeName())
        .process(cluster.getSolrClient());

    Replica replica1 = getCollectionState(collectionName).getSlice("s1")
        .getReplicas(replica -> replica.getNodeName().equals(cluster.getJettySolrRunner(1).getNodeName())).get(0);

    CollectionAdminRequest.addReplicaToShard(collectionName, "s1")
        .setNode(cluster.getJettySolrRunner(2).getNodeName())
        .process(cluster.getSolrClient());

    Replica replica2 = getCollectionState(collectionName).getSlice("s1")
        .getReplicas(replica -> replica.getNodeName().equals(cluster.getJettySolrRunner(2).getNodeName())).get(0);

    cluster.getSolrClient().add(collectionName, new SolrInputDocument("id", "1"));
    cluster.getSolrClient().add(collectionName, new SolrInputDocument("id", "2"));
    cluster.getSolrClient().commit(collectionName);

    // replica in node1 won't be able to do recovery
    proxies.get(cluster.getJettySolrRunner(0)).close();
    // leader won't be able to send request to replica in node1
    proxies.get(cluster.getJettySolrRunner(1)).close();

    addDoc(collectionName, 3, cluster.getJettySolrRunner(0));

    try (ZkShardTerms zkShardTerms = new ZkShardTerms(collectionName, "s1", cluster.getZkClient())) {
      assertEquals(3, zkShardTerms.getTerms().size());
      assertEquals(zkShardTerms.getHighestTerm(), zkShardTerms.getTerm(leader.getName()));
      assertEquals(zkShardTerms.getHighestTerm(), zkShardTerms.getTerm(replica2.getName()));
      assertTrue(zkShardTerms.getHighestTerm() + "," + zkShardTerms.getTerm(replica1.getName()), zkShardTerms.getHighestTerm() >= zkShardTerms.getTerm(replica1.getName()));
    }

    proxies.get(cluster.getJettySolrRunner(2)).close();
    addDoc(collectionName, 4, cluster.getJettySolrRunner(0));

    try (ZkShardTerms zkShardTerms = new ZkShardTerms(collectionName, "s1", cluster.getZkClient())) {
      assertEquals(3, zkShardTerms.getTerms().size());
      assertEquals(zkShardTerms.getHighestTerm(), zkShardTerms.getTerm(leader.getName()));
      assertTrue(zkShardTerms.getHighestTerm() + "," + zkShardTerms.getTerm(replica2.getName()), zkShardTerms.getHighestTerm() >= zkShardTerms.getTerm(replica2.getName()));
      assertTrue(zkShardTerms.getHighestTerm() >= zkShardTerms.getTerm(replica1.getName()));
      assertTrue(zkShardTerms.getTerm(replica2.getName()) >= zkShardTerms.getTerm(replica1.getName()));
    }

    proxies.get(cluster.getJettySolrRunner(1)).reopen();
    proxies.get(cluster.getJettySolrRunner(2)).reopen();
    
    
    JettySolrRunner j = cluster.getJettySolrRunner(0);
    j.stop();

    cluster.getJettySolrRunner(0).start();
    proxies.get(cluster.getJettySolrRunner(0)).reopen();

    cluster.waitForActiveCollection(collectionName, 1, 3);
    assertDocsExistInAllReplicas(Arrays.asList(leader, replica1), collectionName, 1, 3);

    try {
      CollectionAdminRequest.deleteCollection(collectionName).process(cluster.getSolrClient()); // connection may be off from pool
    } catch (Exception e) {
      CollectionAdminRequest.deleteCollection(collectionName).process(cluster.getSolrClient());
    }
  }


  private void addDoc(String collection, int docId, JettySolrRunner solrRunner) throws IOException, SolrServerException {
    try (HttpSolrClient solrClient = new HttpSolrClient.Builder(solrRunner.getBaseUrl().toString()).build()) {
      solrClient.add(collection, new SolrInputDocument("id", String.valueOf(docId)));
      solrClient.commit(collection);
    }
  }

  private void assertDocsExistInAllReplicas(List<Replica> notLeaders,
                                            String testCollectionName, int firstDocId, int lastDocId) throws Exception {
    Replica leader =
        cluster.getSolrClient().getZkStateReader().getLeaderRetry(testCollectionName, "s1", 10000);
    Http2SolrClient leaderSolr = getHttpSolrClient(leader, testCollectionName);
    List<Http2SolrClient> replicas =
            new ArrayList<>(notLeaders.size());

    for (Replica r : notLeaders) {
      replicas.add(getHttpSolrClient(r, testCollectionName));
    }
    try {
      for (int d = firstDocId; d <= lastDocId; d++) {
        String docId = String.valueOf(d);
        assertDocExists(leaderSolr, testCollectionName, docId);
        for (Http2SolrClient replicaSolr : replicas) {
          assertDocExists(replicaSolr, testCollectionName, docId);
        }
      }
    } finally {
      if (leaderSolr != null) {
        leaderSolr.close();
      }
      for (Http2SolrClient replicaSolr : replicas) {
        replicaSolr.close();
      }
    }
  }

  private void assertDocExists(Http2SolrClient solr, String coll, String docId) throws Exception {
    NamedList rsp = realTimeGetDocId(solr, docId);
    String match = JSONTestUtil.matchObj("/id", rsp.get("doc"), docId);
    assertTrue("Doc with id=" + docId + " not found in " + solr.getBaseURL()
        + " due to: " + match + "; rsp="+rsp, match == null);
  }

  private NamedList realTimeGetDocId(Http2SolrClient solr, String docId) throws SolrServerException, IOException {
    QueryRequest qr = new QueryRequest(params("qt", "/get", "id", docId, "distrib", "false"));
    return solr.request(qr);
  }

  protected Http2SolrClient getHttpSolrClient(Replica replica, String coll) throws Exception {
    String url = replica.getBaseUrl() + "/" + coll;
    return SolrTestCaseJ4.getHttpSolrClient(url);
  }


}
