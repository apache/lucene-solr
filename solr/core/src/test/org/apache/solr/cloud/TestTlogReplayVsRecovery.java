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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.lucene.util.LuceneTestCase.AwaitsFix;

import org.apache.solr.JSONTestUtil;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.cloud.SocketProxy;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.RequestStatusState;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.util.TestInjection;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AwaitsFix(bugUrl="https://issues.apache.org/jira/browse/SOLR-13486;https://issues.apache.org/jira/browse/SOLR-14183")
public class TestTlogReplayVsRecovery extends SolrCloudTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final String COLLECTION = "collecion_with_slow_tlog_recovery";
  
  private JettySolrRunner NODE0;
  private JettySolrRunner NODE1;
  private Map<JettySolrRunner, SocketProxy> proxies;
  private Map<URI, JettySolrRunner> jettys;

  // we want to ensure there is tlog replay on the leader after we restart it,
  // so in addition to not committing the docs we add during network partition
  // we also want to ensure that our leader doesn't do a "Commit on close"
  //
  // TODO: once SOLR-13486 is fixed, we should randomize this...
  private static final boolean TEST_VALUE_FOR_SKIP_COMMIT_ON_CLOSE = true;
  
  @Before
  public void setupCluster() throws Exception {
    TestInjection.skipIndexWriterCommitOnClose = TEST_VALUE_FOR_SKIP_COMMIT_ON_CLOSE;
    
    System.setProperty("solr.directoryFactory", "solr.StandardDirectoryFactory");
    System.setProperty("solr.ulog.numRecordsToKeep", "1000");
    System.setProperty("leaderVoteWait", "60000");

    configureCluster(2)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();

    NODE0 = cluster.getJettySolrRunner(0);
    NODE1 = cluster.getJettySolrRunner(1);
      
    // Add proxies
    proxies = new HashMap<>(cluster.getJettySolrRunners().size());
    jettys = new HashMap<>();
    for (JettySolrRunner jetty:cluster.getJettySolrRunners()) {
      SocketProxy proxy = new SocketProxy();
      jetty.setProxyPort(proxy.getListenPort());
      cluster.stopJettySolrRunner(jetty);//TODO: Can we avoid this restart
      cluster.startJettySolrRunner(jetty);
      proxy.open(jetty.getBaseUrl().toURI());
      if (log.isInfoEnabled()) {
        log.info("Adding proxy for URL: {}. Proxy: {}", jetty.getBaseUrl(), proxy.getUrl());
      }
      proxies.put(jetty, proxy);
      jettys.put(proxy.getUrl(), jetty);
    }
  }

  @After
  public void tearDownCluster() throws Exception {
    TestInjection.reset();
    
    if (null != proxies) {
      for (SocketProxy proxy : proxies.values()) {
        proxy.close();
      }
      proxies = null;
    }
    jettys = null;
    System.clearProperty("solr.directoryFactory");
    System.clearProperty("solr.ulog.numRecordsToKeep");
    System.clearProperty("leaderVoteWait");
    
    shutdownCluster();
  }

  public void testManyDocsInTlogReplayWhileReplicaIsTryingToRecover() throws Exception {
    // TODO: One the basic problem in SOLR-13486 is fixed, this test can be made more robust by:
    // 1) randomizing the number of committedDocs (pre net split) & uncommittedDocs (post net split)
    //    to trigger diff recovery strategies & shutdown behavior
    // 2) replace "committedDocs + uncommittedDocs" with 4 variables:
    //    a: docs committed before network split (add + commit)
    //    b: docs not committed before network split (add w/o commit)
    //    c: docs committed after network split (add + commit)
    //    d: docs not committed after network split (add w/o commit)
    final int committedDocs = 3;
    final int uncommittedDocs = 50;
    
    log.info("Create Collection...");
    assertEquals(RequestStatusState.COMPLETED,
                 CollectionAdminRequest.createCollection(COLLECTION, 1, 2)
                 .setCreateNodeSet("")
                 .processAndWait(cluster.getSolrClient(), DEFAULT_TIMEOUT));
    assertEquals(RequestStatusState.COMPLETED,
                 CollectionAdminRequest.addReplicaToShard(COLLECTION, "shard1")
                 .setNode(NODE0.getNodeName())
                 .processAndWait(cluster.getSolrClient(), DEFAULT_TIMEOUT));
    
    waitForState("Timeout waiting for shard leader", COLLECTION, clusterShape(1, 1));

    assertEquals(RequestStatusState.COMPLETED,
                 CollectionAdminRequest.addReplicaToShard(COLLECTION, "shard1")
                 .setNode(NODE1.getNodeName())
                 .processAndWait(cluster.getSolrClient(), DEFAULT_TIMEOUT));
    
    cluster.waitForActiveCollection(COLLECTION, 1, 2);
    
    waitForState("Timeout waiting for 1x2 collection", COLLECTION, clusterShape(1, 2));
    
    final Replica leader = getCollectionState(COLLECTION).getSlice("shard1").getLeader();
    assertEquals("Sanity check failed", NODE0.getNodeName(), leader.getNodeName());

    log.info("Add and commit {} docs...", committedDocs);
    addDocs(true, committedDocs, 1);
    assertDocsExistInBothReplicas(1, committedDocs);

    log.info("Partition nodes...");
    proxies.get(NODE0).close();
    proxies.get(NODE1).close();

    log.info("Adding {} (uncommitted) docs during network partition....", uncommittedDocs);
    addDocs(false, uncommittedDocs, committedDocs + 1);

    log.info("Stopping leader node...");
    assertEquals("Something broke our expected skipIndexWriterCommitOnClose",
                 TEST_VALUE_FOR_SKIP_COMMIT_ON_CLOSE, TestInjection.skipIndexWriterCommitOnClose);
    NODE0.stop();
    cluster.waitForJettyToStop(NODE0);

    log.info("Un-Partition replica (NODE1)...");
    proxies.get(NODE1).reopen();
    
    waitForState("Timeout waiting for leader goes DOWN", COLLECTION, (liveNodes, collectionState)
                 -> collectionState.getReplica(leader.getName()).getState() == Replica.State.DOWN);

    // Sanity check that a new (out of sync) replica doesn't come up in our place...
    expectThrows(TimeoutException.class,
                 "Did not time out waiting for new leader, out of sync replica became leader",
                 () -> {
                   cluster.getSolrClient().waitForState(COLLECTION, 10, TimeUnit.SECONDS, (state) -> {
            Replica newLeader = state.getSlice("shard1").getLeader();
            if (newLeader != null && !newLeader.getName().equals(leader.getName()) && newLeader.getState() == Replica.State.ACTIVE) {
              // this is is the bad case, our "bad" state was found before timeout
              log.error("WTF: New Leader={}", newLeader);
              return true;
            }
            return false; // still no bad state, wait for timeout
          });
      });

    log.info("Enabling TestInjection.updateLogReplayRandomPause");
    TestInjection.updateLogReplayRandomPause = "true:100";
      
    log.info("Un-Partition & restart leader (NODE0)...");
    proxies.get(NODE0).reopen();
    NODE0.start();

    log.info("Waiting for all nodes and active collection...");
    
    cluster.waitForAllNodes(30);;
    waitForState("Timeout waiting for leader", COLLECTION, (liveNodes, collectionState) -> {
      Replica newLeader = collectionState.getLeader("shard1");
      return newLeader != null && newLeader.getName().equals(leader.getName());
    });
    waitForState("Timeout waiting for active collection", COLLECTION, clusterShape(1, 2));
    
    cluster.waitForActiveCollection(COLLECTION, 1, 2);

    log.info("Check docs on both replicas...");
    assertDocsExistInBothReplicas(1, committedDocs + uncommittedDocs);
    
    log.info("Test ok, delete collection...");
    CollectionAdminRequest.deleteCollection(COLLECTION).process(cluster.getSolrClient());
  }

  /** 
   * Adds the specified number of docs directly to the leader, 
   * using increasing docIds begining with startId.  Commits if and only if the boolean is true.
   */
  private void addDocs(final boolean commit, final int numDocs, final int startId) throws SolrServerException, IOException {

    List<SolrInputDocument> docs = new ArrayList<>(numDocs);
    for (int i = 0; i < numDocs; i++) {
      int id = startId + i;
      docs.add(new SolrInputDocument("id", String.valueOf(id), "fieldName_s", String.valueOf(id)));
    }
    // For simplicity, we always add out docs directly to NODE0
    // (where the leader should be) and bypass the proxy...
    try (HttpSolrClient client = getHttpSolrClient(NODE0.getBaseUrl().toString())) {
      assertEquals(0, client.add(COLLECTION, docs).getStatus());
      if (commit) {
        assertEquals(0, client.commit(COLLECTION).getStatus());
      }
    }
  }

  /**
   * uses distrib=false RTG requests to verify that every doc between firstDocId and lastDocId 
   * (inclusive) can be found on both the leader and the replica
   */
  private void assertDocsExistInBothReplicas(int firstDocId,
                                             int lastDocId) throws Exception {
    try (HttpSolrClient leaderSolr = getHttpSolrClient(NODE0.getBaseUrl().toString());
         HttpSolrClient replicaSolr = getHttpSolrClient(NODE1.getBaseUrl().toString())) {
      for (int d = firstDocId; d <= lastDocId; d++) {
        String docId = String.valueOf(d);
        assertDocExists("leader", leaderSolr, docId);
        assertDocExists("replica", replicaSolr, docId);
      }
    }
  }

  /**
   * uses distrib=false RTG requests to verify that the specified docId can be found using the 
   * specified solr client
   */
  private void assertDocExists(final String clientName, final HttpSolrClient client, final String docId) throws Exception {
    final QueryResponse rsp = (new QueryRequest(params("qt", "/get",
                                                       "id", docId,
                                                       "_trace", clientName,
                                                       "distrib", "false")))
      .process(client, COLLECTION);
    assertEquals(0, rsp.getStatus());
    
    String match = JSONTestUtil.matchObj("/id", rsp.getResponse().get("doc"), docId);
    assertTrue("Doc with id=" + docId + " not found in " + clientName
               + " due to: " + match + "; rsp="+rsp, match == null);
  }

}
