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
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.solr.JSONTestUtil;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.util.TimeOut;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LIRRollingUpdatesTest extends SolrCloudTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static Map<URI, SocketProxy> proxies;
  private static Map<URI, JettySolrRunner> jettys;

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(3)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
    // Add proxies
    proxies = new HashMap<>(cluster.getJettySolrRunners().size());
    jettys = new HashMap<>(cluster.getJettySolrRunners().size());
    for (JettySolrRunner jetty:cluster.getJettySolrRunners()) {
      SocketProxy proxy = new SocketProxy();
      jetty.setProxyPort(proxy.getListenPort());
      cluster.stopJettySolrRunner(jetty);//TODO: Can we avoid this restart
      cluster.startJettySolrRunner(jetty);
      proxy.open(jetty.getBaseUrl().toURI());
      log.info("Adding proxy for URL: " + jetty.getBaseUrl() + ". Proxy: " + proxy.getUrl());
      proxies.put(proxy.getUrl(), proxy);
      jettys.put(proxy.getUrl(), jetty);
    }
  }


  @AfterClass
  public static void tearDownCluster() throws Exception {
    for (SocketProxy proxy:proxies.values()) {
      proxy.close();
    }
    proxies = null;
    jettys = null;
  }

  @Test
  // 12-Jun-2018 @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 21-May-2018
  @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // added 09-Aug-2018
  public void testNewReplicaOldLeader() throws Exception {

    String collection = "testNewReplicaOldLeader";
    CollectionAdminRequest.createCollection(collection, 1, 2)
        .setCreateNodeSet("")
        .process(cluster.getSolrClient());
    Properties oldLir = new Properties();
    oldLir.setProperty("lirVersion", "old");

    CollectionAdminRequest
        .addReplicaToShard(collection, "shard1")
        .setProperties(oldLir)
        .setNode(cluster.getJettySolrRunner(0).getNodeName())
        .process(cluster.getSolrClient());

    CollectionAdminRequest
        .addReplicaToShard(collection, "shard1")
        .setProperties(oldLir)
        .setNode(cluster.getJettySolrRunner(1).getNodeName())
        .process(cluster.getSolrClient());
    waitForState("Time waiting for 1x2 collection", collection, clusterShape(1, 2));

    addDocs(collection, 2, 0);

    Slice shard1 = getCollectionState(collection).getSlice("shard1");
    //introduce network partition between leader & replica
    Replica notLeader = shard1.getReplicas(x -> x != shard1.getLeader()).get(0);
    assertTrue(runInOldLIRMode(collection, "shard1", notLeader));
    getProxyForReplica(notLeader).close();
    getProxyForReplica(shard1.getLeader()).close();

    addDoc(collection, 2, getJettyForReplica(shard1.getLeader()));
    waitForState("Replica " + notLeader.getName() + " is not put as DOWN", collection,
        (liveNodes, collectionState) ->
            collectionState.getSlice("shard1").getReplica(notLeader.getName()).getState() == Replica.State.DOWN);
    getProxyForReplica(shard1.getLeader()).reopen();
    getProxyForReplica(notLeader).reopen();
    // make sure that, when new replica works with old leader, it still can recovery normally
    waitForState("Timeout waiting for recovering", collection, clusterShape(1, 2));
    assertDocsExistInAllReplicas(Collections.singletonList(notLeader), collection, 0, 2);

    // make sure that, when new replica restart during LIR, it still can recovery normally (by looking at LIR node)
    getProxyForReplica(notLeader).close();
    getProxyForReplica(shard1.getLeader()).close();

    addDoc(collection, 3, getJettyForReplica(shard1.getLeader()));
    waitForState("Replica " + notLeader.getName() + " is not put as DOWN", collection,
        (liveNodes, collectionState) ->
            collectionState.getSlice("shard1").getReplica(notLeader.getName()).getState() == Replica.State.DOWN);

    JettySolrRunner notLeaderJetty = getJettyForReplica(notLeader);
    notLeaderJetty.stop();
    waitForState("Node did not leave", collection, (liveNodes, collectionState) -> liveNodes.size() == 2);
    upgrade(notLeaderJetty);
    notLeaderJetty.start();

    getProxyForReplica(shard1.getLeader()).reopen();
    getProxyForReplica(notLeader).reopen();
    waitForState("Timeout waiting for recovering", collection, clusterShape(1, 2));
    assertFalse(runInOldLIRMode(collection, "shard1", notLeader));
    assertDocsExistInAllReplicas(Collections.singletonList(notLeader), collection, 0, 3);

    CollectionAdminRequest.deleteCollection(collection).process(cluster.getSolrClient());
  }

  @Test
  // 12-Jun-2018 @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 04-May-2018
  @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // added 09-Aug-2018
  public void testNewLeaderOldReplica() throws Exception {
    // in case of new leader & old replica, new leader can still put old replica into LIR

    String collection = "testNewLeaderOldReplica";
    CollectionAdminRequest.createCollection(collection, 1, 2)
        .setCreateNodeSet("")
        .process(cluster.getSolrClient());
    Properties oldLir = new Properties();
    oldLir.setProperty("lirVersion", "old");

    CollectionAdminRequest
        .addReplicaToShard(collection, "shard1")
        .setNode(cluster.getJettySolrRunner(0).getNodeName())
        .process(cluster.getSolrClient());
    waitForState("Timeout waiting for shard1 become active", collection, (liveNodes, collectionState) -> {
      Slice shard1 = collectionState.getSlice("shard1");
      if (shard1.getReplicas().size() == 1 && shard1.getLeader() != null) return true;
      return false;
    });

    CollectionAdminRequest
        .addReplicaToShard(collection, "shard1")
        .setProperties(oldLir)
        .setNode(cluster.getJettySolrRunner(1).getNodeName())
        .process(cluster.getSolrClient());
    waitForState("Time waiting for 1x2 collection", collection, clusterShape(1, 2));

    Slice shard1 = getCollectionState(collection).getSlice("shard1");
    Replica notLeader = shard1.getReplicas(x -> x != shard1.getLeader()).get(0);
    Replica leader = shard1.getLeader();

    assertTrue(runInOldLIRMode(collection, "shard1", notLeader));
    assertFalse(runInOldLIRMode(collection, "shard1", leader));

    addDocs(collection, 2, 0);
    getProxyForReplica(notLeader).close();
    getProxyForReplica(leader).close();

    JettySolrRunner leaderJetty = getJettyForReplica(leader);
    addDoc(collection, 2, leaderJetty);
    waitForState("Replica " + notLeader.getName() + " is not put as DOWN", collection,
        (liveNodes, collectionState) ->
            collectionState.getSlice("shard1").getReplica(notLeader.getName()).getState() == Replica.State.DOWN);
    // wait a little bit
    Thread.sleep(500);
    getProxyForReplica(notLeader).reopen();
    getProxyForReplica(leader).reopen();

    waitForState("Timeout waiting for recovering", collection, clusterShape(1, 2));
    assertDocsExistInAllReplicas(Collections.singletonList(notLeader), collection, 0, 2);

    // ensure that after recovery, the upgraded replica will clean its LIR status cause it is no longer needed
    assertFalse(cluster.getSolrClient().getZkStateReader().getZkClient().exists(
        ZkController.getLeaderInitiatedRecoveryZnodePath(collection, "shard1", notLeader.getName()), true));
    // ensure that, leader should not register other replica's term
    try (ZkShardTerms zkShardTerms = new ZkShardTerms(collection, "shard1", cluster.getZkClient())) {
      assertFalse(zkShardTerms.getTerms().containsKey(notLeader.getName()));
    }
    CollectionAdminRequest.deleteCollection(collection).process(cluster.getSolrClient());
  }

  public void testLeaderAndMixedReplicas(boolean leaderInOldMode) throws Exception {
    // in case of new leader and mixed old replica and new replica, new leader can still put all of them into recovery
    // step1 : setup collection
    String collection = "testMixedReplicas-"+leaderInOldMode;
    CollectionAdminRequest.createCollection(collection, 1, 2)
        .setCreateNodeSet("")
        .process(cluster.getSolrClient());
    Properties oldLir = new Properties();
    oldLir.setProperty("lirVersion", "old");

    if (leaderInOldMode) {
      CollectionAdminRequest
          .addReplicaToShard(collection, "shard1")
          .setProperties(oldLir)
          .setNode(cluster.getJettySolrRunner(0).getNodeName())
          .process(cluster.getSolrClient());
    } else {
      CollectionAdminRequest
          .addReplicaToShard(collection, "shard1")
          .setNode(cluster.getJettySolrRunner(0).getNodeName())
          .process(cluster.getSolrClient());
    }

    waitForState("Timeout waiting for shard1 become active", collection, clusterShape(1, 1));

    CollectionAdminRequest
        .addReplicaToShard(collection, "shard1")
        .setProperties(oldLir)
        .setNode(cluster.getJettySolrRunner(1).getNodeName())
        .process(cluster.getSolrClient());

    CollectionAdminRequest
        .addReplicaToShard(collection, "shard1")
        .setNode(cluster.getJettySolrRunner(2).getNodeName())
        .process(cluster.getSolrClient());
    waitForState("Timeout waiting for shard1 become active", collection, clusterShape(1, 3));

    Slice shard1 = getCollectionState(collection).getSlice("shard1");
    Replica replicaInOldMode = shard1.getReplicas(x -> x != shard1.getLeader()).get(0);
    Replica replicaInNewMode = shard1.getReplicas(x -> x != shard1.getLeader()).get(1);
    Replica leader = shard1.getLeader();

    assertEquals(leaderInOldMode, runInOldLIRMode(collection, "shard1", leader));
    if (!runInOldLIRMode(collection, "shard1", replicaInOldMode)) {
      Replica temp = replicaInOldMode;
      replicaInOldMode = replicaInNewMode;
      replicaInNewMode = temp;
    }
    assertTrue(runInOldLIRMode(collection, "shard1", replicaInOldMode));
    assertFalse(runInOldLIRMode(collection, "shard1", replicaInNewMode));

    addDocs(collection, 2, 0);

    // step2 : introduce network partition then add doc, replicas should be put into recovery
    getProxyForReplica(replicaInOldMode).close();
    getProxyForReplica(replicaInNewMode).close();
    getProxyForReplica(leader).close();

    JettySolrRunner leaderJetty = getJettyForReplica(leader);
    addDoc(collection, 2, leaderJetty);

    Replica finalReplicaInOldMode = replicaInOldMode;
    waitForState("Replica " + replicaInOldMode.getName() + " is not put as DOWN", collection,
        (liveNodes, collectionState) ->
            collectionState.getSlice("shard1").getReplica(finalReplicaInOldMode.getName()).getState() == Replica.State.DOWN);
    Replica finalReplicaInNewMode = replicaInNewMode;
    waitForState("Replica " + finalReplicaInNewMode.getName() + " is not put as DOWN", collection,
        (liveNodes, collectionState) ->
            collectionState.getSlice("shard1").getReplica(finalReplicaInNewMode.getName()).getState() == Replica.State.DOWN);

    // wait a little bit
    Thread.sleep(500);
    getProxyForReplica(replicaInOldMode).reopen();
    getProxyForReplica(replicaInNewMode).reopen();
    getProxyForReplica(leader).reopen();

    waitForState("Timeout waiting for recovering", collection, clusterShape(1, 3));
    assertDocsExistInAllReplicas(Arrays.asList(replicaInNewMode, replicaInOldMode), collection, 0, 2);

    addDocs(collection, 3, 3);

    // ensure that, leader should not register other replica's term
    try (ZkShardTerms zkShardTerms = new ZkShardTerms(collection, "shard1", cluster.getZkClient())) {
      assertFalse(zkShardTerms.getTerms().containsKey(replicaInOldMode.getName()));
    }

    // step3 : upgrade the replica running in old mode to the new mode
    getProxyForReplica(leader).close();
    getProxyForReplica(replicaInOldMode).close();
    addDoc(collection, 6, leaderJetty);
    JettySolrRunner oldJetty = getJettyForReplica(replicaInOldMode);
    oldJetty.stop();
    waitForState("Node did not leave", collection, (liveNodes, collectionState)
        -> liveNodes.size() == 2);
    upgrade(oldJetty);

    oldJetty.start();
    getProxyForReplica(leader).reopen();
    getProxyForReplica(replicaInOldMode).reopen();

    waitForState("Timeout waiting for recovering", collection, clusterShape(1, 3));
    assertDocsExistInAllReplicas(Arrays.asList(replicaInNewMode, replicaInOldMode), collection, 0, 6);

    CollectionAdminRequest.deleteCollection(collection).process(cluster.getSolrClient());
  }

  @Test
  // 12-Jun-2018 @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 04-May-2018
  @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // added 09-Aug-2018
  public void testNewLeaderAndMixedReplicas() throws Exception {
    testLeaderAndMixedReplicas(false);
  }

  @Test
  // 12-Jun-2018 @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 04-May-2018
  public void testOldLeaderAndMixedReplicas() throws Exception {
    testLeaderAndMixedReplicas(true);
  }

  private void upgrade(JettySolrRunner solrRunner) {
    File[] corePaths = new File(solrRunner.getSolrHome()).listFiles();
    for (File corePath : corePaths) {
      File coreProperties = new File(corePath, "core.properties");
      if (!coreProperties.exists()) continue;
      Properties properties = new Properties();

      try (Reader reader = new InputStreamReader(new FileInputStream(coreProperties), "UTF-8")) {
        properties.load(reader);
      } catch (Exception e) {
        continue;
      }
      properties.remove("lirVersion");
      try (Writer writer = new OutputStreamWriter(new FileOutputStream(coreProperties), "UTF-8")) {
        properties.store(writer, "Upgraded");
      } catch (Exception e) {
        continue;
      }
    }
  }

  protected void assertDocsExistInAllReplicas(List<Replica> notLeaders,
                                              String testCollectionName, int firstDocId, int lastDocId)
      throws Exception {
    Replica leader =
        cluster.getSolrClient().getZkStateReader().getLeaderRetry(testCollectionName, "shard1", 10000);
    HttpSolrClient leaderSolr = getHttpSolrClient(leader, testCollectionName);
    List<HttpSolrClient> replicas =
        new ArrayList<HttpSolrClient>(notLeaders.size());

    for (Replica r : notLeaders) {
      replicas.add(getHttpSolrClient(r, testCollectionName));
    }
    try {
      for (int d = firstDocId; d <= lastDocId; d++) {
        String docId = String.valueOf(d);
        assertDocExists(leaderSolr, testCollectionName, docId);
        for (HttpSolrClient replicaSolr : replicas) {
          assertDocExists(replicaSolr, testCollectionName, docId);
        }
      }
    } finally {
      if (leaderSolr != null) {
        leaderSolr.close();
      }
      for (HttpSolrClient replicaSolr : replicas) {
        replicaSolr.close();
      }
    }
  }

  protected void assertDocExists(HttpSolrClient solr, String coll, String docId) throws Exception {
    NamedList rsp = realTimeGetDocId(solr, docId);
    String match = JSONTestUtil.matchObj("/id", rsp.get("doc"), docId);
    assertTrue("Doc with id=" + docId + " not found in " + solr.getBaseURL()
        + " due to: " + match + "; rsp="+rsp, match == null);
  }

  private NamedList realTimeGetDocId(HttpSolrClient solr, String docId) throws SolrServerException, IOException {
    QueryRequest qr = new QueryRequest(params("qt", "/get", "id", docId, "distrib", "false"));
    return solr.request(qr);
  }

  protected HttpSolrClient getHttpSolrClient(Replica replica, String coll) throws Exception {
    ZkCoreNodeProps zkProps = new ZkCoreNodeProps(replica);
    String url = zkProps.getBaseUrl() + "/" + coll;
    return getHttpSolrClient(url);
  }

  private <T> void waitFor(int waitTimeInSecs, T expected, Supplier<T> supplier) throws InterruptedException {
    TimeOut timeOut = new TimeOut(waitTimeInSecs, TimeUnit.SECONDS, new TimeSource.CurrentTimeSource());
    while (!timeOut.hasTimedOut()) {
      if (expected == supplier.get()) return;
      Thread.sleep(100);
    }
    assertEquals(expected, supplier.get());
  }

  private boolean runInOldLIRMode(String collection, String shard, Replica replica) {
    try (ZkShardTerms shardTerms = new ZkShardTerms(collection, shard, cluster.getZkClient())) {
      return !shardTerms.registered(replica.getName());
    }
  }

  private void addDoc(String collection, int docId, JettySolrRunner solrRunner) throws IOException, SolrServerException {
    try (HttpSolrClient solrClient = new HttpSolrClient.Builder(solrRunner.getBaseUrl().toString()).build()) {
      solrClient.add(collection, new SolrInputDocument("id", String.valueOf(docId), "fieldName_s", String.valueOf(docId)));
    }
  }

  private void addDocs(String collection, int numDocs, int startId) throws SolrServerException, IOException {
    List<SolrInputDocument> docs = new ArrayList<>(numDocs);
    for (int i = 0; i < numDocs; i++) {
      int id = startId + i;
      docs.add(new SolrInputDocument("id", String.valueOf(id), "fieldName_s", String.valueOf(id)));
    }
    cluster.getSolrClient().add(collection, docs);
    cluster.getSolrClient().commit(collection);
  }


  protected JettySolrRunner getJettyForReplica(Replica replica) throws Exception {
    String replicaBaseUrl = replica.getStr(ZkStateReader.BASE_URL_PROP);
    assertNotNull(replicaBaseUrl);
    URL baseUrl = new URL(replicaBaseUrl);

    JettySolrRunner proxy = jettys.get(baseUrl.toURI());
    assertNotNull("No proxy found for " + baseUrl + "!", proxy);
    return proxy;
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
}
