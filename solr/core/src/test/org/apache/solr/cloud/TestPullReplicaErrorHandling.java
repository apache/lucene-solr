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
import java.net.URL;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.cloud.SocketProxy;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.CollectionStatePredicate;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.core.SolrCore;
import org.apache.solr.util.TestInjection;
import org.apache.solr.util.TimeOut;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressSSL(bugUrl = "https://issues.apache.org/jira/browse/SOLR-5776")
public class TestPullReplicaErrorHandling extends SolrCloudTestCase {
  
  private final static int REPLICATION_TIMEOUT_SECS = 10;
  
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static Map<URI, SocketProxy> proxies;
  private static Map<URI, JettySolrRunner> jettys;

  private String collectionName = null;
  
  private String suggestedCollectionName() {
    return (getTestClass().getSimpleName().replace("Test", "") + "_" + getSaferTestName().split(" ")[0]).replaceAll("(.)(\\p{Upper})", "$1_$2").toLowerCase(Locale.ROOT);
  }

  @BeforeClass
  public static void setupCluster() throws Exception {
    System.setProperty("solr.zkclienttimeout", "20000");

    configureCluster(4)
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
      cluster.waitForAllNodes(30);
      proxy.open(jetty.getBaseUrl().toURI());
      if (log.isInfoEnabled()) {
        log.info("Adding proxy for URL: {}. Proxy: {}", jetty.getBaseUrl(), proxy.getUrl());
      }
      proxies.put(proxy.getUrl(), proxy);
      jettys.put(proxy.getUrl(), jetty);
    }
    TimeOut t = new TimeOut(10, TimeUnit.SECONDS, TimeSource.NANO_TIME);
    while (true) {
      try {
        CollectionAdminRequest.ClusterProp clusterPropRequest = CollectionAdminRequest.setClusterProperty(ZkStateReader.LEGACY_CLOUD, "false");
        CollectionAdminResponse response = clusterPropRequest.process(cluster.getSolrClient());
        assertEquals(0, response.getStatus());
        break;
      } catch (SolrServerException e) {
        Thread.sleep(50);
        if (t.hasTimedOut()) {
          throw e;
        }
      }
    }
  }
  
  @AfterClass
  public static void tearDownCluster() throws Exception {
    if (null != proxies) {
      for (SocketProxy proxy : proxies.values()) {
        proxy.close();
      }
      proxies = null;
    }
    jettys = null;
    TestInjection.reset();
  }
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    collectionName = suggestedCollectionName();
    expectThrows(SolrException.class, () -> getCollectionState(collectionName));
    cluster.getSolrClient().setDefaultCollection(collectionName);
    cluster.waitForAllNodes(30);
  }

  @Override
  public void tearDown() throws Exception {
    if (cluster.getSolrClient().getZkStateReader().getClusterState().getCollectionOrNull(collectionName) != null) {
      log.info("tearDown deleting collection");
      CollectionAdminRequest.deleteCollection(collectionName).process(cluster.getSolrClient());
      log.info("Collection deleted");
      waitForDeletion(collectionName);
    }
    collectionName = null;
    super.tearDown();
  }
  
//  @Repeat(iterations=10)
//commented 9-Aug-2018  @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // added 20-Jul-2018
public void testCantConnectToPullReplica() throws Exception {
    int numShards = 2;
    CollectionAdminRequest.createCollection(collectionName, "conf", numShards, 1, 0, 1)
      .setMaxShardsPerNode(1)
      .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(collectionName, numShards, numShards * 2);
    addDocs(10);
    DocCollection docCollection = assertNumberOfReplicas(numShards, 0, numShards, false, true);
    Slice s = docCollection.getSlices().iterator().next();
    SocketProxy proxy = getProxyForReplica(s.getReplicas(EnumSet.of(Replica.Type.PULL)).get(0));
    try {
      proxy.close();
      for (int i = 1; i <= 10; i ++) {
        addDocs(10 + i);
        try (HttpSolrClient leaderClient = getHttpSolrClient(s.getLeader().getCoreUrl())) {
          assertNumDocs(10 + i, leaderClient);
        }
      }

      SolrServerException e = expectThrows(SolrServerException.class, () -> {
        try(HttpSolrClient pullReplicaClient = getHttpSolrClient(s.getReplicas(EnumSet.of(Replica.Type.PULL)).get(0).getCoreUrl())) {
          pullReplicaClient.query(new SolrQuery("*:*")).getResults().getNumFound();
        }
      });
      
      assertNumberOfReplicas(numShards, 0, numShards, true, true);// Replica should still be active, since it doesn't disconnect from ZooKeeper
      {
        long numFound = 0;
        TimeOut t = new TimeOut(REPLICATION_TIMEOUT_SECS, TimeUnit.SECONDS, TimeSource.NANO_TIME);
        while (numFound < 20 && !t.hasTimedOut()) {
          Thread.sleep(200);
          numFound = cluster.getSolrClient().query(collectionName, new SolrQuery("*:*")).getResults().getNumFound();
        }
      }
    } finally {
      proxy.reopen();
    }
    
    try (HttpSolrClient pullReplicaClient = getHttpSolrClient(s.getReplicas(EnumSet.of(Replica.Type.PULL)).get(0).getCoreUrl())) {
      assertNumDocs(20, pullReplicaClient);
    }
  }
  
  public void testCantConnectToLeader() throws Exception {
    int numShards = 1;
    CollectionAdminRequest.createCollection(collectionName, "conf", numShards, 1, 0, 1)
      .setMaxShardsPerNode(1)
      .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(collectionName, numShards, numShards * 2);
    addDocs(10);
    DocCollection docCollection = assertNumberOfReplicas(numShards, 0, numShards, false, true);
    Slice s = docCollection.getSlices().iterator().next();
    SocketProxy proxy = getProxyForReplica(s.getLeader());
    try {
      // wait for replication
      try (HttpSolrClient pullReplicaClient = getHttpSolrClient(s.getReplicas(EnumSet.of(Replica.Type.PULL)).get(0).getCoreUrl())) {
        assertNumDocs(10, pullReplicaClient);
      }
      proxy.close();
      expectThrows(SolrException.class, ()->addDocs(1));
      try (HttpSolrClient pullReplicaClient = getHttpSolrClient(s.getReplicas(EnumSet.of(Replica.Type.PULL)).get(0).getCoreUrl())) {
        assertNumDocs(10, pullReplicaClient);
      }
      assertNumDocs(10, cluster.getSolrClient());
    } finally {
      log.info("Opening leader node");
      proxy.reopen();
    }
//     Back to normal
//    Even if the leader is back to normal, the replica can get broken pipe for some time when trying to connect to it. The commit
//    can fail if it's sent to the replica and it forwards it to the leader, and since it uses CUSC the error is hidden! That breaks
//    the last part of this test.
//    addDocs(20);
//    assertNumDocs(20, cluster.getSolrClient(), 300);
//    try (HttpSolrClient pullReplicaClient = getHttpSolrClient(s.getReplicas(EnumSet.of(Replica.Type.PULL)).get(0).getCoreUrl())) {
//      assertNumDocs(20, pullReplicaClient);
//    }
  }
  
  public void testPullReplicaDisconnectsFromZooKeeper() throws Exception {
    int numShards = 1;
    CollectionAdminRequest.createCollection(collectionName, "conf", numShards, 1, 0, 1)
      .setMaxShardsPerNode(1)
      .process(cluster.getSolrClient());
    addDocs(10);
    DocCollection docCollection = assertNumberOfReplicas(numShards, 0, numShards, false, true);
    Slice s = docCollection.getSlices().iterator().next();
    try (HttpSolrClient pullReplicaClient = getHttpSolrClient(s.getReplicas(EnumSet.of(Replica.Type.PULL)).get(0).getCoreUrl())) {
      assertNumDocs(10, pullReplicaClient);
    }
    addDocs(20);
    JettySolrRunner jetty = getJettyForReplica(s.getReplicas(EnumSet.of(Replica.Type.PULL)).get(0));
    cluster.expireZkSession(jetty);
    addDocs(30);
    waitForState("Expecting node to be disconnected", collectionName, activeReplicaCount(1, 0, 0));
    addDocs(40);
    waitForState("Expecting node to be reconnected", collectionName, activeReplicaCount(1, 0, 1));
    try (HttpSolrClient pullReplicaClient = getHttpSolrClient(s.getReplicas(EnumSet.of(Replica.Type.PULL)).get(0).getCoreUrl())) {
      assertNumDocs(40, pullReplicaClient);
    }
  }

  public void testCloseHooksDeletedOnReconnect() throws Exception {
    CollectionAdminRequest.createCollection(collectionName, "conf", 1, 1, 0, 1)
      .process(cluster.getSolrClient());
    addDocs(10);

    DocCollection docCollection = assertNumberOfReplicas(1, 0, 1, false, true);
    Slice s = docCollection.getSlices().iterator().next();
    JettySolrRunner jetty = getJettyForReplica(s.getReplicas(EnumSet.of(Replica.Type.PULL)).get(0));
    SolrCore core = jetty.getCoreContainer().getCores().iterator().next();

    for (int i = 0; i < (TEST_NIGHTLY ? 5 : 2); i++) {
      cluster.expireZkSession(jetty);
      waitForState("Expecting node to be disconnected", collectionName, activeReplicaCount(1, 0, 0));
      waitForState("Expecting node to reconnect", collectionName, activeReplicaCount(1, 0, 1));
      // We have two active ReplicationHandler with two close hooks each, one for triggering recovery and one for doing interval polling
      assertEquals(5, core.getCloseHooks().size());
    }
  }
  
  private void assertNumDocs(int numDocs, SolrClient client, int timeoutSecs) throws InterruptedException, SolrServerException, IOException {
    TimeOut t = new TimeOut(timeoutSecs, TimeUnit.SECONDS, TimeSource.NANO_TIME);
    long numFound = -1;
    while (!t.hasTimedOut()) {
      Thread.sleep(200);
      numFound = client.query(new SolrQuery("*:*")).getResults().getNumFound();
      if (numFound == numDocs) {
        return;
      }
    }
    fail("Didn't get expected doc count. Expected: " + numDocs + ", Found: " + numFound);
  }
  
  
  private void assertNumDocs(int numDocs, SolrClient client) throws InterruptedException, SolrServerException, IOException {
    assertNumDocs(numDocs, client, REPLICATION_TIMEOUT_SECS);
  }

  private void addDocs(int numDocs) throws SolrServerException, IOException {
    List<SolrInputDocument> docs = new ArrayList<>(numDocs);
    for (int i = 0; i < numDocs; i++) {
      docs.add(new SolrInputDocument("id", String.valueOf(i), "fieldName_s", String.valueOf(i)));
    }
    cluster.getSolrClient().add(collectionName, docs);
    cluster.getSolrClient().commit(collectionName);
  }
  
  private DocCollection assertNumberOfReplicas(int numWriter, int numActive, int numPassive, boolean updateCollection, boolean activeOnly) throws KeeperException, InterruptedException {
    if (updateCollection) {
      cluster.getSolrClient().getZkStateReader().forceUpdateCollection(collectionName);
    }
    DocCollection docCollection = getCollectionState(collectionName);
    assertNotNull(docCollection);
    assertEquals("Unexpected number of writer replicas: " + docCollection, numWriter, 
        docCollection.getReplicas(EnumSet.of(Replica.Type.NRT)).stream().filter(r->!activeOnly || r.getState() == Replica.State.ACTIVE).count());
    assertEquals("Unexpected number of pull replicas: " + docCollection, numPassive, 
        docCollection.getReplicas(EnumSet.of(Replica.Type.PULL)).stream().filter(r->!activeOnly || r.getState() == Replica.State.ACTIVE).count());
    assertEquals("Unexpected number of active replicas: " + docCollection, numActive, 
        docCollection.getReplicas(EnumSet.of(Replica.Type.TLOG)).stream().filter(r->!activeOnly || r.getState() == Replica.State.ACTIVE).count());
    return docCollection;
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
  
  private void waitForDeletion(String collection) throws InterruptedException, KeeperException {
    TimeOut t = new TimeOut(10, TimeUnit.SECONDS, TimeSource.NANO_TIME);
    while (cluster.getSolrClient().getZkStateReader().getClusterState().hasCollection(collection)) {
      log.info("Collection not yet deleted");
      try {
        Thread.sleep(100);
        if (t.hasTimedOut()) {
          fail("Timed out waiting for collection " + collection + " to be deleted.");
        }
        cluster.getSolrClient().getZkStateReader().forceUpdateCollection(collection);
      } catch(SolrException e) {
        return;
      }
      
    }
  }
  
  private CollectionStatePredicate activeReplicaCount(int numWriter, int numActive, int numPassive) {
    return (liveNodes, collectionState) -> {
      int writersFound = 0, activesFound = 0, passivesFound = 0;
      if (collectionState == null)
        return false;
      for (Slice slice : collectionState) {
        for (Replica replica : slice) {
          if (replica.isActive(liveNodes))
            switch (replica.getType()) {
              case TLOG:
                activesFound++;
                break;
              case PULL:
                passivesFound++;
                break;
              case NRT:
                writersFound++;
                break;
              default:
                throw new AssertionError("Unexpected replica type");
            }
        }
      }
      return numWriter == writersFound && numActive == activesFound && numPassive == passivesFound;
    };
  }

}
