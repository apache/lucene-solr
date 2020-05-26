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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.response.CoreAdminResponse;
import org.apache.solr.client.solrj.response.RequestStatusState;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.util.IdUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MoveReplicaTest extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  // used by MoveReplicaHDFSTest
  protected boolean inPlaceMove = true;

  protected String getConfigSet() {
    return "cloud-dynamic";
  }

  @Before
  public void beforeTest() throws Exception {
    inPlaceMove = true;

    configureCluster(4)
        .addConfig("conf1", configset(getConfigSet()))
        .addConfig("conf2", configset(getConfigSet()))
        .withSolrXml(TEST_PATH().resolve("solr.xml"))
        .configure();

    NamedList<Object> overSeerStatus = cluster.getSolrClient().request(CollectionAdminRequest.getOverseerStatus());
    JettySolrRunner overseerJetty = null;
    String overseerLeader = (String) overSeerStatus.get("leader");
    for (JettySolrRunner jetty : cluster.getJettySolrRunners()) {
      if (jetty.getNodeName().equals(overseerLeader)) {
        overseerJetty = jetty;
        break;
      }
    }
    if (overseerJetty == null) {
      fail("no overseer leader!");
    }
  }

  @After
  public void afterTest() throws Exception {
    try {
      shutdownCluster();
    } finally {
      super.tearDown();
    }
  }

  @Test
  // commented out on: 17-Feb-2019   @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // annotated on: 24-Dec-2018
  public void test() throws Exception {
    String coll = getTestClass().getSimpleName() + "_coll_" + inPlaceMove;
    if (log.isInfoEnabled()) {
      log.info("total_jettys: {}", cluster.getJettySolrRunners().size());
    }
    int REPLICATION = 2;

    CloudSolrClient cloudClient = cluster.getSolrClient();

    // random create tlog or pull type replicas with nrt
    boolean isTlog = random().nextBoolean();
    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(coll, "conf1", 2, 1, isTlog ? 1 : 0, !isTlog ? 1 : 0);
    create.setMaxShardsPerNode(2);
    create.setAutoAddReplicas(false);
    cloudClient.request(create);

    addDocs(coll, 100);

    Replica replica = getRandomReplica(coll, cloudClient);
    Set<String> liveNodes = cloudClient.getZkStateReader().getClusterState().getLiveNodes();
    ArrayList<String> l = new ArrayList<>(liveNodes);
    Collections.shuffle(l, random());
    String targetNode = null;
    for (String node : liveNodes) {
      if (!replica.getNodeName().equals(node)) {
        targetNode = node;
        break;
      }
    }
    assertNotNull(targetNode);
    String shardId = null;
    for (Slice slice : cloudClient.getZkStateReader().getClusterState().getCollection(coll).getSlices()) {
      if (slice.getReplicas().contains(replica)) {
        shardId = slice.getName();
      }
    }

    int sourceNumCores = getNumOfCores(cloudClient, replica.getNodeName(), coll, replica.getType().name());
    int targetNumCores = getNumOfCores(cloudClient, targetNode, coll, replica.getType().name());

    CollectionAdminRequest.MoveReplica moveReplica = createMoveReplicaRequest(coll, replica, targetNode);
    moveReplica.setInPlaceMove(inPlaceMove);
    String asyncId = IdUtils.randomId();
    moveReplica.processAsync(asyncId, cloudClient);
    CollectionAdminRequest.RequestStatus requestStatus = CollectionAdminRequest.requestStatus(asyncId);
    // wait for async request success
    boolean success = false;
    for (int i = 0; i < 200; i++) {
      CollectionAdminRequest.RequestStatusResponse rsp = requestStatus.process(cloudClient);
      if (rsp.getRequestStatus() == RequestStatusState.COMPLETED) {
        success = true;
        break;
      }
      assertNotSame(rsp.getRequestStatus(), RequestStatusState.FAILED);
      Thread.sleep(500);
    }
    assertTrue(success);
    assertEquals("should be one less core on the source node!", sourceNumCores - 1, getNumOfCores(cloudClient, replica.getNodeName(), coll, replica.getType().name()));
    assertEquals("should be one more core on target node!", targetNumCores + 1, getNumOfCores(cloudClient, targetNode, coll, replica.getType().name()));
    // wait for recovery
    boolean recovered = false;
    for (int i = 0; i < 300; i++) {
      DocCollection collState = getCollectionState(coll);
      log.debug("###### {}", collState);
      Collection<Replica> replicas = collState.getSlice(shardId).getReplicas();
      boolean allActive = true;
      boolean hasLeaders = true;
      if (replicas != null && !replicas.isEmpty()) {
        for (Replica r : replicas) {
          if (!r.getNodeName().equals(targetNode)) {
            continue;
          }
          if (!r.isActive(Collections.singleton(targetNode))) {
            log.info("Not active: {}", r);
            allActive = false;
          }
        }
      } else {
        allActive = false;
      }
      for (Slice slice : collState.getSlices()) {
        if (slice.getLeader() == null) {
          hasLeaders = false;
        }
      }
      if (allActive && hasLeaders) {
        // check the number of active replicas
        assertEquals("total number of replicas", REPLICATION, replicas.size());
        recovered = true;
        break;
      } else {
        log.info("--- waiting, allActive={}, hasLeaders={}", allActive, hasLeaders);
        Thread.sleep(1000);
      }
    }
    assertTrue("replica never fully recovered", recovered);

    assertEquals(100, cluster.getSolrClient().query(coll, new SolrQuery("*:*")).getResults().getNumFound());

    moveReplica = createMoveReplicaRequest(coll, replica, targetNode, shardId);
    moveReplica.setInPlaceMove(inPlaceMove);
    moveReplica.process(cloudClient);
    checkNumOfCores(cloudClient, replica.getNodeName(), coll, sourceNumCores);
    // wait for recovery
    recovered = false;
    for (int i = 0; i < 300; i++) {
      DocCollection collState = getCollectionState(coll);
      log.debug("###### {}", collState);
      Collection<Replica> replicas = collState.getSlice(shardId).getReplicas();
      boolean allActive = true;
      boolean hasLeaders = true;
      if (replicas != null && !replicas.isEmpty()) {
        for (Replica r : replicas) {
          if (!r.getNodeName().equals(replica.getNodeName())) {
            continue;
          }
          if (!r.isActive(Collections.singleton(replica.getNodeName()))) {
            log.info("Not active yet: {}", r);
            allActive = false;
          }
        }
      } else {
        allActive = false;
      }
      for (Slice slice : collState.getSlices()) {
        if (slice.getLeader() == null) {
          hasLeaders = false;
        }
      }
      if (allActive && hasLeaders) {
        assertEquals("total number of replicas", REPLICATION, replicas.size());
        recovered = true;
        break;
      } else {
        Thread.sleep(1000);
      }
    }
    assertTrue("replica never fully recovered", recovered);

    assertEquals(100, cluster.getSolrClient().query(coll, new SolrQuery("*:*")).getResults().getNumFound());
  }

  //Commented out 5-Dec-2017
  // @AwaitsFix(bugUrl = "https://issues.apache.org/jira/browse/SOLR-11458")
  @Test
  // 12-Jun-2018 @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 17-Mar-2018 This JIRA is fixed, but this test still fails
  //17-Aug-2018 commented  @LuceneTestCase.BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 2-Aug-2018
  // commented out on: 17-Feb-2019   @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // annotated on: 24-Dec-2018
  public void testFailedMove() throws Exception {
    String coll = getTestClass().getSimpleName() + "_failed_coll_" + inPlaceMove;
    int REPLICATION = 2;

    CloudSolrClient cloudClient = cluster.getSolrClient();

    // random create tlog or pull type replicas with nrt
    boolean isTlog = random().nextBoolean();
    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(coll, "conf1", 2, 1, isTlog ? 1 : 0, !isTlog ? 1 : 0);
    create.setAutoAddReplicas(false);
    cloudClient.request(create);

    addDocs(coll, 100);

    NamedList<Object> overSeerStatus = cluster.getSolrClient().request(CollectionAdminRequest.getOverseerStatus());
    String overseerLeader = (String) overSeerStatus.get("leader");

    // don't kill overseer in this test
    Replica replica;
    int count = 10;
    do {
      replica = getRandomReplica(coll, cloudClient);
    } while (!replica.getNodeName().equals(overseerLeader) && count-- > 0);
    assertNotNull("could not find non-overseer replica???", replica);
    Set<String> liveNodes = cloudClient.getZkStateReader().getClusterState().getLiveNodes();
    ArrayList<String> l = new ArrayList<>(liveNodes);
    Collections.shuffle(l, random());
    String targetNode = null;
    for (String node : liveNodes) {
      if (!replica.getNodeName().equals(node) && !overseerLeader.equals(node)) {
        targetNode = node;
        break;
      }
    }
    assertNotNull(targetNode);
    CollectionAdminRequest.MoveReplica moveReplica = createMoveReplicaRequest(coll, replica, targetNode);
    moveReplica.setInPlaceMove(inPlaceMove);
    // start moving
    String asyncId = IdUtils.randomId();
    moveReplica.processAsync(asyncId, cloudClient);
    // shut down target node
    for (int i = 0; i < cluster.getJettySolrRunners().size(); i++) {
      if (cluster.getJettySolrRunner(i).getNodeName().equals(targetNode)) {
        JettySolrRunner j = cluster.stopJettySolrRunner(i);
        cluster.waitForJettyToStop(j);
        break;
      }
    }
    CollectionAdminRequest.RequestStatus requestStatus = CollectionAdminRequest.requestStatus(asyncId);
    // wait for async request success
    boolean success = true;
    for (int i = 0; i < 200; i++) {
      CollectionAdminRequest.RequestStatusResponse rsp = requestStatus.process(cloudClient);
      assertNotSame(rsp.getRequestStatus().toString(), rsp.getRequestStatus(), RequestStatusState.COMPLETED);
      if (rsp.getRequestStatus() == RequestStatusState.FAILED) {
        success = false;
        break;
      }
      Thread.sleep(500);
    }
    assertFalse(success);

    if (log.isInfoEnabled()) {
      log.info("--- current collection state: {}", cloudClient.getZkStateReader().getClusterState().getCollection(coll));
    }
    assertEquals(100, cluster.getSolrClient().query(coll, new SolrQuery("*:*")).getResults().getNumFound());
  }

  private CollectionAdminRequest.MoveReplica createMoveReplicaRequest(String coll, Replica replica, String targetNode, String shardId) {
    return new CollectionAdminRequest.MoveReplica(coll, shardId, targetNode, replica.getNodeName());
  }

  private CollectionAdminRequest.MoveReplica createMoveReplicaRequest(String coll, Replica replica, String targetNode) {
    return new CollectionAdminRequest.MoveReplica(coll, replica.getName(), targetNode);
  }

  private Replica getRandomReplica(String coll, CloudSolrClient cloudClient) {
    List<Replica> replicas = cloudClient.getZkStateReader().getClusterState().getCollection(coll).getReplicas();
    Collections.shuffle(replicas, random());
    return replicas.get(0);
  }

  private void checkNumOfCores(CloudSolrClient cloudClient, String nodeName, String collectionName, int expectedCores) throws IOException, SolrServerException {
    assertEquals(nodeName + " does not have expected number of cores", expectedCores, getNumOfCores(cloudClient, nodeName, collectionName));
  }

  private int getNumOfCores(CloudSolrClient cloudClient, String nodeName, String collectionName) throws IOException, SolrServerException {
    return getNumOfCores(cloudClient, nodeName, collectionName, null);
  }

  private int getNumOfCores(CloudSolrClient cloudClient, String nodeName, String collectionName, String replicaType) throws IOException, SolrServerException {
    try (HttpSolrClient coreclient = getHttpSolrClient(cloudClient.getZkStateReader().getBaseUrlForNodeName(nodeName))) {
      CoreAdminResponse status = CoreAdminRequest.getStatus(null, coreclient);
      if (status.getCoreStatus().size() == 0) {
        return 0;
      }
      if (collectionName == null && replicaType == null) {
        return status.getCoreStatus().size();
      }
      // filter size by collection name
      int size = 0;
      for (Map.Entry<String, NamedList<Object>> stringNamedListEntry : status.getCoreStatus()) {
        if (collectionName != null) {
          String coll = (String) stringNamedListEntry.getValue().findRecursive("cloud", "collection");
          if (!collectionName.equals(coll)) {
            continue;
          }
        }
        if (replicaType != null) {
          String type = (String) stringNamedListEntry.getValue().findRecursive("cloud", "replicaType");
          if (!replicaType.equals(type)) {
            continue;
          }
        }
        size++;
      }
      return size;
    }
  }

  protected void addDocs(String collection, int numDocs) throws Exception {
    SolrClient solrClient = cluster.getSolrClient();
    for (int docId = 1; docId <= numDocs; docId++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", docId);
      solrClient.add(collection, doc);
    }
    solrClient.commit(collection);
    Thread.sleep(5000);
  }
}
