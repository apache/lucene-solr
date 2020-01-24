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
package org.apache.solr.cloud.autoscaling;

import static org.apache.solr.common.util.Utils.makeMap;

import java.lang.invoke.MethodHandles;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.cloud.CollectionStatePredicate;
import org.apache.solr.common.cloud.ClusterStateUtil;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.util.TimeOut;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@org.apache.solr.util.LogLevel("org.apache.solr.cloud.autoscaling=DEBUG;org.apache.solr.cloud.autoscaling.NodeLostTrigger=TRACE;org.apache.solr.cloud.Overseer=DEBUG;org.apache.solr.cloud.overseer=DEBUG")
public class AutoAddReplicasIntegrationTest extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());


  protected String getConfigSet() {
    return "cloud-minimal";
  }
  
  @Before
  public void setupCluster() throws Exception {
    configureCluster(3)
        .addConfig("conf", configset(getConfigSet()))
        .withSolrXml(TEST_PATH().resolve("solr.xml"))
        .configure();

    new V2Request.Builder("/cluster")
        .withMethod(SolrRequest.METHOD.POST)
        .withPayload("{set-obj-property:{defaults : {cluster: {useLegacyReplicaAssignment:true}}}}")
        .build()
        .process(cluster.getSolrClient());
  }
  
  @After
  public void tearDown() throws Exception {
    try {
      shutdownCluster();
    } finally {
      super.tearDown();
    }
  }

  /**
   * Test that basic autoAddReplicaLogic kicks in when a node is lost 
   */
  @Test
  public void testSimple() throws Exception {
    final String COLLECTION = "test_simple";
    final ZkStateReader zkStateReader = cluster.getSolrClient().getZkStateReader();
    final JettySolrRunner jetty1 = cluster.getJettySolrRunner(1);
    final JettySolrRunner jetty2 = cluster.getJettySolrRunner(2);
    log.info("Creating {} using jetty1:{}/{} and jetty2:{}/{}", COLLECTION,
             jetty1.getNodeName(), jetty1.getLocalPort(),
             jetty2.getNodeName(), jetty2.getLocalPort());
             
    CollectionAdminRequest.createCollection(COLLECTION, "conf", 2, 2)
      .setCreateNodeSet(jetty1.getNodeName()+","+jetty2.getNodeName())
      .setAutoAddReplicas(true)
      .setMaxShardsPerNode(2)
      .process(cluster.getSolrClient());
    
    cluster.waitForActiveCollection(COLLECTION, 2, 4);
    
    // start the tests
    JettySolrRunner lostJetty = random().nextBoolean() ? jetty1 : jetty2;
    String lostNodeName = lostJetty.getNodeName();
    List<Replica> replacedHdfsReplicas = getReplacedSharedFsReplicas(COLLECTION, zkStateReader, lostNodeName);
    log.info("Stopping random node: {} / {}", lostNodeName, lostJetty.getLocalPort());
    lostJetty.stop();
    
    cluster.waitForJettyToStop(lostJetty);
    waitForNodeLeave(lostNodeName);
    
    waitForState(COLLECTION + "=(2,4) w/o down replicas",
                 COLLECTION, clusterShapeNoDownReplicas(2,4), 90, TimeUnit.SECONDS);
                 
    checkSharedFsReplicasMovedCorrectly(replacedHdfsReplicas, zkStateReader, COLLECTION);
    
    log.info("Re-starting (same) random node: {} / {}", lostNodeName, lostJetty.getLocalPort());
    lostJetty.start();
    
    waitForNodeLive(lostJetty);
    
    assertTrue("Timeout waiting for all live and active",
               ClusterStateUtil.waitForAllActiveAndLiveReplicas(zkStateReader, 90000));

  }

  /**
   * Test that basic autoAddReplicaLogic logic is <b>not</b> used if the cluster prop for it is disabled 
   * (even if sys prop is set after collection is created)
   */
  @Test
  public void testClusterPropOverridesCollecitonProp() throws Exception {
    final String COLLECTION = "test_clusterprop";
    final ZkStateReader zkStateReader = cluster.getSolrClient().getZkStateReader();
    final JettySolrRunner jetty1 = cluster.getJettySolrRunner(1);
    final JettySolrRunner jetty2 = cluster.getJettySolrRunner(2);

    log.info("Creating {} using jetty1:{}/{} and jetty2:{}/{}", COLLECTION,
             jetty1.getNodeName(), jetty1.getLocalPort(),
             jetty2.getNodeName(), jetty2.getLocalPort());
             
    CollectionAdminRequest.createCollection(COLLECTION, "conf", 2, 2)
      .setCreateNodeSet(jetty1.getNodeName()+","+jetty2.getNodeName())
      .setAutoAddReplicas(true)
      .setMaxShardsPerNode(2)
      .process(cluster.getSolrClient());
    
    cluster.waitForActiveCollection(COLLECTION, 2, 4);

    // check cluster property is considered
    disableAutoAddReplicasInCluster();

    JettySolrRunner lostJetty = random().nextBoolean() ? jetty1 : jetty2;
    String lostNodeName = lostJetty.getNodeName();
    List<Replica> replacedHdfsReplicas = getReplacedSharedFsReplicas(COLLECTION, zkStateReader, lostNodeName);
    
    log.info("Stopping random node: {} / {}", lostNodeName, lostJetty.getLocalPort());
    lostJetty.stop();
    
    cluster.waitForJettyToStop(lostJetty);
    
    waitForNodeLeave(lostNodeName);
    
    waitForState(COLLECTION + "=(2,2)", COLLECTION,
                 clusterShape(2, 2), 90, TimeUnit.SECONDS);
                 
    
    log.info("Re-starting (same) random node: {} / {}", lostNodeName, lostJetty.getLocalPort());
    lostJetty.start();
    
    waitForNodeLive(lostJetty);
    
    assertTrue("Timeout waiting for all live and active",
               ClusterStateUtil.waitForAllActiveAndLiveReplicas(zkStateReader, 90000));
    
    waitForState(COLLECTION + "=(2,4) w/o down replicas",
                 COLLECTION, clusterShapeNoDownReplicas(2,4), 90, TimeUnit.SECONDS);

  }

  /**
   * Test that we can modify a collection after creation to add autoAddReplicas.
   */
  @Test
  public void testAddCollectionPropAfterCreation() throws Exception {
    final String COLLECTION = "test_addprop";
    final ZkStateReader zkStateReader = cluster.getSolrClient().getZkStateReader();
    final JettySolrRunner jetty1 = cluster.getJettySolrRunner(1);
    final JettySolrRunner jetty2 = cluster.getJettySolrRunner(2);

    log.info("Creating {} using jetty1:{}/{} and jetty2:{}/{}", COLLECTION,
             jetty1.getNodeName(), jetty1.getLocalPort(),
             jetty2.getNodeName(), jetty2.getLocalPort());
             
    CollectionAdminRequest.createCollection(COLLECTION, "conf", 2, 2)
      .setCreateNodeSet(jetty1.getNodeName()+","+jetty2.getNodeName())
      .setAutoAddReplicas(false) // NOTE: false
      .setMaxShardsPerNode(2)
      .process(cluster.getSolrClient());
    
    cluster.waitForActiveCollection(COLLECTION, 2, 4);
    
    log.info("Modifying {} to use autoAddReplicas", COLLECTION);
    new CollectionAdminRequest.AsyncCollectionAdminRequest(CollectionParams.CollectionAction.MODIFYCOLLECTION) {
      @Override
      public SolrParams getParams() {
        ModifiableSolrParams params = (ModifiableSolrParams) super.getParams();
        params.set("collection", COLLECTION);
        params.set("autoAddReplicas", true);
        return params;
      }
    }.process(cluster.getSolrClient());

    JettySolrRunner lostJetty = random().nextBoolean() ? jetty1 : jetty2;
    String lostNodeName = lostJetty.getNodeName();
    List<Replica> replacedHdfsReplicas = getReplacedSharedFsReplicas(COLLECTION, zkStateReader, lostNodeName);

    log.info("Stopping random node: {} / {}", lostNodeName, lostJetty.getLocalPort());
    lostJetty.stop();
    
    cluster.waitForJettyToStop(lostJetty);
    
    waitForNodeLeave(lostNodeName);

    waitForState(COLLECTION + "=(2,4) w/o down replicas",
                 COLLECTION, clusterShapeNoDownReplicas(2,4), 90, TimeUnit.SECONDS);
    checkSharedFsReplicasMovedCorrectly(replacedHdfsReplicas, zkStateReader, COLLECTION);
    
    log.info("Re-starting (same) random node: {} / {}", lostNodeName, lostJetty.getLocalPort());
    lostJetty.start();
    
    waitForNodeLive(lostJetty);
    
    assertTrue("Timeout waiting for all live and active",
               ClusterStateUtil.waitForAllActiveAndLiveReplicas(zkStateReader, 90000));
  }

  /**
   * Test a specific sequence of problematic events:
   * <ul>
   *  <li>create a collection with autoAddReplicas=<b>false</b></li>
   *  <li>stop a nodeX in use by the collection</li>
   *  <li>re-start nodeX</li>
   *  <li>set autoAddReplicas=<b>true</b></li>
   *  <li>re-stop nodeX</li>
   * </ul>
   */
  @Test
  @AwaitsFix(bugUrl="https://issues.apache.org/jira/browse/SOLR-13811")
  public void testRapidStopStartStopWithPropChange() throws Exception {

    // This is the collection we'll be focused on in our testing...
    final String COLLECTION = "test_stoptwice";
    // This is a collection we'll use as a "marker" to ensure we "wait" for the
    // autoAddReplicas logic (via NodeLostTrigger) to kick in at least once before proceeding...
    final String ALT_COLLECTION = "test_dummy";
    
    final ZkStateReader zkStateReader = cluster.getSolrClient().getZkStateReader();
    final JettySolrRunner jetty1 = cluster.getJettySolrRunner(1);
    final JettySolrRunner jetty2 = cluster.getJettySolrRunner(2);

    log.info("Creating {} using jetty1:{}/{} and jetty2:{}/{}", COLLECTION,
             jetty1.getNodeName(), jetty1.getLocalPort(),
             jetty2.getNodeName(), jetty2.getLocalPort());
             
    CollectionAdminRequest.createCollection(COLLECTION, "conf", 2, 2)
      .setCreateNodeSet(jetty1.getNodeName()+","+jetty2.getNodeName())
      .setAutoAddReplicas(false) // NOTE: false
      .setMaxShardsPerNode(2)
      .process(cluster.getSolrClient());
    
    log.info("Creating {} using jetty1:{}/{} and jetty2:{}/{}", ALT_COLLECTION,
             jetty1.getNodeName(), jetty1.getLocalPort(),
             jetty2.getNodeName(), jetty2.getLocalPort());
             
    CollectionAdminRequest.createCollection(ALT_COLLECTION, "conf", 2, 2)
      .setCreateNodeSet(jetty1.getNodeName()+","+jetty2.getNodeName())
      .setAutoAddReplicas(true) // NOTE: true
      .setMaxShardsPerNode(2)
      .process(cluster.getSolrClient());
    
    cluster.waitForActiveCollection(COLLECTION, 2, 4);
    cluster.waitForActiveCollection(ALT_COLLECTION, 2, 4);

    JettySolrRunner lostJetty = random().nextBoolean() ? jetty1 : jetty2;
    String lostNodeName = lostJetty.getNodeName();
    List<Replica> replacedHdfsReplicas = getReplacedSharedFsReplicas(COLLECTION, zkStateReader, lostNodeName);

    log.info("Stopping random node: {} / {}", lostNodeName, lostJetty.getLocalPort());
    lostJetty.stop();
    
    cluster.waitForJettyToStop(lostJetty);
    waitForNodeLeave(lostNodeName);
    
    // ensure that our marker collection indicates that the autoAddReplicas logic
    // has detected the down node and done some processing
    waitForState(ALT_COLLECTION + "=(2,4) w/o down replicas",
                 ALT_COLLECTION, clusterShapeNoDownReplicas(2,4), 90, TimeUnit.SECONDS);

    waitForState(COLLECTION + "=(2,2)", COLLECTION, clusterShape(2, 2));
    
    log.info("Re-starting (same) random node: {} / {}", lostNodeName, lostJetty.getLocalPort());
    lostJetty.start();
    // save time, don't bother waiting for lostJetty to start until after updating collection prop...
    
    log.info("Modifying {} to use autoAddReplicas", COLLECTION);
    new CollectionAdminRequest.AsyncCollectionAdminRequest(CollectionParams.CollectionAction.MODIFYCOLLECTION) {
      @Override
      public SolrParams getParams() {
        ModifiableSolrParams params = (ModifiableSolrParams) super.getParams();
        params.set("collection", COLLECTION);
        params.set("autoAddReplicas", true);
        return params;
      }
    }.process(cluster.getSolrClient());

    // make sure lostJetty is fully up before stopping again...
    waitForNodeLive(lostJetty);

    log.info("Re-Stopping (same) random node: {} / {}", lostNodeName, lostJetty.getLocalPort());
    lostJetty.stop();
    
    cluster.waitForJettyToStop(lostJetty);
    waitForNodeLeave(lostNodeName);

    // TODO: this is the problematic situation...
    // wether or not NodeLostTrigger noticed that lostJetty was re-started and shutdown *again*
    // and that the new auoAddReplicas=true since the last time lostJetty was shutdown is respected
    waitForState(COLLECTION + "=(2,4) w/o down replicas",
                 COLLECTION, clusterShapeNoDownReplicas(2,4), 90, TimeUnit.SECONDS);
    checkSharedFsReplicasMovedCorrectly(replacedHdfsReplicas, zkStateReader, COLLECTION);
    
    log.info("Re-Re-starting (same) random node: {} / {}", lostNodeName, lostJetty.getLocalPort());
    lostJetty.start();
    
    waitForNodeLive(lostJetty);
    
    assertTrue("Timeout waiting for all live and active",
               ClusterStateUtil.waitForAllActiveAndLiveReplicas(zkStateReader, 90000));
  }
  
  private void disableAutoAddReplicasInCluster() throws SolrServerException, IOException {
    Map m = makeMap(
        "action", CollectionParams.CollectionAction.CLUSTERPROP.toLower(),
        "name", ZkStateReader.AUTO_ADD_REPLICAS,
        "val", "false");
    QueryRequest request = new QueryRequest(new MapSolrParams(m));
    request.setPath("/admin/collections");
    cluster.getSolrClient().request(request);
  }

  private void enableAutoAddReplicasInCluster() throws SolrServerException, IOException {
    Map m = makeMap(
        "action", CollectionParams.CollectionAction.CLUSTERPROP.toLower(),
        "name", ZkStateReader.AUTO_ADD_REPLICAS);
    QueryRequest request = new QueryRequest(new MapSolrParams(m));
    request.setPath("/admin/collections");
    cluster.getSolrClient().request(request);
  }

  private void checkSharedFsReplicasMovedCorrectly(List<Replica> replacedHdfsReplicas, ZkStateReader zkStateReader, String collection){
    DocCollection docCollection = zkStateReader.getClusterState().getCollection(collection);
    for (Replica replica :replacedHdfsReplicas) {
      boolean found = false;
      String dataDir = replica.getStr("dataDir");
      String ulogDir = replica.getStr("ulogDir");
      for (Replica replica2 : docCollection.getReplicas()) {
        if (dataDir.equals(replica2.getStr("dataDir")) && ulogDir.equals(replica2.getStr("ulogDir"))) {
          found = true;
          break;
        }
      }
      if (!found) fail("Can not found a replica with same dataDir and ulogDir as " + replica + " from:" + docCollection.getReplicas());
    }
  }

  private List<Replica> getReplacedSharedFsReplicas(String collection, ZkStateReader zkStateReader, String lostNodeName) {
    List<Replica> replacedHdfsReplicas = new ArrayList<>();
    for (Replica replica : zkStateReader.getClusterState().getCollection(collection).getReplicas()) {
      String dataDir = replica.getStr("dataDir");
      if (replica.getNodeName().equals(lostNodeName) && dataDir != null) {
        replacedHdfsReplicas.add(replica);
      }
    }

    return replacedHdfsReplicas;
  }

  /** 
   * {@link MiniSolrCloudCluster#waitForNode} Doesn't check isRunning first, and we don't want to 
   * use {@link MiniSolrCloudCluster#waitForAllNodes} because we don't want to waste cycles checking 
   * nodes we aren't messing with  
   */
  private void waitForNodeLive(final JettySolrRunner jetty)
    throws InterruptedException, TimeoutException, IOException {
    log.info("waitForNodeLive: {}/{}", jetty.getNodeName(), jetty.getLocalPort());
    
    TimeOut timeout = new TimeOut(30, TimeUnit.SECONDS, TimeSource.NANO_TIME);
    while(!timeout.hasTimedOut()) {
      if (jetty.isRunning()) {
        break;
      }
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        // ignore
      }
    }
    if (timeout.hasTimedOut()) {
      throw new TimeoutException("Waiting for Jetty to stop timed out");
    }
    cluster.waitForNode(jetty, 30);
  }
    
  private void waitForNodeLeave(String lostNodeName) throws InterruptedException, TimeoutException {
    log.info("waitForNodeLeave: {}", lostNodeName);
    ZkStateReader reader = cluster.getSolrClient().getZkStateReader();
    reader.waitForLiveNodes(30, TimeUnit.SECONDS, (o, n) -> !n.contains(lostNodeName));
  }

  
  private static CollectionStatePredicate clusterShapeNoDownReplicas(final int expectedShards,
                                                                     final int expectedReplicas) {
    return (liveNodes, collectionState)
      -> (clusterShape(expectedShards, expectedReplicas).matches(liveNodes, collectionState)
          && collectionState.getReplicas().size() == expectedReplicas);
  }
  
}
