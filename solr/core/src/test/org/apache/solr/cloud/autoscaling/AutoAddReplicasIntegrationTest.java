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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.cloud.ClusterStateUtil;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.util.LogLevel;
import org.apache.solr.util.TimeOut;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

@LogLevel("org.apache.solr.cloud.autoscaling=DEBUG;org.apache.solr.client.solrj.cloud.autoscaling=DEBUG;org.apache.solr.cloud=DEBUG;org.apache.solr.cloud.Overseer=DEBUG;org.apache.solr.cloud.overseer=DEBUG;")
public class AutoAddReplicasIntegrationTest extends SolrCloudTestCase {
  private static final String COLLECTION1 =  "testSimple1";
  private static final String COLLECTION2 =  "testSimple2";

  @Before
  public void setupCluster() throws Exception {
    configureCluster(3)
        .addConfig("conf", configset("cloud-minimal"))
        .withSolrXml(TEST_PATH().resolve("solr.xml"))
        .configure();

    new V2Request.Builder("/cluster")
        .withMethod(SolrRequest.METHOD.POST)
        .withPayload("{set-obj-property:{defaults : {cluster: {useLegacyReplicaAssignment:true}}}}}")
        .build()
        .process(cluster.getSolrClient());
  }
  
  @After
  public void tearDown() throws Exception {
    shutdownCluster();
    super.tearDown();
  }

  @Test
  public void testSimple() throws Exception {
    JettySolrRunner jetty1 = cluster.getJettySolrRunner(0);
    JettySolrRunner jetty2 = cluster.getJettySolrRunner(1);
    JettySolrRunner jetty3 = cluster.getJettySolrRunner(2);
    CollectionAdminRequest.createCollection(COLLECTION1, "conf", 2, 2)
        .setCreateNodeSet(jetty1.getNodeName()+","+jetty2.getNodeName())
        .setAutoAddReplicas(true)
        .setMaxShardsPerNode(2)
        .process(cluster.getSolrClient());
    
    cluster.waitForActiveCollection(COLLECTION1, 2, 4);
    
    CollectionAdminRequest.createCollection(COLLECTION2, "conf", 2, 2)
        .setCreateNodeSet(jetty2.getNodeName()+","+jetty3.getNodeName())
        .setAutoAddReplicas(false)
        .setMaxShardsPerNode(2)
        .process(cluster.getSolrClient());
    
    cluster.waitForActiveCollection(COLLECTION2, 2, 4);
    
    // the number of cores in jetty1 (5) will be larger than jetty3 (1)
    CollectionAdminRequest.createCollection("testSimple3", "conf", 3, 1)
        .setCreateNodeSet(jetty1.getNodeName())
        .setAutoAddReplicas(false)
        .setMaxShardsPerNode(3)
        .process(cluster.getSolrClient());

    cluster.waitForActiveCollection("testSimple3", 3, 3);
    
    ZkStateReader zkStateReader = cluster.getSolrClient().getZkStateReader();

    // start the tests
    JettySolrRunner lostJetty = random().nextBoolean() ? cluster.getJettySolrRunner(0) : cluster.getJettySolrRunner(1);
    String lostNodeName = lostJetty.getNodeName();
    List<Replica> replacedHdfsReplicas = getReplacedSharedFsReplicas(COLLECTION1, zkStateReader, lostNodeName);
    lostJetty.stop();
    
    cluster.waitForJettyToStop(lostJetty);
    
    waitForNodeLeave(lostNodeName);
    
    // ensure that 2 shards have 2 active replicas and only 4 replicas in total
    // i.e. old replicas have been deleted.
    // todo remove the condition for total replicas == 4 after SOLR-11591 is fixed
    waitForState("Waiting for collection " + COLLECTION1, COLLECTION1, (liveNodes, collectionState) -> clusterShape(2, 4).matches(liveNodes, collectionState)
        && collectionState.getReplicas().size() == 4, 90, TimeUnit.SECONDS);
    checkSharedFsReplicasMovedCorrectly(replacedHdfsReplicas, zkStateReader, COLLECTION1);
    lostJetty.start();
    
    cluster.waitForAllNodes(30);
    
    assertTrue("Timeout waiting for all live and active", ClusterStateUtil.waitForAllActiveAndLiveReplicas(cluster.getSolrClient().getZkStateReader(), 90000));

    // check cluster property is considered
    disableAutoAddReplicasInCluster();
    lostNodeName = jetty3.getNodeName();
    jetty3.stop();
    
    cluster.waitForJettyToStop(jetty3);
    
    waitForNodeLeave(lostNodeName);
    
    waitForState("Waiting for collection " + COLLECTION1, COLLECTION1, clusterShape(2, 2));
    jetty3.start();
    waitForState("Waiting for collection " + COLLECTION1, COLLECTION1, clusterShape(2, 4));
    waitForState("Waiting for collection " + COLLECTION2, COLLECTION2, clusterShape(2, 4));
    enableAutoAddReplicasInCluster();


    // test for multiple collections
    new CollectionAdminRequest.AsyncCollectionAdminRequest(CollectionParams.CollectionAction.MODIFYCOLLECTION) {
      @Override
      public SolrParams getParams() {
        ModifiableSolrParams params = (ModifiableSolrParams) super.getParams();
        params.set("collection", COLLECTION2);
        params.set("autoAddReplicas", true);
        return params;
      }
    }.process(cluster.getSolrClient());

    lostNodeName = jetty2.getNodeName();
    replacedHdfsReplicas = getReplacedSharedFsReplicas(COLLECTION2, zkStateReader, lostNodeName);
    
    jetty2.stop();
    
    cluster.waitForJettyToStop(jetty2);
    
    waitForNodeLeave(lostNodeName);
    waitForState("Waiting for collection " + COLLECTION1, COLLECTION1, clusterShape(2, 4), 45, TimeUnit.SECONDS);
    waitForState("Waiting for collection " + COLLECTION2, COLLECTION2, clusterShape(2, 4), 45, TimeUnit.SECONDS);
    checkSharedFsReplicasMovedCorrectly(replacedHdfsReplicas, zkStateReader, COLLECTION2);

    // overseer failover test..
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

  private void waitForNodeLeave(String lostNodeName) throws InterruptedException {
    ZkStateReader reader = cluster.getSolrClient().getZkStateReader();
    TimeOut timeOut = new TimeOut(20, TimeUnit.SECONDS, TimeSource.NANO_TIME);
    while (reader.getClusterState().getLiveNodes().contains(lostNodeName)) {
      Thread.sleep(100);
      if (timeOut.hasTimedOut()) fail("Wait for " + lostNodeName + " to leave failed!");
    }
  }
}
