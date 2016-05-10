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

import static org.apache.solr.cloud.ReplicaPropertiesBase.verifyUniqueAcrossCollection;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.commons.codec.binary.StringUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.CoreAdminResponse;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.util.TimeOut;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

@LuceneTestCase.Slow
public class CollectionsAPISolrJTest extends SolrCloudTestCase {

  private static final String DEFAULT_COLLECTION = "collection1";
  private static final String SHARD1 = "shard1";

  private static CloudSolrClient cloudClient;
  
  @BeforeClass
  public static void setup() throws Exception {
    configureCluster(4)
        .addConfig("conf1", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
        .configure();
    cloudClient = cluster.buildSolrClient();

    CollectionAdminResponse response = CollectionAdminRequest.createCollection(DEFAULT_COLLECTION, "conf1", 1, 1)
        .process(cloudClient);
    assertEquals(0, response.getStatus());
  }

  @AfterClass
  public static void deleteDefaultCollection() throws SolrServerException, IOException {
    CollectionAdminResponse response = CollectionAdminRequest.deleteCollection(DEFAULT_COLLECTION)
        .process(cloudClient);
    assertEquals(0, response.getStatus());

    cloudClient.close();
  }
  
  @Test
  public void testCreateAndDeleteCollection() throws Exception {
    String collectionName = "solrj_test";
    CollectionAdminResponse response = CollectionAdminRequest.createCollection(collectionName, "conf1", 2, 2)
        .setRouterField("myOwnField")
        .setStateFormat(1)
        .process(cloudClient);
    assertEquals(0, response.getStatus());
    assertTrue(response.isSuccess());
    Map<String, NamedList<Integer>> coresStatus = response.getCollectionCoresStatus();
    assertEquals(4, coresStatus.size());
    for (int i=0; i<4; i++) {
      NamedList<Integer> status = coresStatus.get(collectionName + "_shard" + (i/2+1) + "_replica" + (i%2+1));
      assertEquals(0, (int)status.get("status"));
      assertTrue(status.get("QTime") > 0);
    }

    cloudClient.setDefaultCollection(collectionName);
    CollectionAdminRequest.Delete delete = CollectionAdminRequest.deleteCollection(collectionName);
    response = delete.process(cloudClient);

    assertEquals(0, response.getStatus());
    assertTrue(response.isSuccess());
    Map<String,NamedList<Integer>> nodesStatus = response.getCollectionNodesStatus();
    assertNull("Deleted collection " + collectionName + "still exists",
        cloudClient.getZkStateReader().getClusterState().getCollectionOrNull(collectionName));
    assertEquals(4, nodesStatus.size());
    
    // Test Creating a collection with new stateformat.
    collectionName = "solrj_newstateformat";
    response = CollectionAdminRequest.createCollection(collectionName, "conf1", 2, 1)
        .setStateFormat(2)
        .process(cloudClient);
    assertEquals(0, response.getStatus());
    assertTrue(response.isSuccess());
    
    ZkStateReader zkStateReader = cloudClient.getZkStateReader();
    AbstractDistribZkTestBase.waitForRecoveriesToFinish(collectionName, zkStateReader, false, true, 330);

    assertTrue("Collection state does not exist", cloudClient.getZkStateReader().getZkClient()
        .exists(ZkStateReader.getCollectionPath(collectionName), true));
  }
  
  @Test
  public void testCreateAndDeleteShard() throws IOException, SolrServerException {
    // Create an implicit collection
    String collectionName = "solrj_implicit";
    CollectionAdminResponse response = CollectionAdminRequest.createCollection(collectionName, "conf1", 1, 1)
        .setShards("shardA,shardB")
        .setRouterName("implicit")
        .process(cloudClient);

    assertEquals(0, response.getStatus());
    assertTrue(response.isSuccess());
    Map<String, NamedList<Integer>> coresStatus = response.getCollectionCoresStatus();
    assertEquals(2, coresStatus.size());

    cloudClient.setDefaultCollection(collectionName);
    // Add a shard to the implicit collection
    response = CollectionAdminRequest.createShard(collectionName, "shardC")
        .process(cloudClient);

    assertEquals(0, response.getStatus());
    assertTrue(response.isSuccess());
    coresStatus = response.getCollectionCoresStatus();
    assertEquals(1, coresStatus.size());
    assertEquals(0, (int) coresStatus.get(collectionName + "_shardC_replica1").get("status"));

    response = CollectionAdminRequest.deleteShard(collectionName, "shardC")
        .process(cloudClient);

    assertEquals(0, response.getStatus());
    assertTrue(response.isSuccess());
    Map<String, NamedList<Integer>> nodesStatus = response.getCollectionNodesStatus();
    assertEquals(1, nodesStatus.size());
  }
  
  @Test
  public void testReloadCollection() throws IOException, SolrServerException {
    cloudClient.setDefaultCollection(DEFAULT_COLLECTION);
    CollectionAdminResponse response = CollectionAdminRequest.reloadCollection(DEFAULT_COLLECTION)
        .process(cloudClient);
    assertEquals(0, response.getStatus());
  }

  @Test
  public void testCreateAndDeleteAlias() throws IOException, SolrServerException {
    CollectionAdminResponse response = CollectionAdminRequest.createAlias("solrj_alias", DEFAULT_COLLECTION)
        .process(cloudClient);
    assertEquals(0, response.getStatus());

    response = CollectionAdminRequest.deleteAlias("solrj_alias")
        .process(cloudClient);
    assertEquals(0, response.getStatus());
  }
  
  @Test
  public void testSplitShard() throws Exception {
    String collectionName = "solrj_test_splitshard";
    cloudClient.setDefaultCollection(collectionName);
    
    CollectionAdminResponse response = CollectionAdminRequest.createCollection(collectionName, "conf1", 2, 1)
        .process(cloudClient);
    assertEquals(0, response.getStatus());
    
    response = CollectionAdminRequest.splitShard(collectionName)
        .setShardName("shard1")
        .process(cloudClient);
    assertEquals(0, response.getStatus());
    assertTrue(response.isSuccess());

    Map<String, NamedList<Integer>> coresStatus = response.getCollectionCoresStatus();
    assertEquals(0, (int) coresStatus.get(collectionName + "_shard1_0_replica1").get("status"));
    assertEquals(0, (int) coresStatus.get(collectionName + "_shard1_1_replica1").get("status"));

    waitForSlices(collectionName, 3, 10);
    
    // Test splitting using split.key
    response = CollectionAdminRequest.splitShard(collectionName)
        .setSplitKey("b!")
        .process(cloudClient);

    waitForSlices(collectionName, 5, 10);
  }
  
  private void waitForSlices(String collectionName, int count, int maxTries) throws InterruptedException {
    int tries = 0;
    do {
      ++tries;
      assertTrue("Max tries exceeded waiting for " + count + " slices", tries < maxTries);
      ClusterState clusterState = cloudClient.getZkStateReader().getClusterState();
      Collection<Slice> slices = clusterState.getCollection(collectionName).getActiveSlices();
      if (slices.size() == count) break;
      Thread.sleep(1000);
    } while (true);
  }

  @Test
  public void testCreateCollectionWithPropertyParam() throws Exception {
    String collectionName = "solrj_test_core_props";
    
    File tmpDir = createTempDir("testPropertyParamsForCreate").toFile();
    File dataDir = new File(tmpDir, "dataDir-" + TestUtil.randomSimpleString(random(), 1, 5));
    File ulogDir = new File(tmpDir, "ulogDir-" + TestUtil.randomSimpleString(random(), 1, 5));

    Properties properties = new Properties();
    properties.put(CoreAdminParams.DATA_DIR, dataDir.getAbsolutePath());
    properties.put(CoreAdminParams.ULOG_DIR, ulogDir.getAbsolutePath());

    CollectionAdminResponse response = CollectionAdminRequest.createCollection(collectionName, "conf1", 1, 1)
        .setProperties(properties)
        .process(cloudClient);
    assertEquals(0, response.getStatus());
    assertTrue(response.isSuccess());
    Map<String, NamedList<Integer>> coresStatus = response.getCollectionCoresStatus();
    assertEquals(1, coresStatus.size());

    DocCollection testCollection = cloudClient.getZkStateReader()
        .getClusterState().getCollection(collectionName);

    Replica replica1 = testCollection.getReplica("core_node1");

    try (HttpSolrClient client = getHttpSolrClient(replica1.getStr("base_url"))) {
      CoreAdminResponse status = CoreAdminRequest.getStatus(replica1.getStr("core"), client);
      NamedList<Object> coreStatus = status.getCoreStatus(replica1.getStr("core"));
      String dataDirStr = (String) coreStatus.get("dataDir");
      assertEquals("Data dir does not match param given in property.dataDir syntax",
          new File(dataDirStr).getAbsolutePath(), dataDir.getAbsolutePath());
    }

    response = CollectionAdminRequest.deleteCollection(collectionName)
        .process(cloudClient);
    assertEquals(0, response.getStatus());
  }

  @Test
  public void testAddAndDeleteReplica() throws Exception {
    String collectionName = "solrj_replicatests";
    int maxShardsPerNode = ((4 / cloudClient.getZkStateReader().getClusterState().getLiveNodes().size())) + 1;
    CollectionAdminResponse response = CollectionAdminRequest.createCollection(collectionName, "conf1", 1, 2)
        .setMaxShardsPerNode(maxShardsPerNode)
        .process(cloudClient);
    assertTrue(response.isSuccess());

    cloudClient.setDefaultCollection(collectionName);

    String newReplicaName = Assign.assignNode(cloudClient.getZkStateReader().getClusterState().getCollection(collectionName));
    ArrayList<String> nodeList = new ArrayList<>(cloudClient.getZkStateReader().getClusterState().getLiveNodes());
    Collections.shuffle(nodeList, random());
    response = CollectionAdminRequest.addReplicaToShard(collectionName, "shard1")
        .setNode(nodeList.get(0))
        .process(cloudClient);
    assertEquals(0, response.getStatus());
    assertTrue(response.isSuccess());

    TimeOut timeout = new TimeOut(3, TimeUnit.SECONDS);
    Replica newReplica = null;

    while (! timeout.hasTimedOut() && newReplica == null) {
      Slice slice = cloudClient.getZkStateReader().getClusterState().getCollection(collectionName).getSlice("shard1");
      newReplica = slice.getReplica(newReplicaName);
    }

    assertNotNull(newReplica);

    assertEquals("Replica should be created on the right node",
        cloudClient.getZkStateReader().getBaseUrlForNodeName(nodeList.get(0)),
        newReplica.getStr(ZkStateReader.BASE_URL_PROP)
    );
    
    // Test DELETEREPLICA
    response = CollectionAdminRequest.deleteReplica(collectionName, "shard1", newReplicaName)
        .process(cloudClient);
    assertEquals(0, response.getStatus());

    timeout = new TimeOut(3, TimeUnit.SECONDS);

    while (! timeout.hasTimedOut() && newReplica != null) {
      Slice slice = cloudClient.getZkStateReader().getClusterState().getCollection(collectionName).getSlice("shard1");
      newReplica = slice.getReplica(newReplicaName);
    }

    assertNull(newReplica);
  }

  @Test
  public void testClusterProp() throws InterruptedException, IOException, SolrServerException {
    CollectionAdminResponse response = CollectionAdminRequest.setClusterProperty(ZkStateReader.LEGACY_CLOUD, "false")
        .process(cloudClient);
    assertEquals(0, response.getStatus());

    TimeOut timeout = new TimeOut(3, TimeUnit.SECONDS);
    boolean changed = false;
    
    while(! timeout.hasTimedOut()){
      Thread.sleep(10);
      changed = Objects.equals("false",
          cloudClient.getZkStateReader().getClusterProps().get(ZkStateReader.LEGACY_CLOUD));
      if(changed) break;
    }
    assertTrue("The Cluster property wasn't set", changed);
    
    // Unset ClusterProp that we set.
    response = CollectionAdminRequest.setClusterProperty(ZkStateReader.LEGACY_CLOUD, null)
        .process(cloudClient);
    assertEquals(0, response.getStatus());

    timeout = new TimeOut(3, TimeUnit.SECONDS);
    changed = false;
    while(! timeout.hasTimedOut()) {
      Thread.sleep(10);
      changed = (cloudClient.getZkStateReader().getClusterProps().get(ZkStateReader.LEGACY_CLOUD) == null);
      if(changed)  
        break;
    }
    assertTrue("The Cluster property wasn't unset", changed);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testAddAndRemoveRole() throws Exception {
    cloudClient.setDefaultCollection(DEFAULT_COLLECTION);
    Replica replica = cloudClient.getZkStateReader().getLeaderRetry(DEFAULT_COLLECTION, SHARD1);
    CollectionAdminResponse response = CollectionAdminRequest.addRole(replica.getNodeName(), "overseer")
        .process(cloudClient);
    assertEquals(0, response.getStatus());

    response = new CollectionAdminRequest.ClusterStatus()
            .setCollectionName(DEFAULT_COLLECTION)
            .process(cloudClient);

    NamedList<Object> rsp = response.getResponse();
    NamedList<Object> cluster = (NamedList<Object>) rsp.get("cluster");
    assertNotNull("Cluster state should not be null", cluster);
    Map<String, Object> roles = (Map<String, Object>) cluster.get("roles");
    assertNotNull("Role information should not be null", roles);
    List<String> overseer = (List<String>) roles.get("overseer");
    assertNotNull(overseer);
    assertEquals(1, overseer.size());
    assertTrue(overseer.contains(replica.getNodeName()));
    
    // Remove role
    CollectionAdminRequest.removeRole(replica.getNodeName(), "overseer").process(cloudClient);

    response = new CollectionAdminRequest.ClusterStatus()
        .setCollectionName(DEFAULT_COLLECTION)
        .process(cloudClient);

    rsp = response.getResponse();
    cluster = (NamedList<Object>) rsp.get("cluster");
    assertNotNull("Cluster state should not be null", cluster);
    roles = (Map<String, Object>) cluster.get("roles");
    assertNotNull("Role information should not be null", roles);
    overseer = (List<String>) roles.get("overseer");
    assertFalse(overseer.contains(replica.getNodeName()));
  }
  
  @Test
  public void testOverseerStatus() throws IOException, SolrServerException {
    CollectionAdminResponse response = new CollectionAdminRequest.OverseerStatus().process(cloudClient);
    assertEquals(0, response.getStatus());
    assertNotNull("overseer_operations shouldn't be null", response.getResponse().get("overseer_operations"));
  }
  
  @Test
  public void testList() throws IOException, SolrServerException {
    CollectionAdminResponse response = new CollectionAdminRequest.List().process(cloudClient);
    assertEquals(0, response.getStatus());
    assertNotNull("collection list should not be null", response.getResponse().get("collections"));
  }
  
  @Test
  public void testAddAndDeleteReplicaProp() throws InterruptedException, IOException, SolrServerException {
    Replica replica = cloudClient.getZkStateReader().getLeaderRetry(DEFAULT_COLLECTION, SHARD1);
    CollectionAdminResponse response = CollectionAdminRequest.addReplicaProperty(DEFAULT_COLLECTION,
                                                                                 SHARD1,
                                                                                 replica.getName(),
                                                                                 "preferredleader",
                                                                                 "true")
        .process(cloudClient);
    assertEquals(0, response.getStatus());

    TimeOut timeout = new TimeOut(20, TimeUnit.SECONDS);
    String propertyValue = null;
    
    String replicaName = replica.getName();
    while (! timeout.hasTimedOut()) {
      ClusterState clusterState = cloudClient.getZkStateReader().getClusterState();
      replica = clusterState.getCollection(DEFAULT_COLLECTION).getReplica(replicaName);
      propertyValue = replica.getStr("property.preferredleader"); 
      if(StringUtils.equals("true", propertyValue))
        break;
      Thread.sleep(50);
    }
    
    assertEquals("Replica property was not updated, Latest value: " +
        cloudClient.getZkStateReader().getClusterState().getCollection(DEFAULT_COLLECTION).getReplica(replicaName),
        "true",
        propertyValue);

    response = CollectionAdminRequest.deleteReplicaProperty(DEFAULT_COLLECTION,
                                                            SHARD1,
                                                            replicaName,
                                                            "property.preferredleader")
        .process(cloudClient);
    assertEquals(0, response.getStatus());

    timeout = new TimeOut(20, TimeUnit.SECONDS);
    boolean updated = false;

    while (! timeout.hasTimedOut()) {
      ClusterState clusterState = cloudClient.getZkStateReader().getClusterState();
      replica = clusterState.getCollection(DEFAULT_COLLECTION).getReplica(replicaName);
      updated = replica.getStr("property.preferredleader") == null;
      if(updated)
        break;
      Thread.sleep(50);
    }

    assertTrue("Replica property was not removed", updated); 
  }
  
  @Test
  public void testBalanceShardUnique() throws Exception {
    CollectionAdminResponse response = CollectionAdminRequest.balanceReplicaProperty(DEFAULT_COLLECTION, "preferredLeader")
        .process(cloudClient);
    assertEquals(0, response.getStatus());

    verifyUniqueAcrossCollection(cloudClient, DEFAULT_COLLECTION, "property.preferredleader");    
  }

}
