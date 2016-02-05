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
import org.apache.zookeeper.KeeperException;
import org.junit.Test;

import static org.apache.solr.cloud.ReplicaPropertiesBase.verifyUniqueAcrossCollection;

@LuceneTestCase.Slow
public class CollectionsAPISolrJTests extends AbstractFullDistribZkTestBase {

  @Test
  public void test() throws Exception {
    testCreateAndDeleteCollection();
    testCreateAndDeleteShard();
    testReloadCollection();
    testCreateAndDeleteAlias();
    testSplitShard();
    testCreateCollectionWithPropertyParam();
    testAddAndDeleteReplica();
    testClusterProp();
    testAddAndRemoveRole();
    testOverseerStatus();
    testList();
    testAddAndDeleteReplicaProp();
    testBalanceShardUnique();
  }

  protected void testCreateAndDeleteCollection() throws Exception {
    String collectionName = "solrj_test";
    CollectionAdminRequest.Create createCollectionRequest = new CollectionAdminRequest.Create()
            .setCollectionName(collectionName)
            .setNumShards(2)
            .setReplicationFactor(2)
            .setConfigName("conf1")
            .setRouterField("myOwnField")
            .setStateFormat(1);
    CollectionAdminResponse response = createCollectionRequest.process(cloudClient);

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
    CollectionAdminRequest.Delete deleteCollectionRequest = new CollectionAdminRequest.Delete()
            .setCollectionName(collectionName);
    response = deleteCollectionRequest.process(cloudClient);

    assertEquals(0, response.getStatus());
    assertTrue(response.isSuccess());
    Map<String,NamedList<Integer>> nodesStatus = response.getCollectionNodesStatus();
    assertNull("Deleted collection " + collectionName + "still exists",
        cloudClient.getZkStateReader().getClusterState().getCollectionOrNull(collectionName));
    assertEquals(4, nodesStatus.size());
    
    // Test Creating a collection with new stateformat.
    collectionName = "solrj_newstateformat";
    createCollectionRequest = new CollectionAdminRequest.Create()
            .setCollectionName(collectionName)
            .setNumShards(2)
            .setConfigName("conf1")
            .setStateFormat(2);

    response = createCollectionRequest.process(cloudClient);
    assertEquals(0, response.getStatus());
    assertTrue(response.isSuccess());

    waitForRecoveriesToFinish(collectionName, false);
    assertTrue("Collection state does not exist",
        cloudClient.getZkStateReader().getZkClient()
            .exists(ZkStateReader.getCollectionPath(collectionName), true));

  }
  
  protected void testCreateAndDeleteShard() throws IOException, SolrServerException {
    // Create an implicit collection
    String collectionName = "solrj_implicit";
    CollectionAdminResponse response = new CollectionAdminRequest.Create()
            .setCollectionName(collectionName)
            .setShards("shardA,shardB")
            .setConfigName("conf1")
            .setRouterName("implicit").process(cloudClient);

    assertEquals(0, response.getStatus());
    assertTrue(response.isSuccess());
    Map<String, NamedList<Integer>> coresStatus = response.getCollectionCoresStatus();
    assertEquals(2, coresStatus.size());

    cloudClient.setDefaultCollection(collectionName);
    // Add a shard to the implicit collection
    response = new CollectionAdminRequest
        .CreateShard()
            .setCollectionName(collectionName)
            .setShardName("shardC").process(cloudClient);

    assertEquals(0, response.getStatus());
    assertTrue(response.isSuccess());
    coresStatus = response.getCollectionCoresStatus();
    assertEquals(1, coresStatus.size());
    assertEquals(0, (int) coresStatus.get(collectionName + "_shardC_replica1").get("status"));

    CollectionAdminRequest.DeleteShard deleteShardRequest = new CollectionAdminRequest
        .DeleteShard()
            .setCollectionName(collectionName)
            .setShardName("shardC");
    response = deleteShardRequest.process(cloudClient);

    assertEquals(0, response.getStatus());
    assertTrue(response.isSuccess());
    Map<String, NamedList<Integer>> nodesStatus = response.getCollectionNodesStatus();
    assertEquals(1, nodesStatus.size());
  }
  
  protected void testReloadCollection() throws IOException, SolrServerException {
    cloudClient.setDefaultCollection(DEFAULT_COLLECTION);
    CollectionAdminRequest.Reload reloadCollectionRequest = new CollectionAdminRequest.Reload()
            .setCollectionName("collection1");
    CollectionAdminResponse response = reloadCollectionRequest.process(cloudClient);

    assertEquals(0, response.getStatus());
  }
  
  protected void testCreateAndDeleteAlias() throws IOException, SolrServerException {
    CollectionAdminRequest.CreateAlias createAliasRequest = new CollectionAdminRequest
        .CreateAlias()
            .setAliasName("solrj_alias")
            .setAliasedCollections(DEFAULT_COLLECTION);
    CollectionAdminResponse response = createAliasRequest.process(cloudClient);

    assertEquals(0, response.getStatus());

    CollectionAdminRequest.DeleteAlias deleteAliasRequest = new CollectionAdminRequest.DeleteAlias()
            .setAliasName("solrj_alias");
    deleteAliasRequest.process(cloudClient);
    
    assertEquals(0, response.getStatus());
  }
  
  protected void testSplitShard() throws Exception {
    String collectionName = "solrj_test_splitshard";
    cloudClient.setDefaultCollection(collectionName);
    
    CollectionAdminRequest.Create createCollectionRequest = new CollectionAdminRequest.Create()
            .setConfigName("conf1")
            .setNumShards(2)
            .setCollectionName(collectionName);
    createCollectionRequest.process(cloudClient);
    
    CollectionAdminRequest.SplitShard splitShardRequest = new CollectionAdminRequest.SplitShard()
            .setCollectionName(collectionName)
            .setShardName("shard1");
    CollectionAdminResponse response = splitShardRequest.process(cloudClient);

    assertEquals(0, response.getStatus());
    assertTrue(response.isSuccess());
    Map<String, NamedList<Integer>> coresStatus = response.getCollectionCoresStatus();
    assertEquals(0, (int) coresStatus.get(collectionName + "_shard1_0_replica1").get("status"));
    assertEquals(0, (int) coresStatus.get(collectionName + "_shard1_1_replica1").get("status"));

    waitForRecoveriesToFinish(collectionName, false);
    waitForThingsToLevelOut(10);
    
    // Test splitting using split.key
    splitShardRequest = new CollectionAdminRequest.SplitShard()
            .setCollectionName(collectionName)
            .setSplitKey("b!");
    response = splitShardRequest.process(cloudClient);

    assertEquals(0, response.getStatus());
    assertTrue(response.isSuccess());

    waitForRecoveriesToFinish(collectionName, false);
    waitForThingsToLevelOut(10);
    
    ClusterState clusterState = cloudClient.getZkStateReader().getClusterState();
    Collection<Slice> slices = clusterState.getActiveSlices(collectionName);
    assertEquals("ClusterState: "+ clusterState.getActiveSlices(collectionName), 5, slices.size());  
    
  }

  private void testCreateCollectionWithPropertyParam() throws Exception {
    String collectionName = "solrj_test_core_props";
    
    File tmpDir = createTempDir("testPropertyParamsForCreate").toFile();
    File dataDir = new File(tmpDir, "dataDir-" + TestUtil.randomSimpleString(random(), 1, 5));
    File ulogDir = new File(tmpDir, "ulogDir-" + TestUtil.randomSimpleString(random(), 1, 5));

    Properties properties = new Properties();
    properties.put(CoreAdminParams.DATA_DIR, dataDir.getAbsolutePath());
    properties.put(CoreAdminParams.ULOG_DIR, ulogDir.getAbsolutePath());

    CollectionAdminRequest.Create createReq = new CollectionAdminRequest.Create()
            .setCollectionName(collectionName)
            .setNumShards(1)
            .setConfigName("conf1")
            .setProperties(properties);

    CollectionAdminResponse response = createReq.process(cloudClient);
    assertEquals(0, response.getStatus());
    assertTrue(response.isSuccess());
    Map<String, NamedList<Integer>> coresStatus = response.getCollectionCoresStatus();
    assertEquals(1, coresStatus.size());

    DocCollection testCollection = cloudClient.getZkStateReader()
        .getClusterState().getCollection(collectionName);

    Replica replica1 = testCollection.getReplica("core_node1");

    try (HttpSolrClient client = new HttpSolrClient(replica1.getStr("base_url"))) {
      CoreAdminResponse status = CoreAdminRequest.getStatus(replica1.getStr("core"), client);
      NamedList<Object> coreStatus = status.getCoreStatus(replica1.getStr("core"));
      String dataDirStr = (String) coreStatus.get("dataDir");
      assertEquals("Data dir does not match param given in property.dataDir syntax",
          new File(dataDirStr).getAbsolutePath(), dataDir.getAbsolutePath());
    }

    CollectionAdminRequest.Delete deleteCollectionRequest = new CollectionAdminRequest.Delete();
    deleteCollectionRequest.setCollectionName(collectionName);
    deleteCollectionRequest.process(cloudClient);
  }

  private void testAddAndDeleteReplica() throws Exception {
    String collectionName = "solrj_replicatests";
    createCollection(collectionName, cloudClient, 1, 2);

    cloudClient.setDefaultCollection(collectionName);

    String newReplicaName = Assign.assignNode(collectionName, cloudClient.getZkStateReader().getClusterState());
    ArrayList<String> nodeList = new ArrayList<>(cloudClient.getZkStateReader().getClusterState().getLiveNodes());
    Collections.shuffle(nodeList, random());
    CollectionAdminRequest.AddReplica addReplica = new CollectionAdminRequest.AddReplica()
            .setCollectionName(collectionName)
            .setShardName("shard1")
            .setNode(nodeList.get(0));
    CollectionAdminResponse response = addReplica.process(cloudClient);

    assertEquals(0, response.getStatus());
    assertTrue(response.isSuccess());

    TimeOut timeout = new TimeOut(3, TimeUnit.SECONDS);
    Replica newReplica = null;

    while (! timeout.hasTimedOut() && newReplica == null) {
      Slice slice = cloudClient.getZkStateReader().getClusterState().getSlice(collectionName, "shard1");
      newReplica = slice.getReplica(newReplicaName);
    }

    assertNotNull(newReplica);

    assertEquals("Replica should be created on the right node",
        cloudClient.getZkStateReader().getBaseUrlForNodeName(nodeList.get(0)),
        newReplica.getStr(ZkStateReader.BASE_URL_PROP)
    );
    
    // Test DELETEREPLICA
    CollectionAdminRequest.DeleteReplica deleteReplicaRequest = new CollectionAdminRequest.DeleteReplica()
            .setCollectionName(collectionName)
            .setShardName("shard1")
            .setReplica(newReplicaName);
    response = deleteReplicaRequest.process(cloudClient);

    assertEquals(0, response.getStatus());

    timeout = new TimeOut(3, TimeUnit.SECONDS);

    while (! timeout.hasTimedOut() && newReplica != null) {
      Slice slice = cloudClient.getZkStateReader().getClusterState().getSlice(collectionName, "shard1");
      newReplica = slice.getReplica(newReplicaName);
    }

    assertNull(newReplica);
  }

  private void testClusterProp() throws InterruptedException, IOException, SolrServerException {
    CollectionAdminRequest.ClusterProp clusterPropRequest = new CollectionAdminRequest.ClusterProp()
            .setPropertyName(ZkStateReader.LEGACY_CLOUD)
            .setPropertyValue("false");
    CollectionAdminResponse response = clusterPropRequest.process(cloudClient);

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
    clusterPropRequest = new CollectionAdminRequest.ClusterProp()
            .setPropertyName(ZkStateReader.LEGACY_CLOUD)
            .setPropertyValue(null);
    clusterPropRequest.process(cloudClient);

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

  private void testAddAndRemoveRole() throws InterruptedException, IOException, SolrServerException {
    cloudClient.setDefaultCollection(DEFAULT_COLLECTION);
    Replica replica = cloudClient.getZkStateReader().getLeaderRetry(DEFAULT_COLLECTION, SHARD1);
    CollectionAdminRequest.AddRole addRoleRequest = new CollectionAdminRequest.AddRole()
            .setNode(replica.getNodeName())
            .setRole("overseer");
    addRoleRequest.process(cloudClient);

    CollectionAdminRequest.ClusterStatus clusterStatusRequest = new CollectionAdminRequest.ClusterStatus()
            .setCollectionName(DEFAULT_COLLECTION);
    CollectionAdminResponse response = clusterStatusRequest.process(cloudClient);

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
    new CollectionAdminRequest.RemoveRole()
            .setNode(replica.getNodeName())
            .setRole("overseer")
            .process(cloudClient);

    clusterStatusRequest = new CollectionAdminRequest.ClusterStatus();
    clusterStatusRequest.setCollectionName(DEFAULT_COLLECTION);
    response = clusterStatusRequest.process(cloudClient);

    rsp = response.getResponse();
    cluster = (NamedList<Object>) rsp.get("cluster");
    assertNotNull("Cluster state should not be null", cluster);
    roles = (Map<String, Object>) cluster.get("roles");
    assertNotNull("Role information should not be null", roles);
    overseer = (List<String>) roles.get("overseer");
    assertFalse(overseer.contains(replica.getNodeName()));
  }
  
  private void testOverseerStatus() throws IOException, SolrServerException {
    CollectionAdminResponse response = new CollectionAdminRequest.OverseerStatus().process(cloudClient);
    assertEquals(0, response.getStatus());
    assertNotNull("overseer_operations shouldn't be null", response.getResponse().get("overseer_operations"));
  }
  
  private void testList() throws IOException, SolrServerException {
    CollectionAdminResponse response = new CollectionAdminRequest.List().process(cloudClient);
    assertEquals(0, response.getStatus());
    assertNotNull("collection list should not be null", response.getResponse().get("collections"));
  }
  
  private void testAddAndDeleteReplicaProp() throws InterruptedException, IOException, SolrServerException {
    Replica replica = cloudClient.getZkStateReader().getLeaderRetry(DEFAULT_COLLECTION, SHARD1);
    CollectionAdminResponse response = new CollectionAdminRequest.AddReplicaProp()
            .setCollectionName(DEFAULT_COLLECTION)
            .setShardName(SHARD1)
            .setReplica(replica.getName())
            .setPropertyName("preferredleader")
            .setPropertyValue("true").process(cloudClient);
    assertEquals(0, response.getStatus());

    TimeOut timeout = new TimeOut(20, TimeUnit.SECONDS);
    String propertyValue = null;
    
    String replicaName = replica.getName();
    while (! timeout.hasTimedOut()) {
      ClusterState clusterState = cloudClient.getZkStateReader().getClusterState();
      replica = clusterState.getReplica(DEFAULT_COLLECTION, replicaName);
      propertyValue = replica.getStr("property.preferredleader"); 
      if(StringUtils.equals("true", propertyValue))
        break;
      Thread.sleep(50);
    }
    
    assertEquals("Replica property was not updated, Latest value: " +
        cloudClient.getZkStateReader().getClusterState().getReplica(DEFAULT_COLLECTION, replicaName),
        "true",
        propertyValue);

    response = new CollectionAdminRequest.DeleteReplicaProp()
            .setCollectionName(DEFAULT_COLLECTION)
            .setShardName(SHARD1)
            .setReplica(replicaName)
            .setPropertyName("property.preferredleader").process(cloudClient);
    assertEquals(0, response.getStatus());

    timeout = new TimeOut(20, TimeUnit.SECONDS);
    boolean updated = false;

    while (! timeout.hasTimedOut()) {
      ClusterState clusterState = cloudClient.getZkStateReader().getClusterState();
      replica = clusterState.getReplica(DEFAULT_COLLECTION, replicaName);
      updated = replica.getStr("property.preferredleader") == null;
      if(updated)
        break;
      Thread.sleep(50);
    }

    assertTrue("Replica property was not removed", updated);
    
  }
  
  private void testBalanceShardUnique() throws IOException,
      SolrServerException, KeeperException, InterruptedException {
    CollectionAdminResponse response = new CollectionAdminRequest.BalanceShardUnique()
            .setCollection(DEFAULT_COLLECTION)
            .setPropertyName("preferredLeader").process(cloudClient);
    assertEquals(0, response.getStatus());

    verifyUniqueAcrossCollection(cloudClient, DEFAULT_COLLECTION, "property.preferredleader");    
  }
}
