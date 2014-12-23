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

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
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

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;

@LuceneTestCase.Slow
public class CollectionsAPISolrJTests extends AbstractFullDistribZkTestBase {
  
  @Override
  public void doTest() throws Exception {
    testCreateAndDeleteCollection();
    testCreateAndDeleteShard();
    testReloadCollection();
    testCreateAndDeleteAlias();
    testSplitShard();
    testCreateCollectionWithPropertyParam();
  }

  public void tearDown() throws Exception {
    if (controlClient != null) {
      controlClient.shutdown();
    }
    if (cloudClient != null) {
      cloudClient.shutdown();
    }
    if (controlClientCloud != null) {
      controlClientCloud.shutdown();
    }
    super.tearDown();
  }


  protected void testCreateAndDeleteCollection() throws Exception {
    String collectionName = "solrj_test";
    CollectionAdminRequest.Create createCollectionRequest = new CollectionAdminRequest.Create();
    createCollectionRequest.setCollectionName(collectionName);
    createCollectionRequest.setNumShards(2);
    createCollectionRequest.setReplicationFactor(2);
    createCollectionRequest.setConfigName("conf1");
    createCollectionRequest.setRouterField("myOwnField");
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
    CollectionAdminRequest.Delete deleteCollectionRequest = new CollectionAdminRequest.Delete();
    deleteCollectionRequest.setCollectionName(collectionName);
    response = deleteCollectionRequest.process(cloudClient);

    assertEquals(0, response.getStatus());
    assertTrue(response.isSuccess());
    Map<String,NamedList<Integer>> nodesStatus = response.getCollectionNodesStatus();
    assertNull("Deleted collection " + collectionName + "still exists",
        cloudClient.getZkStateReader().getClusterState().getCollectionOrNull(collectionName));
    assertEquals(4, nodesStatus.size());
    
    // Test Creating a collection with new stateformat.
    collectionName = "solrj_newstateformat";
    createCollectionRequest = new CollectionAdminRequest.Create();
    createCollectionRequest.setCollectionName(collectionName);
    createCollectionRequest.setNumShards(2);
    createCollectionRequest.setConfigName("conf1");
    createCollectionRequest.setStateFormat(2);

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
    CollectionAdminRequest.Create createCollectionRequest = new CollectionAdminRequest.Create();
    createCollectionRequest.setCollectionName(collectionName);
    createCollectionRequest.setShards("shardA,shardB");
    createCollectionRequest.setConfigName("conf1");
    createCollectionRequest.setRouterName("implicit");
    CollectionAdminResponse response = createCollectionRequest.process(cloudClient);

    assertEquals(0, response.getStatus());
    assertTrue(response.isSuccess());
    Map<String, NamedList<Integer>> coresStatus = response.getCollectionCoresStatus();
    assertEquals(2, coresStatus.size());

    cloudClient.setDefaultCollection(collectionName);
    // Add a shard to the implicit collection
    CollectionAdminRequest.CreateShard createShardRequest = new CollectionAdminRequest
        .CreateShard();
    createShardRequest.setCollectionName(collectionName);
    createShardRequest.setShardName("shardC");
    response = createShardRequest.process(cloudClient);

    assertEquals(0, response.getStatus());
    assertTrue(response.isSuccess());
    coresStatus = response.getCollectionCoresStatus();
    assertEquals(1, coresStatus.size());
    assertEquals(0, (int) coresStatus.get(collectionName + "_shardC_replica1").get("status"));

    CollectionAdminRequest.DeleteShard deleteShardRequest = new CollectionAdminRequest
        .DeleteShard();
    deleteShardRequest.setCollectionName(collectionName);
    deleteShardRequest.setShardName("shardC");
    response = deleteShardRequest.process(cloudClient);

    assertEquals(0, response.getStatus());
    assertTrue(response.isSuccess());
    Map<String, NamedList<Integer>> nodesStatus = response.getCollectionNodesStatus();
    assertEquals(1, nodesStatus.size());
  }
  
  protected void testReloadCollection() throws IOException, SolrServerException {
    cloudClient.setDefaultCollection(DEFAULT_COLLECTION);
    CollectionAdminRequest.Reload reloadCollectionRequest = new CollectionAdminRequest.Reload();
    reloadCollectionRequest.setCollectionName("collection1");
    CollectionAdminResponse response = reloadCollectionRequest.process(cloudClient);

    assertEquals(0, response.getStatus());
  }
  
  protected void testCreateAndDeleteAlias() throws IOException, SolrServerException {
    CollectionAdminRequest.CreateAlias createAliasRequest = new CollectionAdminRequest
        .CreateAlias();
    createAliasRequest.setCollectionName("solrj_alias");
    createAliasRequest.setAliasedCollections("collection1");
    CollectionAdminResponse response = createAliasRequest.process(cloudClient);

    assertEquals(0, response.getStatus());

    CollectionAdminRequest.DeleteAlias deleteAliasRequest = new CollectionAdminRequest.DeleteAlias();
    deleteAliasRequest.setCollectionName("solrj_alias");
    deleteAliasRequest.process(cloudClient);
    
    assertEquals(0, response.getStatus());
  }
  
  protected void testSplitShard() throws Exception {
    String collectionName = "solrj_test_splitshard";
    cloudClient.setDefaultCollection(collectionName);
    
    CollectionAdminRequest.Create createCollectionRequest = new CollectionAdminRequest.Create();
    createCollectionRequest.setConfigName("conf1");
    createCollectionRequest.setNumShards(2);
    createCollectionRequest.setCollectionName(collectionName);
    createCollectionRequest.process(cloudClient);
    
    CollectionAdminRequest.SplitShard splitShardRequest = new CollectionAdminRequest.SplitShard();
    splitShardRequest.setCollectionName(collectionName);
    splitShardRequest.setShardName("shard1");
    CollectionAdminResponse response = splitShardRequest.process(cloudClient);

    assertEquals(0, response.getStatus());
    assertTrue(response.isSuccess());
    Map<String, NamedList<Integer>> coresStatus = response.getCollectionCoresStatus();
    assertEquals(0, (int) coresStatus.get(collectionName + "_shard1_0_replica1").get("status"));
    assertEquals(0, (int) coresStatus.get(collectionName + "_shard1_1_replica1").get("status"));

    waitForRecoveriesToFinish(collectionName, false);
    waitForThingsToLevelOut(10);
    
    // Test splitting using split.key
    splitShardRequest = new CollectionAdminRequest.SplitShard();
    splitShardRequest.setCollectionName(collectionName);
    splitShardRequest.setSplitKey("b!");
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
    File instanceDir = new File(tmpDir, "instanceDir-" + TestUtil.randomSimpleString(random(), 1, 5));
    File dataDir = new File(tmpDir, "dataDir-" + TestUtil.randomSimpleString(random(), 1, 5));
    File ulogDir = new File(tmpDir, "ulogDir-" + TestUtil.randomSimpleString(random(), 1, 5));

    Properties properties = new Properties();
    properties.put(CoreAdminParams.INSTANCE_DIR, instanceDir.getAbsolutePath());
    properties.put(CoreAdminParams.DATA_DIR, dataDir.getAbsolutePath());
    properties.put(CoreAdminParams.ULOG_DIR, ulogDir.getAbsolutePath());

    CollectionAdminRequest.Create createReq = new CollectionAdminRequest.Create();
    createReq.setCollectionName(collectionName);
    createReq.setNumShards(1);
    createReq.setConfigName("conf1");
    createReq.setProperties(properties);

    CollectionAdminResponse response = createReq.process(cloudClient);
    assertEquals(0, response.getStatus());
    assertTrue(response.isSuccess());
    Map<String, NamedList<Integer>> coresStatus = response.getCollectionCoresStatus();
    assertEquals(1, coresStatus.size());

    DocCollection testCollection = cloudClient.getZkStateReader()
        .getClusterState().getCollection(collectionName);

    Replica replica1 = testCollection.getReplica("core_node1");

    HttpSolrServer solrServer = new HttpSolrServer(replica1.getStr("base_url"));
    try {
      CoreAdminResponse status = CoreAdminRequest.getStatus(replica1.getStr("core"), solrServer);
      NamedList<Object> coreStatus = status.getCoreStatus(replica1.getStr("core"));
      String dataDirStr = (String) coreStatus.get("dataDir");
      String instanceDirStr = (String) coreStatus.get("instanceDir");
      assertEquals("Instance dir does not match param passed in property.instanceDir syntax",
          new File(instanceDirStr).getAbsolutePath(), instanceDir.getAbsolutePath());
      assertEquals("Data dir does not match param given in property.dataDir syntax",
          new File(dataDirStr).getAbsolutePath(), dataDir.getAbsolutePath());

    } finally {
      solrServer.shutdown();
    }

    CollectionAdminRequest.Delete deleteCollectionRequest = new CollectionAdminRequest.Delete();
    deleteCollectionRequest.setCollectionName(collectionName);
    deleteCollectionRequest.process(cloudClient);
  }
}
