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

import static org.apache.solr.common.cloud.ZkStateReader.MAX_SHARDS_PER_NODE;
import static org.apache.solr.cloud.OverseerCollectionProcessor.NUM_SLICES;
import static org.apache.solr.common.cloud.ZkStateReader.REPLICATION_FACTOR;
import static org.apache.solr.common.cloud.ZkNodeProps.makeMap;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.management.ObjectName;

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer.RemoteSolrException;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest.Create;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.CoreAdminResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams.CollectionAction;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrInfoMBean.Category;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.servlet.SolrDispatchFilter;
import org.apache.solr.update.DirectUpdateHandler2;
import org.apache.solr.util.DefaultSolrThreadFactory;
import org.junit.Before;
import org.junit.BeforeClass;

/**
 * Tests the Cloud Collections API.
 */
@Slow
public class CollectionsAPIDistributedZkTest extends AbstractFullDistribZkTestBase {
  
  private static final String DEFAULT_COLLECTION = "collection1";
  private static final boolean DEBUG = false;

  ThreadPoolExecutor executor = new ThreadPoolExecutor(0,
      Integer.MAX_VALUE, 5, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(),
      new DefaultSolrThreadFactory("testExecutor"));
  
  CompletionService<Object> completionService;
  Set<Future<Object>> pending;
  
  // we randomly use a second config set rather than just one
  private boolean secondConfigSet = random().nextBoolean();
  private boolean oldStyleSolrXml = false;
  
  @BeforeClass
  public static void beforeThisClass2() throws Exception {

  }
  
  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    
    useJettyDataDir = false;
    
    oldStyleSolrXml = random().nextBoolean();
    if (oldStyleSolrXml) {
      System.err.println("Using old style solr.xml");
    } else {
      System.err.println("Using new style solr.xml");
    }
    if (secondConfigSet ) {
      String zkHost = zkServer.getZkHost();
      String zkAddress = zkServer.getZkAddress();
      SolrZkClient zkClient = new SolrZkClient(zkHost, AbstractZkTestCase.TIMEOUT);
      zkClient.makePath("/solr", false, true);
      zkClient.close();

      zkClient = new SolrZkClient(zkAddress, AbstractZkTestCase.TIMEOUT);

      File solrhome = new File(TEST_HOME());
      
      // for now, always upload the config and schema to the canonical names
      AbstractZkTestCase.putConfig("conf2", zkClient, solrhome, getCloudSolrConfig(), "solrconfig.xml");
      AbstractZkTestCase.putConfig("conf2", zkClient, solrhome, "schema.xml", "schema.xml");

      AbstractZkTestCase.putConfig("conf2", zkClient, solrhome, "solrconfig.snippet.randomindexconfig.xml");
      AbstractZkTestCase.putConfig("conf2", zkClient, solrhome, "stopwords.txt");
      AbstractZkTestCase.putConfig("conf2", zkClient, solrhome, "protwords.txt");
      AbstractZkTestCase.putConfig("conf2", zkClient, solrhome, "currency.xml");
      AbstractZkTestCase.putConfig("conf2", zkClient, solrhome, "open-exchange-rates.json");
      AbstractZkTestCase.putConfig("conf2", zkClient, solrhome, "mapping-ISOLatin1Accent.txt");
      AbstractZkTestCase.putConfig("conf2", zkClient, solrhome, "old_synonyms.txt");
      AbstractZkTestCase.putConfig("conf2", zkClient, solrhome, "synonyms.txt");
      AbstractZkTestCase.putConfig("conf2", zkClient, solrhome, "elevate.xml");
      zkClient.close();
    }
    
    System.setProperty("numShards", Integer.toString(sliceCount));
    System.setProperty("solr.xml.persist", "true");
  }
  
  protected String getSolrXml() {
    // test old style and new style solr.xml
    return oldStyleSolrXml ? "solr-no-core-old-style.xml" : "solr-no-core.xml";
  }

  
  public CollectionsAPIDistributedZkTest() {
    fixShardCount = true;
    
    sliceCount = 2;
    shardCount = 4;
    completionService = new ExecutorCompletionService<>(executor);
    pending = new HashSet<>();
    checkCreatedVsState = false;
    
  }
  
  @Override
  protected void setDistributedParams(ModifiableSolrParams params) {

    if (r.nextBoolean()) {
      // don't set shards, let that be figured out from the cloud state
    } else {
      // use shard ids rather than physical locations
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < shardCount; i++) {
        if (i > 0)
          sb.append(',');
        sb.append("shard" + (i + 3));
      }
      params.set("shards", sb.toString());
    }
  }
  
  @Override
  public void doTest() throws Exception {
    testSolrJAPICalls();
    testNodesUsedByCreate();
    testCollectionsAPI();
    testCollectionsAPIAddRemoveStress();
    testErrorHandling();
    testNoCollectionSpecified();
    deletePartiallyCreatedCollection();
    deleteCollectionRemovesStaleZkCollectionsNode();
    clusterPropTest();
    addReplicaTest();
    // last
    deleteCollectionWithDownNodes();
    
    if (DEBUG) {
      super.printLayout();
    }
  }
  
  private void deleteCollectionRemovesStaleZkCollectionsNode() throws Exception {
    
    // we can use this client because we just want base url
    final String baseUrl = getBaseUrl((HttpSolrServer) clients.get(0));
    
    String collectionName = "out_of_sync_collection";
    
    List<Integer> numShardsNumReplicaList = new ArrayList<>();
    numShardsNumReplicaList.add(2);
    numShardsNumReplicaList.add(1);
    
    
    cloudClient.getZkStateReader().getZkClient().makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/" + collectionName, true);
    
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("action", CollectionAction.DELETE.toString());
    params.set("name", collectionName);
    QueryRequest request = new QueryRequest(params);
    request.setPath("/admin/collections");
    try {
      NamedList<Object> resp = createNewSolrServer("", baseUrl)
          .request(request);
      fail("Expected to fail, because collection is not in clusterstate");
    } catch (RemoteSolrException e) {
      
    }
    
    checkForMissingCollection(collectionName);
    
    assertFalse(cloudClient.getZkStateReader().getZkClient().exists(ZkStateReader.COLLECTIONS_ZKNODE + "/" + collectionName, true));

  }

  private void testSolrJAPICalls() throws Exception {
    SolrServer server = createNewSolrServer("", getBaseUrl((HttpSolrServer) clients.get(0)));
    CollectionAdminResponse response;
    Map<String, NamedList<Integer>> coresStatus;
    Map<String, NamedList<Integer>> nodesStatus;

    CollectionAdminRequest.Create createCollectionRequest = new CollectionAdminRequest.Create();
    createCollectionRequest.setCollectionName("solrj_collection");
    createCollectionRequest.setNumShards(2);
    createCollectionRequest.setReplicationFactor(2);
    createCollectionRequest.setConfigName("conf1");
    createCollectionRequest.setRouterField("myOwnField");
    response = createCollectionRequest.process(server);

    assertEquals(0, response.getStatus());
    assertTrue(response.isSuccess());
    coresStatus = response.getCollectionCoresStatus();
    assertEquals(4, coresStatus.size());
    for (int i=0; i<4; i++) {
      NamedList<Integer> status = coresStatus.get("solrj_collection_shard" + (i/2+1) + "_replica" + (i%2+1));
      assertEquals(0, (int)status.get("status"));
      assertTrue(status.get("QTime") > 0);
    }

    // TODO: This test tries to validates defaulting to implicit router.
    createCollectionRequest = new CollectionAdminRequest.Create();
    createCollectionRequest.setCollectionName("solrj_implicit");
    createCollectionRequest.setShards("shardA,shardB");
    createCollectionRequest.setConfigName("conf1");
    createCollectionRequest.setRouterName("implicit");
    response = createCollectionRequest.process(server);

    assertEquals(0, response.getStatus());
    assertTrue(response.isSuccess());
    coresStatus = response.getCollectionCoresStatus();
    assertEquals(2, coresStatus.size());

    CollectionAdminRequest.CreateShard createShardRequest = new CollectionAdminRequest
        .CreateShard();
    createShardRequest.setCollectionName("solrj_implicit");
    createShardRequest.setShardName("shardC");
    response = createShardRequest.process(server);

    assertEquals(0, response.getStatus());
    assertTrue(response.isSuccess());
    coresStatus = response.getCollectionCoresStatus();
    assertEquals(1, coresStatus.size());
    assertEquals(0, (int) coresStatus.get("solrj_implicit_shardC_replica1").get("status"));

    CollectionAdminRequest.DeleteShard deleteShardRequest = new CollectionAdminRequest
        .DeleteShard();
    deleteShardRequest.setCollectionName("solrj_implicit");
    deleteShardRequest.setShardName("shardC");
    response = deleteShardRequest.process(server);

    assertEquals(0, response.getStatus());
    assertTrue(response.isSuccess());
    nodesStatus = response.getCollectionNodesStatus();
    assertEquals(1, nodesStatus.size());

    CollectionAdminRequest.Delete deleteCollectionRequest = new CollectionAdminRequest.Delete();
    deleteCollectionRequest.setCollectionName("solrj_implicit");
    response = deleteCollectionRequest.process(server);

    assertEquals(0, response.getStatus());
    assertTrue(response.isSuccess());
    nodesStatus = response.getCollectionNodesStatus();
    assertEquals(2, nodesStatus.size());

    createCollectionRequest = new CollectionAdminRequest.Create();
    createCollectionRequest.setCollectionName("conf1");
    createCollectionRequest.setNumShards(4);
    createCollectionRequest.setConfigName("conf1");
    response = createCollectionRequest.process(server);

    assertEquals(0, response.getStatus());
    assertTrue(response.isSuccess());

    CollectionAdminRequest.Reload reloadCollectionRequest = new CollectionAdminRequest.Reload();
    reloadCollectionRequest.setCollectionName("conf1");
    response = reloadCollectionRequest.process(server);

    assertEquals(0, response.getStatus());

    CollectionAdminRequest.CreateAlias createAliasRequest = new CollectionAdminRequest
        .CreateAlias();
    createAliasRequest.setCollectionName("solrj_alias");
    createAliasRequest.setAliasedCollections("conf1,solrj_collection");
    response = createAliasRequest.process(server);

    assertEquals(0, response.getStatus());

    CollectionAdminRequest.DeleteAlias deleteAliasRequest = new CollectionAdminRequest.DeleteAlias();
    deleteAliasRequest.setCollectionName("solrj_alias");
    deleteAliasRequest.process(server);

    assertEquals(0, response.getStatus());

    CollectionAdminRequest.SplitShard splitShardRequest = new CollectionAdminRequest.SplitShard();
    splitShardRequest.setCollectionName("conf1");
    splitShardRequest.setShardName("shard1");
    response = splitShardRequest.process(server);

    assertEquals(0, response.getStatus());
    assertTrue(response.isSuccess());
    coresStatus = response.getCollectionCoresStatus();
    assertEquals(0, (int) coresStatus.get("conf1_shard1_0_replica1").get("status"));
    assertEquals(0, (int) coresStatus.get("conf1_shard1_0_replica1").get("status"));

    deleteCollectionRequest = new CollectionAdminRequest.Delete();
    deleteCollectionRequest.setCollectionName("conf1");
    response = deleteCollectionRequest.process(server);

    assertEquals(0, response.getStatus());
    nodesStatus = response.getCollectionNodesStatus();
    assertTrue(response.isSuccess());
    assertEquals(4, nodesStatus.size());

    deleteCollectionRequest = new CollectionAdminRequest.Delete();
    deleteCollectionRequest.setCollectionName("solrj_collection");
    deleteCollectionRequest.process(server);

    assertEquals(0, response.getStatus());
    nodesStatus = response.getCollectionNodesStatus();
    assertTrue(response.isSuccess());
    assertEquals(4, nodesStatus.size());
  }


  private void deletePartiallyCreatedCollection() throws Exception {
    final String baseUrl = getBaseUrl((HttpSolrServer) clients.get(0));
    String collectionName = "halfdeletedcollection";
    Create createCmd = new Create();
    createCmd.setCoreName("halfdeletedcollection_shard1_replica1");
    createCmd.setCollection(collectionName);
    String dataDir = createTempDir().toFile().getAbsolutePath();
    createCmd.setDataDir(dataDir);
    createCmd.setNumShards(2);
    if (secondConfigSet) {
      createCmd.setCollectionConfigName("conf1");
    }
    createNewSolrServer("", baseUrl).request(createCmd);

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("action", CollectionAction.DELETE.toString());
    params.set("name", collectionName);
    QueryRequest request = new QueryRequest(params);
    request.setPath("/admin/collections");

    NamedList<Object> resp = createNewSolrServer("", baseUrl).request(request);
    
    checkForMissingCollection(collectionName);
    
    // now creating that collection should work
    params = new ModifiableSolrParams();
    params.set("action", CollectionAction.CREATE.toString());
    params.set("name", collectionName);
    params.set("numShards", 2);
    request = new QueryRequest(params);
    request.setPath("/admin/collections");
    if (secondConfigSet) {
      params.set("collection.configName", "conf1");
    }
    resp = createNewSolrServer("", baseUrl).request(request);
  }
  
  
  private void deleteCollectionWithDownNodes() throws Exception {
    String baseUrl = getBaseUrl((HttpSolrServer) clients.get(0));
    // now try to remove a collection when a couple of it's nodes are down
    if (secondConfigSet) {
      createCollection(null, "halfdeletedcollection2", 3, 3, 6,
          createNewSolrServer("", baseUrl), null, "conf2");
    } else {
      createCollection(null, "halfdeletedcollection2", 3, 3, 6,
          createNewSolrServer("", baseUrl), null);
    }
    
    waitForRecoveriesToFinish("halfdeletedcollection2", false);
    
    // stop a couple nodes
    ChaosMonkey.stop(jettys.get(0));
    ChaosMonkey.stop(jettys.get(1));
    
    // wait for leaders to settle out
    for (int i = 1; i < 4; i++) {
      cloudClient.getZkStateReader().getLeaderRetry("halfdeletedcollection2", "shard" + i, 30000);
    }
    
    baseUrl = getBaseUrl((HttpSolrServer) clients.get(2));
    
    // remove a collection
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("action", CollectionAction.DELETE.toString());
    params.set("name", "halfdeletedcollection2");
    QueryRequest request = new QueryRequest(params);
    request.setPath("/admin/collections");
    
    createNewSolrServer("", baseUrl).request(request);
    
    long timeout = System.currentTimeMillis() + 10000;
    while (cloudClient.getZkStateReader().getClusterState().hasCollection("halfdeletedcollection2")) {
      if (System.currentTimeMillis() > timeout) {
        throw new AssertionError("Timeout waiting to see removed collection leave clusterstate");
      }
      
      Thread.sleep(200);
      cloudClient.getZkStateReader().updateClusterState(true);
    }

    assertFalse("Still found collection that should be gone", cloudClient.getZkStateReader().getClusterState().hasCollection("halfdeletedcollection2"));

  }

  private void testErrorHandling() throws Exception {
    final String baseUrl = getBaseUrl((HttpSolrServer) clients.get(0));
    
    
    // try a bad action
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("action", "BADACTION");
    String collectionName = "badactioncollection";
    params.set("name", collectionName);
    params.set("numShards", 2);
    QueryRequest request = new QueryRequest(params);
    request.setPath("/admin/collections");
    boolean gotExp = false;
    NamedList<Object> resp = null;
    try {
      resp = createNewSolrServer("", baseUrl).request(request);
    } catch (SolrException e) {
      gotExp = true;
    }
    assertTrue(gotExp);
    
    
    // leave out required param name
    params = new ModifiableSolrParams();
    params.set("action", CollectionAction.CREATE.toString());
    params.set("numShards", 2);
    collectionName = "collection";
    // No Name
    // params.set("name", collectionName);
    if (secondConfigSet) {
      params.set("collection.configName", "conf1");
    }
    request = new QueryRequest(params);
    request.setPath("/admin/collections");
    gotExp = false;
    resp = null;
    try {
      resp = createNewSolrServer("", baseUrl).request(request);
    } catch (SolrException e) {
      gotExp = true;
    }
    assertTrue(gotExp);
    
    // Too many replicas
    params = new ModifiableSolrParams();
    params.set("action", CollectionAction.CREATE.toString());
    collectionName = "collection";
    params.set("name", collectionName);
    params.set("numShards", 2);
    if (secondConfigSet) {
      params.set("collection.configName", "conf1");
    }
    params.set(REPLICATION_FACTOR, 10);
    request = new QueryRequest(params);
    request.setPath("/admin/collections");
    gotExp = false;
    try {
      resp = createNewSolrServer("", baseUrl).request(request);
    } catch (SolrException e) {
      gotExp = true;
    }
    assertTrue(gotExp);
    
    // No numShards should fail
    params = new ModifiableSolrParams();
    params.set("action", CollectionAction.CREATE.toString());
    collectionName = "acollection";
    params.set("name", collectionName);
    params.set(REPLICATION_FACTOR, 10);
    if (secondConfigSet) {
      params.set("collection.configName", "conf1");
    }
    request = new QueryRequest(params);
    request.setPath("/admin/collections");
    gotExp = false;
    resp = null;
    try {
      resp = createNewSolrServer("", baseUrl).request(request);
    } catch (SolrException e) {
      gotExp = true;
    }
    assertTrue(gotExp);
    
    // 0 numShards should fail
    params = new ModifiableSolrParams();
    params.set("action", CollectionAction.CREATE.toString());
    collectionName = "acollection";
    params.set("name", collectionName);
    params.set(REPLICATION_FACTOR, 10);
    params.set("numShards", 0);
    if (secondConfigSet) {
      params.set("collection.configName", "conf1");
    }
    request = new QueryRequest(params);
    request.setPath("/admin/collections");
    gotExp = false;
    resp = null;
    try {
      resp = createNewSolrServer("", baseUrl).request(request);
    } catch (SolrException e) {
      gotExp = true;
    }
    assertTrue(gotExp);
    
    // Fail on one node
    
    // first we make a core with the core name the collections api
    // will try and use - this will cause our mock fail
    Create createCmd = new Create();
    createCmd.setCoreName("halfcollection_shard1_replica1");
    createCmd.setCollection("halfcollectionblocker");
    String dataDir = createTempDir().toFile().getAbsolutePath();
    createCmd.setDataDir(dataDir);
    createCmd.setNumShards(1);
    if (secondConfigSet) {
      createCmd.setCollectionConfigName("conf1");
    }
    createNewSolrServer("", baseUrl).request(createCmd);
    
    createCmd = new Create();
    createCmd.setCoreName("halfcollection_shard1_replica1");
    createCmd.setCollection("halfcollectionblocker2");
    dataDir = createTempDir().toFile().getAbsolutePath();
    createCmd.setDataDir(dataDir);
    createCmd.setNumShards(1);
    if (secondConfigSet) {
      createCmd.setCollectionConfigName("conf1");
    }
    createNewSolrServer("", getBaseUrl((HttpSolrServer) clients.get(1))).request(createCmd);
    
    params = new ModifiableSolrParams();
    params.set("action", CollectionAction.CREATE.toString());
    collectionName = "halfcollection";
    params.set("name", collectionName);
    params.set("numShards", 2);
    params.set("wt", "xml");
    
    if (secondConfigSet) {
      params.set("collection.configName", "conf1");
    }
    
    String nn1 = ((SolrDispatchFilter) jettys.get(0).getDispatchFilter().getFilter()).getCores().getZkController().getNodeName();
    String nn2 =  ((SolrDispatchFilter) jettys.get(1).getDispatchFilter().getFilter()).getCores().getZkController().getNodeName();
    
    params.set(OverseerCollectionProcessor.CREATE_NODE_SET, nn1 + "," + nn2);
    request = new QueryRequest(params);
    request.setPath("/admin/collections");
    gotExp = false;
    resp = createNewSolrServer("", baseUrl).request(request);
    
    SimpleOrderedMap success = (SimpleOrderedMap) resp.get("success");
    SimpleOrderedMap failure = (SimpleOrderedMap) resp.get("failure");

    assertNotNull(resp.toString(), success);
    assertNotNull(resp.toString(), failure);
    
    String val1 = success.getVal(0).toString();
    String val2 = failure.getVal(0).toString();
    assertTrue(val1.contains("SolrException") || val2.contains("SolrException"));
  }
  
  private void testNoCollectionSpecified() throws Exception {
    
    cloudClient.getZkStateReader().updateClusterState(true);
    assertFalse(cloudClient.getZkStateReader().getClusterState().hasCollection("corewithnocollection"));
    assertFalse(cloudClient.getZkStateReader().getClusterState().hasCollection("corewithnocollection2"));
    
    // try and create a SolrCore with no collection name
    Create createCmd = new Create();
    createCmd.setCoreName("corewithnocollection");
    createCmd.setCollection("");
    String dataDir = createTempDir().toFile().getAbsolutePath();
    createCmd.setDataDir(dataDir);
    createCmd.setNumShards(1);
    if (secondConfigSet) {
      createCmd.setCollectionConfigName("conf1");
    }
    
    createNewSolrServer("", getBaseUrl((HttpSolrServer) clients.get(1)))
        .request(createCmd);
    
    // try and create a SolrCore with no collection name
    createCmd.setCollection(null);
    createCmd.setCoreName("corewithnocollection2");
    
    createNewSolrServer("", getBaseUrl((HttpSolrServer) clients.get(1)))
        .request(createCmd);
    
    // in both cases, the collection should have default to the core name
    cloudClient.getZkStateReader().updateClusterState(true);
    assertTrue( cloudClient.getZkStateReader().getClusterState().hasCollection("corewithnocollection"));
    assertTrue(cloudClient.getZkStateReader().getClusterState().hasCollection("corewithnocollection2"));
  }

  private void testNodesUsedByCreate() throws Exception {
    // we can use this client because we just want base url
    final String baseUrl = getBaseUrl((HttpSolrServer) clients.get(0));
    
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("action", CollectionAction.CREATE.toString());

    params.set("numShards", 2);
    params.set(REPLICATION_FACTOR, 2);
    String collectionName = "nodes_used_collection";

    params.set("name", collectionName);
    
    if (secondConfigSet) {
      params.set("collection.configName", "conf1");
    }
    
    QueryRequest request = new QueryRequest(params);
    request.setPath("/admin/collections");
    createNewSolrServer("", baseUrl).request(request);
    
    List<Integer> numShardsNumReplicaList = new ArrayList<>();
    numShardsNumReplicaList.add(2);
    numShardsNumReplicaList.add(2);
    checkForCollection("nodes_used_collection", numShardsNumReplicaList , null);

    List<String> createNodeList = new ArrayList<>();

    Set<String> liveNodes = cloudClient.getZkStateReader().getClusterState()
        .getLiveNodes();
    
    for (String node : liveNodes) {
      createNodeList.add(node);
    }

    DocCollection col = cloudClient.getZkStateReader().getClusterState().getCollection("nodes_used_collection");
    Collection<Slice> slices = col.getSlices();
    for (Slice slice : slices) {
      Collection<Replica> replicas = slice.getReplicas();
      for (Replica replica : replicas) {
        createNodeList.remove(replica.getNodeName());
      }
    }
    assertEquals(createNodeList.toString(), 1, createNodeList.size());

  }

  private void testCollectionsAPI() throws Exception {

    boolean disableLegacy = random().nextBoolean();
    CloudSolrServer client1 = null;

    if (disableLegacy) {
      log.info("legacyCloud=false");
      client1 = createCloudClient(null);
      setClusterProp(client1, ZkStateReader.LEGACY_CLOUD, "false");
    }

    // TODO: fragile - because we dont pass collection.confName, it will only
    // find a default if a conf set with a name matching the collection name is found, or 
    // if there is only one conf set. That and the fact that other tests run first in this
    // env make this pretty fragile
    
    // create new collections rapid fire
    Map<String,List<Integer>> collectionInfos = new HashMap<>();
    int cnt = random().nextInt(TEST_NIGHTLY ? 6 : 1) + 1;
    
    for (int i = 0; i < cnt; i++) {
      int numShards = TestUtil.nextInt(random(), 0, shardCount) + 1;
      int replicationFactor = TestUtil.nextInt(random(), 0, 3) + 1;
      int maxShardsPerNode = (((numShards * replicationFactor) / getCommonCloudSolrServer()
          .getZkStateReader().getClusterState().getLiveNodes().size())) + 1;

      
      CloudSolrServer client = null;
      try {
        if (i == 0) {
          // Test if we can create a collection through CloudSolrServer where
          // you havnt set default-collection
          // This is nice because you want to be able to create you first
          // collection using CloudSolrServer, and in such case there is
          // nothing reasonable to set as default-collection
          client = createCloudClient(null);
        } else if (i == 1) {
          // Test if we can create a collection through CloudSolrServer where
          // you have set default-collection to a non-existing collection
          // This is nice because you want to be able to create you first
          // collection using CloudSolrServer, and in such case there is
          // nothing reasonable to set as default-collection, but you might want
          // to use the same CloudSolrServer throughout the entire
          // lifetime of your client-application, so it is nice to be able to
          // set a default-collection on this CloudSolrServer once and for all
          // and use this CloudSolrServer to create the collection
          client = createCloudClient("awholynewcollection_" + i);
        }
        if (secondConfigSet) {
          createCollection(collectionInfos, "awholynewcollection_" + i,
              numShards, replicationFactor, maxShardsPerNode, client, null, "conf2");
        } else {
          createCollection(collectionInfos, "awholynewcollection_" + i,
              numShards, replicationFactor, maxShardsPerNode, client, null);
        }
      } finally {
        if (client != null) client.shutdown();
      }
    }
    
    Set<Entry<String,List<Integer>>> collectionInfosEntrySet = collectionInfos.entrySet();
    for (Entry<String,List<Integer>> entry : collectionInfosEntrySet) {
      String collection = entry.getKey();
      List<Integer> list = entry.getValue();
      checkForCollection(collection, list, null);
      
      String url = getUrlFromZk(collection);

      HttpSolrServer collectionClient = new HttpSolrServer(url);
      
      // poll for a second - it can take a moment before we are ready to serve
      waitForNon403or404or503(collectionClient);
      collectionClient.shutdown();
    }
    
    // sometimes we restart one of the jetty nodes
    if (random().nextBoolean()) {
      JettySolrRunner jetty = jettys.get(random().nextInt(jettys.size()));
      ChaosMonkey.stop(jetty);
      ChaosMonkey.start(jetty);
      
      for (Entry<String,List<Integer>> entry : collectionInfosEntrySet) {
        String collection = entry.getKey();
        List<Integer> list = entry.getValue();
        checkForCollection(collection, list, null);
        
        String url = getUrlFromZk(collection);
        
        HttpSolrServer collectionClient = new HttpSolrServer(url);
        
        // poll for a second - it can take a moment before we are ready to serve
        waitForNon403or404or503(collectionClient);
        collectionClient.shutdown();
      }
    }

    // sometimes we restart zookeeper
    if (random().nextBoolean()) {
      zkServer.shutdown();
      zkServer = new ZkTestServer(zkServer.getZkDir(), zkServer.getPort());
      zkServer.run();
    }
    
    // sometimes we cause a connection loss - sometimes it will hit the overseer
    if (random().nextBoolean()) {
      JettySolrRunner jetty = jettys.get(random().nextInt(jettys.size()));
      ChaosMonkey.causeConnectionLoss(jetty);
    }
    
    ZkStateReader zkStateReader = getCommonCloudSolrServer().getZkStateReader();
    for (int j = 0; j < cnt; j++) {
      waitForRecoveriesToFinish("awholynewcollection_" + j, zkStateReader, false);
      
      if (secondConfigSet) {
        // let's see if they are using the second config set
        byte[] data = zkStateReader.getZkClient()
            .getData(
                ZkStateReader.COLLECTIONS_ZKNODE + "/" + "awholynewcollection_"
                    + j, null, null, true);
        assertNotNull(data);
        ZkNodeProps props = ZkNodeProps.load(data);
        String configName = props.getStr(ZkController.CONFIGNAME_PROP);
        assertEquals("conf2", configName);
        
      }
    }
    
    checkInstanceDirs(jettys.get(0)); 
    
    List<String> collectionNameList = new ArrayList<>();
    collectionNameList.addAll(collectionInfos.keySet());
    String collectionName = collectionNameList.get(random().nextInt(collectionNameList.size()));
    
    String url = getUrlFromZk(collectionName);

    HttpSolrServer collectionClient = new HttpSolrServer(url);
    
    
    // lets try and use the solrj client to index a couple documents
    SolrInputDocument doc1 = getDoc(id, 6, i1, -600, tlong, 600, t1,
        "humpty dumpy sat on a wall");
    SolrInputDocument doc2 = getDoc(id, 7, i1, -600, tlong, 600, t1,
        "humpty dumpy3 sat on a walls");
    SolrInputDocument doc3 = getDoc(id, 8, i1, -600, tlong, 600, t1,
        "humpty dumpy2 sat on a walled");

    collectionClient.add(doc1);
    
    collectionClient.add(doc2);

    collectionClient.add(doc3);
    
    collectionClient.commit();
    
    assertEquals(3, collectionClient.query(new SolrQuery("*:*")).getResults().getNumFound());
    collectionClient.shutdown();
    collectionClient = null;
    
    // lets try a collection reload
    
    // get core open times
    Map<String,Long> urlToTimeBefore = new HashMap<>();
    collectStartTimes(collectionName, urlToTimeBefore);
    assertTrue(urlToTimeBefore.size() > 0);
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("action", CollectionAction.RELOAD.toString());
    params.set("name", collectionName);
    QueryRequest request = new QueryRequest(params);
    request.setPath("/admin/collections");
    
    // we can use this client because we just want base url
    final String baseUrl = getBaseUrl((HttpSolrServer) clients.get(0));
    
    createNewSolrServer("", baseUrl).request(request);

    // reloads make take a short while
    boolean allTimesAreCorrect = waitForReloads(collectionName, urlToTimeBefore);
    assertTrue("some core start times did not change on reload", allTimesAreCorrect);
    
    
    waitForRecoveriesToFinish("awholynewcollection_" + (cnt - 1), zkStateReader, false);
    
    // remove a collection
    params = new ModifiableSolrParams();
    params.set("action", CollectionAction.DELETE.toString());
    params.set("name", collectionName);
    request = new QueryRequest(params);
    request.setPath("/admin/collections");
 
    createNewSolrServer("", baseUrl).request(request);
    
    // ensure its out of the state
    checkForMissingCollection(collectionName);
    
    //collectionNameList.remove(collectionName);

    // remove an unknown collection
    params = new ModifiableSolrParams();
    params.set("action", CollectionAction.DELETE.toString());
    params.set("name", "unknown_collection");
    request = new QueryRequest(params);
    request.setPath("/admin/collections");
 
    boolean exp = false;
    try {
      createNewSolrServer("", baseUrl).request(request);
    } catch (SolrException e) {
      exp = true;
    }
    assertTrue("Expected exception", exp);
    
    // create another collection should still work
    params = new ModifiableSolrParams();
    params.set("action", CollectionAction.CREATE.toString());

    params.set("numShards", 1);
    params.set(REPLICATION_FACTOR, 2);
    collectionName = "acollectionafterbaddelete";

    params.set("name", collectionName);
    if (secondConfigSet) {
      params.set("collection.configName", "conf1");
    }
    request = new QueryRequest(params);
    request.setPath("/admin/collections");
    createNewSolrServer("", baseUrl).request(request);
    
    List<Integer> list = new ArrayList<>(2);
    list.add(1);
    list.add(2);
    checkForCollection(collectionName, list, null);
    
    url = getUrlFromZk(collectionName);
    
    collectionClient = new HttpSolrServer(url);
    
    // poll for a second - it can take a moment before we are ready to serve
    waitForNon403or404or503(collectionClient);
    collectionClient.shutdown();
    collectionClient = null;
    
    for (int j = 0; j < cnt; j++) {
      waitForRecoveriesToFinish(collectionName, zkStateReader, false);
    }

    // test maxShardsPerNode
    int numLiveNodes = getCommonCloudSolrServer().getZkStateReader().getClusterState().getLiveNodes().size();
    int numShards = (numLiveNodes/2) + 1;
    int replicationFactor = 2;
    int maxShardsPerNode = 1;
    collectionInfos = new HashMap<>();
    CloudSolrServer client = createCloudClient("awholynewcollection_" + cnt);
    try {
      exp = false;
      try {
        createCollection(collectionInfos, "awholynewcollection_" + cnt,
            numShards, replicationFactor, maxShardsPerNode, client, null, "conf1");
      } catch (SolrException e) {
        exp = true;
      }
      assertTrue("expected exception", exp);
    } finally {
      client.shutdown();
    }

    
    // Test createNodeSet
    numLiveNodes = getCommonCloudSolrServer().getZkStateReader().getClusterState().getLiveNodes().size();
    List<String> createNodeList = new ArrayList<>();
    int numOfCreateNodes = numLiveNodes/2;
    assertFalse("createNodeSet test is pointless with only " + numLiveNodes + " nodes running", numOfCreateNodes == 0);
    int i = 0;
    for (String liveNode : getCommonCloudSolrServer().getZkStateReader().getClusterState().getLiveNodes()) {
      if (i < numOfCreateNodes) {
        createNodeList.add(liveNode);
        i++;
      } else {
        break;
      }
    }
    maxShardsPerNode = 2;
    numShards = createNodeList.size() * maxShardsPerNode;
    replicationFactor = 1;
    collectionInfos = new HashMap<>();
    client = createCloudClient("awholynewcollection_" + (cnt+1));
    try {
      CollectionAdminResponse res = createCollection(collectionInfos, "awholynewcollection_" + (cnt+1), numShards, replicationFactor, maxShardsPerNode, client, StrUtils.join(createNodeList, ','), "conf1");
      assertTrue(res.isSuccess());
    } finally {
      client.shutdown();
    }
    checkForCollection(collectionInfos.keySet().iterator().next(), collectionInfos.entrySet().iterator().next().getValue(), createNodeList);
    
    checkNoTwoShardsUseTheSameIndexDir();
    if(disableLegacy) {
      setClusterProp(client1, ZkStateReader.LEGACY_CLOUD, null);
      client1.shutdown();
    }
  }
  
  private void testCollectionsAPIAddRemoveStress() throws Exception {
    
    class CollectionThread extends Thread {
      
      private String name;

      public CollectionThread(String name) {
        this.name = name;
      }
      
      public void run() {
        // create new collections rapid fire
        Map<String,List<Integer>> collectionInfos = new HashMap<>();
        int cnt = random().nextInt(TEST_NIGHTLY ? 13 : 1) + 1;
        
        for (int i = 0; i < cnt; i++) {
          String collectionName = "awholynewstresscollection_" + name + "_" + i;
          int numShards = TestUtil.nextInt(random(), 0, shardCount * 2) + 1;
          int replicationFactor = TestUtil.nextInt(random(), 0, 3) + 1;
          int maxShardsPerNode = (((numShards * 2 * replicationFactor) / getCommonCloudSolrServer()
              .getZkStateReader().getClusterState().getLiveNodes().size())) + 1;
          
          CloudSolrServer client = null;
          try {
            if (i == 0) {
              client = createCloudClient(null);
            } else if (i == 1) {
              client = createCloudClient(collectionName);
            }
            
            createCollection(collectionInfos, collectionName,
                numShards, replicationFactor, maxShardsPerNode, client, null,
                "conf1");
            
            // remove collection
            ModifiableSolrParams params = new ModifiableSolrParams();
            params.set("action", CollectionAction.DELETE.toString());
            params.set("name", collectionName);
            QueryRequest request = new QueryRequest(params);
            request.setPath("/admin/collections");
            
            if (client == null) {
              client = createCloudClient(null);
            }
            
            client.request(request);
            
          } catch (SolrServerException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
          } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
          } finally {
            if (client != null) client.shutdown();
          }
        }
      }
    }
    List<Thread> threads = new ArrayList<>();
    int numThreads = TEST_NIGHTLY ? 6 : 2;
    for (int i = 0; i < numThreads; i++) {
      CollectionThread thread = new CollectionThread("collection" + i);
      threads.add(thread);
    }
    
    for (Thread thread : threads) {
      thread.start();
    }
    for (Thread thread : threads) {
      thread.join();
    }
  }

  private void checkInstanceDirs(JettySolrRunner jetty) {
    CoreContainer cores = ((SolrDispatchFilter) jetty.getDispatchFilter()
        .getFilter()).getCores();
    Collection<SolrCore> theCores = cores.getCores();
    for (SolrCore core : theCores) {
      if (!oldStyleSolrXml) {
        // look for core props file
        assertTrue("Could not find expected core.properties file",
            new File((String) core.getStatistics().get("instanceDir"),
                "core.properties").exists());
      }
      
      assertEquals(
         new File(SolrResourceLoader.normalizeDir(jetty.getSolrHome() + File.separator
              + core.getName())).getAbsolutePath(),
          new File(SolrResourceLoader.normalizeDir((String) core.getStatistics().get(
              "instanceDir"))).getAbsolutePath());
    }
  }

  private boolean waitForReloads(String collectionName, Map<String,Long> urlToTimeBefore) throws SolrServerException, IOException {
    
    
    long timeoutAt = System.currentTimeMillis() + 45000;

    boolean allTimesAreCorrect = false;
    while (System.currentTimeMillis() < timeoutAt) {
      Map<String,Long> urlToTimeAfter = new HashMap<>();
      collectStartTimes(collectionName, urlToTimeAfter);
      
      boolean retry = false;
      Set<Entry<String,Long>> entries = urlToTimeBefore.entrySet();
      for (Entry<String,Long> entry : entries) {
        Long beforeTime = entry.getValue();
        Long afterTime = urlToTimeAfter.get(entry.getKey());
        assertNotNull(afterTime);
        if (afterTime <= beforeTime) {
          retry = true;
          break;
        }

      }
      if (!retry) {
        allTimesAreCorrect = true;
        break;
      }
    }
    return allTimesAreCorrect;
  }

  private void collectStartTimes(String collectionName,
      Map<String,Long> urlToTime) throws SolrServerException, IOException {
    ClusterState clusterState = getCommonCloudSolrServer().getZkStateReader()
        .getClusterState();
//    Map<String,DocCollection> collections = clusterState.getCollectionStates();
    if (clusterState.hasCollection(collectionName)) {
      Map<String,Slice> slices = clusterState.getSlicesMap(collectionName);

      Iterator<Entry<String,Slice>> it = slices.entrySet().iterator();
      while (it.hasNext()) {
        Entry<String,Slice> sliceEntry = it.next();
        Map<String,Replica> sliceShards = sliceEntry.getValue().getReplicasMap();
        Iterator<Entry<String,Replica>> shardIt = sliceShards.entrySet()
            .iterator();
        while (shardIt.hasNext()) {
          Entry<String,Replica> shardEntry = shardIt.next();
          ZkCoreNodeProps coreProps = new ZkCoreNodeProps(shardEntry.getValue());
          HttpSolrServer server = new HttpSolrServer(coreProps.getBaseUrl());
          CoreAdminResponse mcr;
          try {
            mcr = CoreAdminRequest.getStatus(coreProps.getCoreName(), server);
          } finally {
            server.shutdown();
          }
          long before = mcr.getStartTime(coreProps.getCoreName()).getTime();
          urlToTime.put(coreProps.getCoreUrl(), before);
        }
      }
    } else {
      throw new IllegalArgumentException("Could not find collection in :"
          + clusterState.getCollections());
    }
  }

  private String getUrlFromZk(String collection) {
    ClusterState clusterState = getCommonCloudSolrServer().getZkStateReader().getClusterState();
    Map<String,Slice> slices = clusterState.getSlicesMap(collection);
    
    if (slices == null) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Could not find collection:" + collection);
    }
    
    for (Map.Entry<String,Slice> entry : slices.entrySet()) {
      Slice slice = entry.getValue();
      Map<String,Replica> shards = slice.getReplicasMap();
      Set<Map.Entry<String,Replica>> shardEntries = shards.entrySet();
      for (Map.Entry<String,Replica> shardEntry : shardEntries) {
        final ZkNodeProps node = shardEntry.getValue();
        if (clusterState.liveNodesContain(node.getStr(ZkStateReader.NODE_NAME_PROP))) {
          return ZkCoreNodeProps.getCoreUrl(node.getStr(ZkStateReader.BASE_URL_PROP), collection); //new ZkCoreNodeProps(node).getCoreUrl();
        }
      }
    }
    
    throw new RuntimeException("Could not find a live node for collection:" + collection);
  }

/*  private void waitForNon403or404or503(HttpSolrServer collectionClient)
      throws Exception {
    SolrException exp = null;
    long timeoutAt = System.currentTimeMillis() + 30000;
    
    while (System.currentTimeMillis() < timeoutAt) {
      boolean missing = false;

      try {
        collectionClient.query(new SolrQuery("*:*"));
      } catch (SolrException e) {
        if (!(e.code() == 403 || e.code() == 503 || e.code() == 404)) {
          throw e;
        }
        exp = e;
        missing = true;
      }
      if (!missing) {
        return;
      }
      Thread.sleep(50);
    }

    fail("Could not find the new collection - " + exp.code() + " : " + collectionClient.getBaseURL());
  }*/
  
  private void checkNoTwoShardsUseTheSameIndexDir() throws Exception {
    Map<String, Set<String>> indexDirToShardNamesMap = new HashMap<>();
    
    List<MBeanServer> servers = new LinkedList<>();
    servers.add(ManagementFactory.getPlatformMBeanServer());
    servers.addAll(MBeanServerFactory.findMBeanServer(null));
    for (final MBeanServer server : servers) {
      Set<ObjectName> mbeans = new HashSet<>();
      mbeans.addAll(server.queryNames(null, null));
      for (final ObjectName mbean : mbeans) {
        Object value;
        Object indexDir;
        Object name;

        try {
          if (((value = server.getAttribute(mbean, "category")) != null && value
              .toString().equals(Category.CORE.toString()))
              && ((indexDir = server.getAttribute(mbean, "coreName")) != null)
              && ((indexDir = server.getAttribute(mbean, "indexDir")) != null)
              && ((name = server.getAttribute(mbean, "name")) != null)) {
            if (!indexDirToShardNamesMap.containsKey(indexDir.toString())) {
              indexDirToShardNamesMap.put(indexDir.toString(),
                  new HashSet<String>());
            }
            indexDirToShardNamesMap.get(indexDir.toString()).add(
                name.toString());
          }
        } catch (Exception e) {
          // ignore, just continue - probably a "category" or "source" attribute
          // not found
        }
      }
    }
    
    assertTrue(
        "Something is broken in the assert for no shards using the same indexDir - probably something was changed in the attributes published in the MBean of "
            + SolrCore.class.getSimpleName() + " : " + indexDirToShardNamesMap,
        indexDirToShardNamesMap.size() > 0);
    for (Entry<String,Set<String>> entry : indexDirToShardNamesMap.entrySet()) {
      if (entry.getValue().size() > 1) {
        fail("We have shards using the same indexDir. E.g. shards "
            + entry.getValue().toString() + " all use indexDir "
            + entry.getKey());
      }
    }

  }

  private void addReplicaTest() throws Exception {
    String collectionName = "addReplicaColl";
    CloudSolrServer client = createCloudClient(null);
    try {
      createCollection(collectionName, client, 2, 2);
      String newReplicaName = Assign.assignNode(collectionName, client.getZkStateReader().getClusterState());
      ArrayList<String> nodeList = new ArrayList<>(client.getZkStateReader().getClusterState().getLiveNodes());
      Collections.shuffle(nodeList, random());
      CollectionAdminRequest.AddReplica addReplica = new CollectionAdminRequest.AddReplica();
      addReplica.setCollectionName(collectionName);
      addReplica.setShardName("shard1");
      addReplica.setNode(nodeList.get(0));
      client.request(addReplica);

      long timeout = System.currentTimeMillis() + 3000;
      Replica newReplica = null;

      for (; System.currentTimeMillis() < timeout; ) {
        Slice slice = client.getZkStateReader().getClusterState().getSlice(collectionName, "shard1");
        newReplica = slice.getReplica(newReplicaName);
      }

      assertNotNull(newReplica);

      log.info("newReplica {},\n{} ", newReplica, client.getZkStateReader().getBaseUrlForNodeName(nodeList.get(0)));

      assertEquals("Replica should be created on the right node",
          client.getZkStateReader().getBaseUrlForNodeName(nodeList.get(0)), newReplica.getStr(ZkStateReader.BASE_URL_PROP));

      newReplicaName = Assign.assignNode(collectionName, client.getZkStateReader().getClusterState());
      addReplica = new CollectionAdminRequest.AddReplica();
      addReplica.setCollectionName(collectionName);
      addReplica.setShardName("shard2");
      client.request(addReplica);

      timeout = System.currentTimeMillis() + 3000;
      newReplica = null;

      for (; System.currentTimeMillis() < timeout; ) {
        Slice slice = client.getZkStateReader().getClusterState().getSlice(collectionName, "shard2");
        newReplica = slice.getReplica(newReplicaName);
      }

      assertNotNull(newReplica);


    } finally {
      client.shutdown();
    }

  }



  @Override
  protected QueryResponse queryServer(ModifiableSolrParams params) throws SolrServerException {

    if (r.nextBoolean())
      return super.queryServer(params);

    if (r.nextBoolean())
      params.set("collection",DEFAULT_COLLECTION);

    QueryResponse rsp = getCommonCloudSolrServer().query(params);
    return rsp;
  }

  protected void createCollection(String COLL_NAME, CloudSolrServer client,int replicationFactor , int numShards ) throws Exception {
    int maxShardsPerNode = ((((numShards+1) * replicationFactor) / getCommonCloudSolrServer()
        .getZkStateReader().getClusterState().getLiveNodes().size())) + 1;

    Map<String, Object> props = makeMap(
        REPLICATION_FACTOR, replicationFactor,
        MAX_SHARDS_PER_NODE, maxShardsPerNode,
        NUM_SLICES, numShards);
    Map<String,List<Integer>> collectionInfos = new HashMap<>();
    createCollection(collectionInfos, COLL_NAME, props, client,"conf1");
    waitForRecoveriesToFinish(COLL_NAME, false);
  }
  
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    System.clearProperty("numShards");
    System.clearProperty("zkHost");
    System.clearProperty("solr.xml.persist");
    
    // insurance
    DirectUpdateHandler2.commitOnClose = true;
  }

  private void clusterPropTest() throws Exception {
    CloudSolrServer client = createCloudClient(null);

    assertTrue("cluster property not set", setClusterProp(client, ZkStateReader.LEGACY_CLOUD, "false"));
    assertTrue("cluster property not unset ", setClusterProp(client, ZkStateReader.LEGACY_CLOUD, null));

    client.shutdown();
  }

  public static boolean setClusterProp(CloudSolrServer client, String name , String val) throws SolrServerException, IOException, InterruptedException {
    Map m = makeMap(
        "action", CollectionAction.CLUSTERPROP.toLower(),
        "name",name);

    if(val != null) m.put("val", val);
    SolrRequest request = new QueryRequest(new MapSolrParams(m));
    request.setPath("/admin/collections");
    client.request(request);

    long tomeOut = System.currentTimeMillis() + 3000;
    boolean changed = false;
    while(System.currentTimeMillis() <tomeOut){
      Thread.sleep(10);
      changed = Objects.equals(val,client.getZkStateReader().getClusterProps().get(name));
      if(changed) break;
    }
    return changed;
  }
}
