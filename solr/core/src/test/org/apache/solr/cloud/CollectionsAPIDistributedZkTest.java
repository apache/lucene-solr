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

import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.management.ObjectName;
import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
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
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrInfoMBean.Category;
import org.apache.solr.util.TestInjection;
import org.apache.solr.util.TimeOut;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.cloud.OverseerCollectionMessageHandler.NUM_SLICES;
import static org.apache.solr.common.cloud.ZkStateReader.CORE_NAME_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.MAX_SHARDS_PER_NODE;
import static org.apache.solr.common.cloud.ZkStateReader.REPLICATION_FACTOR;
import static org.apache.solr.common.util.Utils.makeMap;

/**
 * Tests the Cloud Collections API.
 */
@Slow
public class CollectionsAPIDistributedZkTest extends AbstractFullDistribZkTestBase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String DEFAULT_COLLECTION = "collection1";

  // we randomly use a second config set rather than just one
  private boolean secondConfigSet = random().nextBoolean();
  
  @BeforeClass
  public static void beforeCollectionsAPIDistributedZkTest() {
    TestInjection.randomDelayInCoreCreation = "true:20";
    System.setProperty("validateAfterInactivity", "200");
  }
  
  @Override
  public void distribSetUp() throws Exception {
    super.distribSetUp();
    
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
      AbstractZkTestCase.putConfig("conf2", zkClient, solrhome, "enumsConfig.xml", "enumsConfig.xml");
      
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
  }
  
  protected String getSolrXml() {
    return "solr-no-core.xml";
  }

  
  public CollectionsAPIDistributedZkTest() {
    sliceCount = 2;
  }
  
  @Override
  protected void setDistributedParams(ModifiableSolrParams params) {

    if (r.nextBoolean()) {
      // don't set shards, let that be figured out from the cloud state
    } else {
      // use shard ids rather than physical locations
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < getShardCount(); i++) {
        if (i > 0)
          sb.append(',');
        sb.append("shard" + (i + 3));
      }
      params.set("shards", sb.toString());
    }
  }

  @Test
  @ShardsFixed(num = 4)
  public void test() throws Exception {
    waitForRecoveriesToFinish(false); // we need to fix no core tests still
    testNodesUsedByCreate();
    testNoConfigSetExist();
    testCollectionsAPI();
    testCollectionsAPIAddRemoveStress();
    testErrorHandling();
    testNoCollectionSpecified();
    deletePartiallyCreatedCollection();
    deleteCollectionRemovesStaleZkCollectionsNode();
    clusterPropTest();
    // last
    deleteCollectionWithDownNodes();
    addReplicaTest();
  }

  private void deleteCollectionRemovesStaleZkCollectionsNode() throws Exception {
    
    // we can use this client because we just want base url
    final String baseUrl = getBaseUrl((HttpSolrClient) clients.get(0));
    
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
    
    // there are remnants of the collection in zk, should work
    makeRequest(baseUrl, request);
    
    assertCollectionNotExists(collectionName, 45);
    
    assertFalse(cloudClient.getZkStateReader().getZkClient().exists(ZkStateReader.COLLECTIONS_ZKNODE + "/" + collectionName, true));

  }

  private void deletePartiallyCreatedCollection() throws Exception {
    final String baseUrl = getBaseUrl((HttpSolrClient) clients.get(0));
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

    makeRequest(baseUrl, createCmd);

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("action", CollectionAction.DELETE.toString());
    params.set("name", collectionName);
    QueryRequest request = new QueryRequest(params);
    request.setPath("/admin/collections");

    makeRequest(baseUrl, request);

    assertCollectionNotExists(collectionName, 45);
    
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
    makeRequest(baseUrl, request);
  }
  
  private void deleteCollectionOnlyInZk() throws Exception {
    final String baseUrl = getBaseUrl((HttpSolrClient) clients.get(0));
    String collectionName = "onlyinzk";

    cloudClient.getZkStateReader().getZkClient().makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/" + collectionName, true);

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("action", CollectionAction.DELETE.toString());
    params.set("name", collectionName);
    QueryRequest request = new QueryRequest(params);
    request.setPath("/admin/collections");

    makeRequest(baseUrl, request);

    assertCollectionNotExists(collectionName, 45);
    
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
    makeRequest(baseUrl, request);
    
    waitForRecoveriesToFinish(collectionName, false);
    
    params = new ModifiableSolrParams();
    params.set("action", CollectionAction.DELETE.toString());
    params.set("name", collectionName);
    request = new QueryRequest(params);
    request.setPath("/admin/collections");

    makeRequest(baseUrl, request);
  }
  
  private void deleteCollectionWithUnloadedCore() throws Exception {
    final String baseUrl = getBaseUrl((HttpSolrClient) clients.get(0));
    
    String collectionName = "corealreadyunloaded";
    try (SolrClient client = createNewSolrClient("", baseUrl)) {
      createCollection(null, collectionName,  2, 1, 2, client, null, "conf1");
    }
    waitForRecoveriesToFinish(collectionName, false);

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("action", CollectionAction.DELETE.toString());
    params.set("name", collectionName);
    QueryRequest request = new QueryRequest(params);
    request.setPath("/admin/collections");

    NamedList<Object> result = makeRequest(baseUrl, request);
    System.out.println("result:" + result);
    Object failure = result.get("failure");
    assertNull("We expect no failures", failure);

    assertCollectionNotExists(collectionName, 45);
    
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
    makeRequest(baseUrl, request);
    
    params = new ModifiableSolrParams();
    params.set("action", CollectionAction.DELETE.toString());
    params.set("name", collectionName);
    request = new QueryRequest(params);
    request.setPath("/admin/collections");

    makeRequest(baseUrl, request);
  }
  
  
  private void deleteCollectionWithDownNodes() throws Exception {
    String baseUrl = getBaseUrl((HttpSolrClient) clients.get(0));
    // now try to remove a collection when a couple of its nodes are down
    if (secondConfigSet) {
      try (SolrClient client = createNewSolrClient("", baseUrl)) {
        createCollection(null, "halfdeletedcollection2", 3, 3, 6, client, null, "conf2");
      }
    } else {
      try (SolrClient client = createNewSolrClient("", baseUrl)) {
        createCollection(null, "halfdeletedcollection2", 3, 3, 6, client, null);
      }
    }
    
    waitForRecoveriesToFinish("halfdeletedcollection2", false);
    
    // stop a couple nodes
    ChaosMonkey.stop(jettys.get(0));
    ChaosMonkey.stop(jettys.get(1));
    
    // wait for leaders to settle out
    for (int i = 1; i < 4; i++) {
      cloudClient.getZkStateReader().getLeaderRetry("halfdeletedcollection2", "shard" + i, 30000);
    }
    
    baseUrl = getBaseUrl((HttpSolrClient) clients.get(2));
    
    // remove a collection
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("action", CollectionAction.DELETE.toString());
    params.set("name", "halfdeletedcollection2");
    QueryRequest request = new QueryRequest(params);
    request.setPath("/admin/collections");
    
    makeRequest(baseUrl, request);

    TimeOut timeout = new TimeOut(10, TimeUnit.SECONDS);
    while (cloudClient.getZkStateReader().getClusterState().hasCollection("halfdeletedcollection2")) {
      if (timeout.hasTimedOut()) {
        throw new AssertionError("Timeout waiting to see removed collection leave clusterstate");
      }
      
      Thread.sleep(200);
    }

    assertFalse("Still found collection that should be gone", cloudClient.getZkStateReader().getClusterState().hasCollection("halfdeletedcollection2"));

  }

  private NamedList<Object> makeRequest(String baseUrl, SolrRequest request, int socketTimeout)
      throws SolrServerException, IOException {
    try (SolrClient client = createNewSolrClient("", baseUrl)) {
      ((HttpSolrClient) client).setSoTimeout(socketTimeout);
      return client.request(request);
    }
  }

  private NamedList<Object> makeRequest(String baseUrl, SolrRequest request)
      throws SolrServerException, IOException {
    try (SolrClient client = createNewSolrClient("", baseUrl)) {
      ((HttpSolrClient) client).setSoTimeout(30000);
      return client.request(request);
    }
  }

  private void testErrorHandling() throws Exception {
    final String baseUrl = getBaseUrl((HttpSolrClient) clients.get(0));
    
    // try a bad action
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("action", "BADACTION");
    String collectionName = "badactioncollection";
    params.set("name", collectionName);
    params.set("numShards", 2);
    QueryRequest request = new QueryRequest(params);
    request.setPath("/admin/collections");
    boolean gotExp = false;
    try {
      makeRequest(baseUrl, request);
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
    try {
      makeRequest(baseUrl, request);
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
      makeRequest(baseUrl, request);
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
    try {
      makeRequest(baseUrl, request);
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
    try {
      makeRequest(baseUrl, request);
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
    makeRequest(baseUrl, createCmd);
    
    createCmd = new Create();
    createCmd.setCoreName("halfcollection_shard1_replica1");
    createCmd.setCollection("halfcollectionblocker2");
    dataDir = createTempDir().toFile().getAbsolutePath();
    createCmd.setDataDir(dataDir);
    createCmd.setNumShards(1);
    if (secondConfigSet) {
      createCmd.setCollectionConfigName("conf1");
    }
    makeRequest(getBaseUrl((HttpSolrClient) clients.get(1)), createCmd);
    
    params = new ModifiableSolrParams();
    params.set("action", CollectionAction.CREATE.toString());
    collectionName = "halfcollection";
    params.set("name", collectionName);
    params.set("numShards", 2);
    params.set("wt", "xml");
    
    if (secondConfigSet) {
      params.set("collection.configName", "conf1");
    }
    
    String nn1 = jettys.get(0).getCoreContainer().getZkController().getNodeName();
    String nn2 =  jettys.get(1).getCoreContainer().getZkController().getNodeName();
    
    params.set(OverseerCollectionMessageHandler.CREATE_NODE_SET, nn1 + "," + nn2);
    request = new QueryRequest(params);
    request.setPath("/admin/collections");
    NamedList<Object> resp = makeRequest(baseUrl, request, 60000);
    
    SimpleOrderedMap success = (SimpleOrderedMap) resp.get("success");
    SimpleOrderedMap failure = (SimpleOrderedMap) resp.get("failure");

    assertNotNull(resp.toString(), success);
    assertNotNull(resp.toString(), failure);
    
    String val1 = success.getVal(0).toString();
    String val2 = failure.getVal(0).toString();
    assertTrue(val1.contains("SolrException") || val2.contains("SolrException"));
  }
  
  private void testNoCollectionSpecified() throws Exception {
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

    makeRequest(getBaseUrl((HttpSolrClient) clients.get(1)), createCmd);
    
    // try and create a SolrCore with no collection name
    createCmd.setCollection(null);
    createCmd.setCoreName("corewithnocollection2");

    makeRequest(getBaseUrl((HttpSolrClient) clients.get(1)), createCmd);
    
    // in both cases, the collection should have default to the core name
    cloudClient.getZkStateReader().forceUpdateCollection("corewithnocollection");
    cloudClient.getZkStateReader().forceUpdateCollection("corewithnocollection2");
    assertTrue(cloudClient.getZkStateReader().getClusterState().hasCollection("corewithnocollection"));
    assertTrue(cloudClient.getZkStateReader().getClusterState().hasCollection("corewithnocollection2"));
  }

  private void testNoConfigSetExist() throws Exception {
    assertFalse(cloudClient.getZkStateReader().getClusterState().hasCollection("corewithnocollection3"));

    // try and create a SolrCore with no collection name
    Create createCmd = new Create();
    createCmd.setCoreName("corewithnocollection3");
    createCmd.setCollection("");
    String dataDir = createTempDir().toFile().getAbsolutePath();
    createCmd.setDataDir(dataDir);
    createCmd.setNumShards(1);
    createCmd.setCollectionConfigName("conf123");
    boolean gotExp = false;
    try {
      makeRequest(getBaseUrl((HttpSolrClient) clients.get(1)), createCmd);
    } catch (SolrException e) {
      gotExp = true;
    }

    assertTrue(gotExp);
    TimeUnit.MILLISECONDS.sleep(200);
    // in both cases, the collection should have default to the core name
    cloudClient.getZkStateReader().forceUpdateCollection("corewithnocollection3");

    Collection<Slice> slices = cloudClient.getZkStateReader().getClusterState().getActiveSlices("corewithnocollection3");
    int replicaCount = 0;
    if (slices != null) {
      for (Slice slice : slices) {
        replicaCount += slice.getReplicas().size();
      }
    }
    assertEquals("replicaCount", 0, replicaCount);

    CollectionAdminRequest.List list = new CollectionAdminRequest.List();
    CollectionAdminResponse res = new CollectionAdminResponse();
        res.setResponse(makeRequest(getBaseUrl((HttpSolrClient) clients.get(1)), list));
    List<String> collections = (List<String>) res.getResponse().get("collections");
    assertTrue(collections.contains("corewithnocollection3"));
  }

  private void testNodesUsedByCreate() throws Exception {
    // we can use this client because we just want base url
    final String baseUrl = getBaseUrl((HttpSolrClient) clients.get(0));
    
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
    makeRequest(baseUrl, request);
    
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
    CloudSolrClient client1 = null;

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
      int numShards = TestUtil.nextInt(random(), 0, getShardCount()) + 1;
      int replicationFactor = TestUtil.nextInt(random(), 0, 3) + 1;
      int maxShardsPerNode = (((numShards * replicationFactor) / getCommonCloudSolrClient()
          .getZkStateReader().getClusterState().getLiveNodes().size())) + 1;

      
      CloudSolrClient client = null;
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
        if (client != null) client.close();
      }
    }
    
    Set<Entry<String,List<Integer>>> collectionInfosEntrySet = collectionInfos.entrySet();
    for (Entry<String,List<Integer>> entry : collectionInfosEntrySet) {
      String collection = entry.getKey();
      List<Integer> list = entry.getValue();
      checkForCollection(collection, list, null);
      
      String url = getUrlFromZk(collection);

      try (HttpSolrClient collectionClient = getHttpSolrClient(url)) {
        // poll for a second - it can take a moment before we are ready to serve
        waitForNon403or404or503(collectionClient);
      }
    }
    
    // sometimes we restart one of the jetty nodes
    if (random().nextBoolean()) {
      JettySolrRunner jetty = jettys.get(random().nextInt(jettys.size()));
      ChaosMonkey.stop(jetty);
      log.info("============ Restarting jetty");
      ChaosMonkey.start(jetty);
      
      for (Entry<String,List<Integer>> entry : collectionInfosEntrySet) {
        String collection = entry.getKey();
        List<Integer> list = entry.getValue();
        checkForCollection(collection, list, null);
        
        String url = getUrlFromZk(collection);
        
        try (HttpSolrClient collectionClient = getHttpSolrClient(url)) {
          // poll for a second - it can take a moment before we are ready to serve
          waitForNon403or404or503(collectionClient);
        }
      }
    }

    // sometimes we restart zookeeper
    if (random().nextBoolean()) {
      zkServer.shutdown();
      log.info("============ Restarting zookeeper");
      zkServer = new ZkTestServer(zkServer.getZkDir(), zkServer.getPort());
      zkServer.run();
    }
    
    // sometimes we cause a connection loss - sometimes it will hit the overseer
    if (random().nextBoolean()) {
      JettySolrRunner jetty = jettys.get(random().nextInt(jettys.size()));
      ChaosMonkey.causeConnectionLoss(jetty);
    }
    
    ZkStateReader zkStateReader = getCommonCloudSolrClient().getZkStateReader();
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

    try (HttpSolrClient collectionClient = getHttpSolrClient(url)) {

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
    }

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
    final String baseUrl = getBaseUrl((HttpSolrClient) clients.get(0));
    
    makeRequest(baseUrl, request);

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
 
    makeRequest(baseUrl, request);
    
    // ensure its out of the state
    assertCollectionNotExists(collectionName, 45);
    
    //collectionNameList.remove(collectionName);

    // remove an unknown collection
    params = new ModifiableSolrParams();
    params.set("action", CollectionAction.DELETE.toString());
    params.set("name", "unknown_collection");
    request = new QueryRequest(params);
    request.setPath("/admin/collections");
 
    boolean exp = false;
    try {
      makeRequest(baseUrl, request);
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
    makeRequest(baseUrl, request);
    
    List<Integer> list = new ArrayList<>(2);
    list.add(1);
    list.add(2);
    checkForCollection(collectionName, list, null);
    
    url = getUrlFromZk(collectionName);
    
    try (HttpSolrClient collectionClient = getHttpSolrClient(url)) {
      // poll for a second - it can take a moment before we are ready to serve
      waitForNon403or404or503(collectionClient);
    }

    for (int j = 0; j < cnt; j++) {
      waitForRecoveriesToFinish(collectionName, zkStateReader, false);
    }

    // test maxShardsPerNode
    int numLiveNodes = getCommonCloudSolrClient().getZkStateReader().getClusterState().getLiveNodes().size();
    int numShards = (numLiveNodes/2) + 1;
    int replicationFactor = 2;
    int maxShardsPerNode = 1;
    collectionInfos = new HashMap<>();
    try (CloudSolrClient client = createCloudClient("awholynewcollection_" + cnt)) {
      exp = false;
      try {
        createCollection(collectionInfos, "awholynewcollection_" + cnt,
            numShards, replicationFactor, maxShardsPerNode, client, null, "conf1");
      } catch (SolrException e) {
        exp = true;
      }
      assertTrue("expected exception", exp);
    }

    
    // Test createNodeSet
    numLiveNodes = getCommonCloudSolrClient().getZkStateReader().getClusterState().getLiveNodes().size();
    List<String> createNodeList = new ArrayList<>();
    int numOfCreateNodes = numLiveNodes/2;
    assertFalse("createNodeSet test is pointless with only " + numLiveNodes + " nodes running", numOfCreateNodes == 0);
    int i = 0;
    for (String liveNode : getCommonCloudSolrClient().getZkStateReader().getClusterState().getLiveNodes()) {
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

    try (SolrClient client = createCloudClient("awholynewcollection_" + (cnt+1))) {
      CollectionAdminResponse res = createCollection(collectionInfos, "awholynewcollection_" + (cnt+1), numShards, replicationFactor, maxShardsPerNode, client, StrUtils.join(createNodeList, ','), "conf1");
      assertTrue(res.isSuccess());
    }
    checkForCollection(collectionInfos.keySet().iterator().next(), collectionInfos.entrySet().iterator().next().getValue(), createNodeList);
    
    checkNoTwoShardsUseTheSameIndexDir();
    if(disableLegacy) {
      setClusterProp(client1, ZkStateReader.LEGACY_CLOUD, null);
      client1.close();
    }
  }
  
  private void testCollectionsAPIAddRemoveStress() throws Exception {
    
    class CollectionThread extends Thread {
      
      public CollectionThread(String name) {
        super(name);
      }
      
      public void run() {
        // create new collections rapid fire
        Map<String,List<Integer>> collectionInfos = new HashMap<>();
        int cnt = random().nextInt(TEST_NIGHTLY ? 13 : 1) + 1;
        
        for (int i = 0; i < cnt; i++) {
          String collectionName = "awholynewstresscollection_" + getName() + "_" + i;
          int numShards = TestUtil.nextInt(random(), 0, getShardCount() * 2) + 1;
          int replicationFactor = TestUtil.nextInt(random(), 0, 3) + 1;
          int maxShardsPerNode = (((numShards * 2 * replicationFactor) / getCommonCloudSolrClient()
              .getZkStateReader().getClusterState().getLiveNodes().size())) + 1;

          try (CloudSolrClient client = createCloudClient(i == 1 ? collectionName : null)) {

            createCollection(collectionInfos, collectionName,
                numShards, replicationFactor, maxShardsPerNode, client, null,
                "conf1");

            // remove collection
            CollectionAdminRequest.Delete delete = new CollectionAdminRequest.Delete()
                    .setCollectionName(collectionName);
            client.request(delete);
          } catch (SolrServerException | IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
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

  private void checkInstanceDirs(JettySolrRunner jetty) throws IOException {
    CoreContainer cores = jetty.getCoreContainer();
    Collection<SolrCore> theCores = cores.getCores();
    for (SolrCore core : theCores) {

      // look for core props file
      Path instancedir = (Path) core.getStatistics().get("instanceDir");
      assertTrue("Could not find expected core.properties file", Files.exists(instancedir.resolve("core.properties")));

      Path expected = Paths.get(jetty.getSolrHome()).toAbsolutePath().resolve("cores").resolve(core.getName());

      assertTrue("Expected: " + expected + "\nFrom core stats: " + instancedir, Files.isSameFile(expected, instancedir));

    }
  }

  private boolean waitForReloads(String collectionName, Map<String,Long> urlToTimeBefore) throws SolrServerException, IOException {


    TimeOut timeout = new TimeOut(45, TimeUnit.SECONDS);

    boolean allTimesAreCorrect = false;
    while (! timeout.hasTimedOut()) {
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
    ClusterState clusterState = getCommonCloudSolrClient().getZkStateReader()
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
          CoreAdminResponse mcr;
          try (HttpSolrClient server = getHttpSolrClient(coreProps.getBaseUrl())) {
            mcr = CoreAdminRequest.getStatus(coreProps.getCoreName(), server);
          }
          long before = mcr.getStartTime(coreProps.getCoreName()).getTime();
          urlToTime.put(coreProps.getCoreUrl(), before);
        }
      }
    } else {
      throw new IllegalArgumentException("Could not find collection in :"
          + clusterState.getCollectionsMap());
    }
  }

  private String getUrlFromZk(String collection) {
    ClusterState clusterState = getCommonCloudSolrClient().getZkStateReader().getClusterState();
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
    try (CloudSolrClient client = createCloudClient(null)) {
      createCollection(collectionName, client, 2, 2);
      String newReplicaName = Assign.assignNode(client.getZkStateReader().getClusterState().getCollection(collectionName));
      ArrayList<String> nodeList = new ArrayList<>(client.getZkStateReader().getClusterState().getLiveNodes());
      Collections.shuffle(nodeList, random());

      Replica newReplica = doAddReplica(collectionName, "shard1",
          Assign.assignNode(client.getZkStateReader().getClusterState().getCollection(collectionName)),
          nodeList.get(0), client, null);

      log.info("newReplica {},\n{} ", newReplica, client.getZkStateReader().getBaseUrlForNodeName(nodeList.get(0)));

      assertEquals("Replica should be created on the right node",
          client.getZkStateReader().getBaseUrlForNodeName(nodeList.get(0)), newReplica.getStr(ZkStateReader.BASE_URL_PROP));

      Properties props = new Properties();
      String instancePathStr = createTempDir().toString();
      props.put(CoreAdminParams.INSTANCE_DIR, instancePathStr); //Use name via the property.instanceDir method
      newReplica = doAddReplica(collectionName, "shard2",
          Assign.assignNode(client.getZkStateReader().getClusterState().getCollection(collectionName)),
          null, client, props);
      assertNotNull(newReplica);

      try (HttpSolrClient coreclient = getHttpSolrClient(newReplica.getStr(ZkStateReader.BASE_URL_PROP))) {
        CoreAdminResponse status = CoreAdminRequest.getStatus(newReplica.getStr("core"), coreclient);
        NamedList<Object> coreStatus = status.getCoreStatus(newReplica.getStr("core"));
        String instanceDirStr = (String) coreStatus.get("instanceDir");
        assertEquals(Paths.get(instanceDirStr).toString(), instancePathStr);
      }

      //Test to make sure we can't create another replica with an existing core_name of that collection
      String coreName = newReplica.getStr(CORE_NAME_PROP);
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set("action", "addreplica");
      params.set("collection", collectionName);
      params.set("shard", "shard1");
      params.set("name", coreName);
      QueryRequest request = new QueryRequest(params);
      request.setPath("/admin/collections");
      try {
        client.request(request);
        fail("AddReplica call should not have been successful");
      } catch (SolrException e) {
        assertTrue(e.getMessage().contains("Another replica with the same core name already exists for this collection"));
      }


      // Check that specifying property.name works. DO NOT remove this when the "name" property is deprecated
      // for ADDREPLICA, this is "property.name". See SOLR-7132
      props = new Properties();
      props.put(CoreAdminParams.NAME, "propertyDotName");

      newReplica = doAddReplica(collectionName, "shard1",
          Assign.assignNode(client.getZkStateReader().getClusterState().getCollection(collectionName)),
          nodeList.get(0), client, props);
      assertEquals("'core' should be 'propertyDotName' ", "propertyDotName", newReplica.getStr("core"));
    }
  }

  private Replica doAddReplica(String collectionName, String shard, String newReplicaName, String node,
                               CloudSolrClient client, Properties props) throws IOException, SolrServerException {
    CollectionAdminRequest.AddReplica addReplica = new CollectionAdminRequest.AddReplica();

    addReplica.setCollectionName(collectionName);
    addReplica.setShardName(shard);
    if (node != null) {
      addReplica.setNode(node);
    }
    if (props != null) {
      addReplica.setProperties(props);
    }
    client.request(addReplica);
    TimeOut timeout = new TimeOut(3, TimeUnit.SECONDS);
    Replica newReplica = null;

    for (; ! timeout.hasTimedOut(); ) {
      Slice slice = client.getZkStateReader().getClusterState().getSlice(collectionName, shard);
      newReplica = slice.getReplica(newReplicaName);
    }

    assertNotNull(newReplica);
    return newReplica;
  }
  @Override
  protected QueryResponse queryServer(ModifiableSolrParams params) throws SolrServerException, IOException {

    if (r.nextBoolean())
      return super.queryServer(params);

    if (r.nextBoolean())
      params.set("collection",DEFAULT_COLLECTION);

    QueryResponse rsp = getCommonCloudSolrClient().query(params);
    return rsp;
  }

  protected void createCollection(String COLL_NAME, CloudSolrClient client,int replicationFactor , int numShards ) throws Exception {
    int maxShardsPerNode = ((((numShards+1) * replicationFactor) / getCommonCloudSolrClient()
        .getZkStateReader().getClusterState().getLiveNodes().size())) + 1;

    Map<String, Object> props = makeMap(
        REPLICATION_FACTOR, replicationFactor,
        MAX_SHARDS_PER_NODE, maxShardsPerNode,
        NUM_SLICES, numShards);
    Map<String,List<Integer>> collectionInfos = new HashMap<>();
    createCollection(collectionInfos, COLL_NAME, props, client, "conf1");
    assertAllActive(COLL_NAME, getCommonCloudSolrClient().getZkStateReader());
    
  }
  
  private void clusterPropTest() throws Exception {
    try (CloudSolrClient client = createCloudClient(null)) {
      assertTrue("cluster property not set", setClusterProp(client, ZkStateReader.LEGACY_CLOUD, "false"));
      assertTrue("cluster property not unset ", setClusterProp(client, ZkStateReader.LEGACY_CLOUD, null));
    }
  }

  public static boolean setClusterProp(CloudSolrClient client, String name , String val) throws SolrServerException, IOException, InterruptedException {
    Map m = makeMap(
        "action", CollectionAction.CLUSTERPROP.toLower(),
        "name",name);

    if(val != null) m.put("val", val);
    SolrRequest request = new QueryRequest(new MapSolrParams(m));
    request.setPath("/admin/collections");
    client.request(request);

    TimeOut timeout = new TimeOut(3, TimeUnit.SECONDS);
    boolean changed = false;
    while(! timeout.hasTimedOut()){
      Thread.sleep(10);
      changed = Objects.equals(val,client.getZkStateReader().getClusterProps().get(name));
      if(changed) break;
    }
    return changed;
  }
}
