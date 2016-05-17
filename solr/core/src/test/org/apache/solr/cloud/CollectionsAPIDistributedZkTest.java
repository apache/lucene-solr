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

import static org.apache.solr.cloud.AbstractDistribZkTestBase.waitForCollectionToDisappear;
import static org.apache.solr.cloud.AbstractDistribZkTestBase.waitForRecoveriesToFinish;
import static org.apache.solr.cloud.AbstractFullDistribZkTestBase.setClusterProp;
import static org.apache.solr.client.solrj.request.CollectionAdminRequest.createCollection;
import static org.apache.solr.client.solrj.request.CollectionAdminRequest.deleteCollection;
import static org.apache.solr.client.solrj.request.CollectionAdminRequest.reloadCollection;
import static org.apache.solr.client.solrj.request.CollectionAdminRequest.setClusterProperty;
import static org.apache.solr.client.solrj.request.CollectionAdminRequest.addReplicaToShard;
import static org.apache.solr.common.cloud.ZkStateReader.CORE_NAME_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.REPLICATION_FACTOR;

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
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.management.ObjectName;

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
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
import org.apache.solr.common.SolrException;
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
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrInfoMBean.Category;
import org.apache.solr.util.TestInjection;
import org.apache.solr.util.TimeOut;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests the Cloud Collections API.
 */
@Slow
public class CollectionsAPIDistributedZkTest extends SolrCloudTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  private static final int NODE_COUNT = 5;

  // we randomly use a second config set rather than just one
  private final boolean secondConfigSet = false; //random().nextBoolean(); FIXME
  private final String configSet = secondConfigSet ? "conf1": "conf2";

  private static CloudSolrClient cloudClient;
  private static ZkTestServer zkServer;
  
  @BeforeClass
  public static void setup() throws Exception {
    configureCluster(NODE_COUNT)
        .addConfig("conf1", TEST_PATH().resolve("configsets").resolve("cloud-jmx").resolve("conf"))
        .addConfig("conf2", TEST_PATH().resolve("configsets").resolve("cloud-jmx").resolve("conf"))
        .configure();
    
    cloudClient = cluster.buildSolrClient(random().nextBoolean());
    cloudClient.setParallelUpdates(random().nextBoolean());
    cloudClient.connect();
    
    zkServer = cluster.getZkServer();
    
    TestInjection.randomDelayInCoreCreation = "true:20";
    System.setProperty("validateAfterInactivity", "200");
  }

  @AfterClass
  public static void deleteDefaultCollection() throws Exception {
    cloudClient.close();
    zkServer.shutdown();
  }
  
  // make sure tests leave all our nodes live
  @After
  public void bar() {
    ClusterState state = cloudClient.getZkStateReader().getClusterState();
    assertEquals(NODE_COUNT, state.getLiveNodes().size());
  }

  @Test
  public void deleteCollectionRemovesStaleZkCollectionsNode() throws Exception {
    String collectionName = "out_of_sync_collection";
    
    cloudClient.getZkStateReader().getZkClient().makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/" + collectionName, true);
    
    CollectionAdminResponse response = deleteCollection(collectionName)
        .process(cloudClient);
    assertEquals(0, response.getStatus());

    waitForCollectionToDisappear(collectionName, cloudClient.getZkStateReader(), false, true, 45);
    assertFalse(cloudClient.getZkStateReader().getZkClient().exists(ZkStateReader.COLLECTIONS_ZKNODE + "/" + collectionName, true));
  }

  @Test
  public void deletePartiallyCreatedCollection() throws Exception {
    String collectionName = "halfdeletedcollection";

    Properties properties = new Properties();
    properties.put(CoreAdminParams.DATA_DIR, createTempDir().toFile().getAbsolutePath());
    properties.put(CoreAdminParams.CORE, "halfdeletedcollection_shard1_replica1");
    CollectionAdminResponse response = createCollection(collectionName, configSet, 2, 1)
        .setProperties(properties)
        .process(cloudClient);
    assertEquals(0, response.getStatus());
    
    response = deleteCollection(collectionName)
        .process(cloudClient);

    waitForCollectionToDisappear(collectionName, cloudClient.getZkStateReader(), false, true, 45);
    assertFalse(cloudClient.getZkStateReader().getZkClient().exists(ZkStateReader.COLLECTIONS_ZKNODE + "/" + collectionName, true));

    // now creating that collection should work
    response = createCollection(collectionName, configSet, 2, 1)
        .process(cloudClient);
    assertEquals(0, response.getStatus());
  }
  
  @Test
  public void deleteCollectionOnlyInZk() throws Exception {
    String collectionName = "onlyinzk";

    cloudClient.getZkStateReader().getZkClient().makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/" + collectionName, true);

    CollectionAdminResponse response = deleteCollection(collectionName)
        .process(cloudClient);
    assertEquals(0, response.getStatus());

    waitForCollectionToDisappear(collectionName, cloudClient.getZkStateReader(), false, true, 45);
    assertFalse(cloudClient.getZkStateReader().getZkClient().exists(ZkStateReader.COLLECTIONS_ZKNODE + "/" + collectionName, true));
    
    // now creating that collection should work
    response = createCollection(collectionName, configSet, 2, 1)
        .process(cloudClient);
    assertEquals(0, response.getStatus());
    
    ZkStateReader zkStateReader = cloudClient.getZkStateReader();
    waitForRecoveriesToFinish(collectionName, zkStateReader, false, true, 330);
    
    response = deleteCollection(collectionName)
        .process(cloudClient);
    assertEquals(0, response.getStatus());
  }
  
  @Test
  public void deleteCollectionWithDownNodes() throws Exception {
    String collectionName = "halfdeletedcollection2";
    
    // now try to remove a collection when a couple of its nodes are down
    CollectionAdminResponse response = createCollection(collectionName, configSet, 3, 3)
        .setMaxShardsPerNode(6)
        .process(cloudClient);
    assertEquals(0, response.getStatus());
    
    ZkStateReader zkStateReader = cloudClient.getZkStateReader();
    waitForRecoveriesToFinish(collectionName, zkStateReader, false, true, 330);

    // stop a couple nodes
    List<JettySolrRunner> nodes = cluster.getJettySolrRunners().subList(0, 2);
    ChaosMonkey.stop(nodes);
    
    // wait for leaders to settle out
    for (int i = 1; i < 4; i++) {
      zkStateReader.getLeaderRetry(collectionName, "shard" + i, 30000);
    }
    
    // remove a collection
    response = deleteCollection(collectionName)
        .process(cloudClient);
    assertEquals(0, response.getStatus());

    TimeOut timeout = new TimeOut(10, TimeUnit.SECONDS);
    while (zkStateReader.getClusterState().hasCollection(collectionName)) {
      if (timeout.hasTimedOut()) {
        throw new AssertionError("Timeout waiting to see removed collection leave clusterstate");
      }      
      Thread.sleep(200);
    }

    assertFalse("Still found collection that should be gone", zkStateReader.getClusterState().hasCollection(collectionName));

    // restart downed nodes
    ChaosMonkey.start(nodes);
  }

  @Test
  public void testMissingParams() throws Exception {
    String collectionName = "badactioncollection";

    // try a bad action
    // can't use CollectionAdminRequest here as that does not allow malformed requests
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("action", "BADACTION");
    params.set("name", collectionName);
    params.set("numShards", 2);
    QueryRequest request = new QueryRequest(params);
    request.setPath("/admin/collections");
    boolean gotExp = false;
    try {
      cloudClient.request(request);
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
      cloudClient.request(request);
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
      cloudClient.request(request);
    } catch (SolrException e) {
      gotExp = true;
    }
    assertTrue(gotExp);
  }
  
  @Test
  public void testBadParams() throws SolrServerException, IOException {
    int exceptionCount = 0;
    
    // Too many replicas
    try {
      createCollection("collection", configSet, 2, 10).process(cloudClient);
    } catch (SolrException e) {
      ++exceptionCount;
    }
    
    // 0 numShards should fail
    try {
      createCollection("acollection", configSet, 0, 10).process(cloudClient);
    } catch (SolrException e) {
      ++exceptionCount;
    }
    
    assertEquals(2, exceptionCount);
  }
  
  @Test
  public void testFailOnOneNode() throws Exception {
    // first we make a core with the core name the collections api
    // will try and use - this will cause our mock fail
    Create createCmd = new Create();
    createCmd.setCoreName("halfcollection_shard1_replica1");
    createCmd.setCollection("halfcollectionblocker");
    String dataDir = createTempDir().toFile().getAbsolutePath();
    createCmd.setDataDir(dataDir);
    createCmd.setNumShards(1);
    createCmd.setCollectionConfigName(configSet);
    String baseUrl = cluster.getJettySolrRunners().get(0).getBaseUrl().toString();
    try (SolrClient client = getHttpSolrClient(baseUrl)) {
      client.request(createCmd);
    }
    
    createCmd = new Create();
    createCmd.setCoreName("halfcollection_shard1_replica1");
    createCmd.setCollection("halfcollectionblocker2");
    dataDir = createTempDir().toFile().getAbsolutePath();
    createCmd.setDataDir(dataDir);
    createCmd.setNumShards(1);
    createCmd.setCollectionConfigName(configSet);
    baseUrl = cluster.getJettySolrRunners().get(1).getBaseUrl().toString();
    try (SolrClient client = getHttpSolrClient(baseUrl)) {
      client.request(createCmd);
    }
    
    String nn1 = cluster.getJettySolrRunners().get(0).getCoreContainer().getZkController().getNodeName();
    String nn2 = cluster.getJettySolrRunners().get(1).getCoreContainer().getZkController().getNodeName();

    CollectionAdminResponse response = createCollection("halfcollection", configSet, 2, 1)
        .setCreateNodeSet(nn1 + "," + nn2)
        .process(cloudClient);
    assertEquals(0, response.getStatus());
    
    NamedList<String> msgs = response.getErrorMessages();
    assertNotNull(msgs);
    assertTrue(msgs.size() > 0);
    assertTrue(msgs.getVal(0).contains("Core with name 'halfcollection_shard1_replica1' already exists"));
  }
  
  @Test
  public void testNoCollectionSpecified() throws Exception {
    assertFalse(cloudClient.getZkStateReader().getClusterState().hasCollection("corewithnocollection"));
    assertFalse(cloudClient.getZkStateReader().getClusterState().hasCollection("corewithnocollection2"));
    
    // try and create a SolrCore with no collection name
    Create createCmd = new Create();
    createCmd.setCoreName("corewithnocollection");
    createCmd.setCollection("");
    createCmd.setDataDir(createTempDir().toFile().getAbsolutePath());
    createCmd.setNumShards(1);
    createCmd.setCollectionConfigName(configSet);
    cloudClient.request(createCmd);
    
    // try and create a SolrCore with null collection name
    createCmd.setCoreName("corewithnocollection2");
    createCmd.setCollection(null);
    createCmd.setDataDir(createTempDir().toFile().getAbsolutePath());
    cloudClient.request(createCmd);
    
    // in both cases, the collection should have default to the core name
    cloudClient.getZkStateReader().forceUpdateCollection("corewithnocollection");
    cloudClient.getZkStateReader().forceUpdateCollection("corewithnocollection2");
    assertTrue(cloudClient.getZkStateReader().getClusterState().hasCollection("corewithnocollection"));
    assertTrue(cloudClient.getZkStateReader().getClusterState().hasCollection("corewithnocollection2"));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testNoConfigSetExist() throws Exception {
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
      cloudClient.request(createCmd);
    } catch (SolrException e) {
      gotExp = true;
    }
    assertTrue(gotExp);
    TimeUnit.MILLISECONDS.sleep(200);
    // the collection should have default to the core name
    ZkStateReader zkStateReader = cloudClient.getZkStateReader();
    zkStateReader.forceUpdateCollection("corewithnocollection3");

    Collection<Slice> slices = zkStateReader.getClusterState().getCollection("corewithnocollection3").getActiveSlices();
    int replicaCount = 0;
    if (slices != null) {
      for (Slice slice : slices) {
        replicaCount += slice.getReplicas().size();
      }
    }
    assertEquals("replicaCount", 0, replicaCount);

    CollectionAdminResponse response = new CollectionAdminRequest.List().process(cloudClient);
    List<String> collections = (List<String>) response.getResponse().get("collections");
    assertTrue(collections.contains("corewithnocollection3"));
  }

  @Test
  public void testNodesUsedByCreate() throws Exception {
    String collectionName = "nodes_used_collection";

    CollectionAdminResponse response = createCollection(collectionName, configSet, 2, 2)
        .process(cloudClient);
    assertEquals(0, response.getStatus());
    
    ClusterState state = cloudClient.getZkStateReader().getClusterState();
    DocCollection collection = state.getCollection(collectionName);
    assertEquals(2, collection.getSlices().size());

    List<String> createNodeList = new ArrayList<>();
    createNodeList.addAll(state.getLiveNodes());

    int shardCount = 0;
    for (Slice slice : collection.getSlices()) {
      Collection<Replica> replicas = slice.getReplicas();
      for (Replica replica : replicas) {
        createNodeList.remove(replica.getNodeName());
        ++shardCount;
      }
    }
    assertEquals(2 * 2, shardCount);
    assertEquals(createNodeList.toString(), 1, createNodeList.size());
  }

  @Test
  public void testCollectionsAPI() throws Exception {
    boolean disableLegacy = random().nextBoolean();
    if (disableLegacy) {
      log.info("legacyCloud=false");
      CollectionAdminResponse response = setClusterProperty(ZkStateReader.LEGACY_CLOUD, "false")
          .process(cloudClient);
      assertEquals(0, response.getStatus());
    }
    
    // create new collections rapid fire
    String collectionPrefix = "awholynewcollection_";
    int cnt = random().nextInt(TEST_NIGHTLY ? 6 : 1) + 1;
    int[] numShards = new int[cnt];
    int[] replicationFactor = new int[cnt];
    
    for (int i = 0; i < numShards.length; i++) {
      numShards[i] = TestUtil.nextInt(random(), 0, 4) + 1;
      replicationFactor[i] = TestUtil.nextInt(random(), 0, 3) + 1;
      int totalShards = numShards[i] * replicationFactor[i];
      int liveNodes = cloudClient.getZkStateReader().getClusterState().getLiveNodes().size();
      int maxShardsPerNode = (totalShards / liveNodes) + 1;

      if (i == 0) {
        // Test if we can create a collection through CloudSolrServer where
        // you havnt set default-collection
        // This is nice because you want to be able to create you first
        // collection using CloudSolrServer, and in such case there is
        // nothing reasonable to set as default-collection
        cloudClient.setDefaultCollection(null);
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
        cloudClient.setDefaultCollection(collectionPrefix + i);
      }
      CollectionAdminResponse response = createCollection(collectionPrefix + i, configSet, numShards[i], replicationFactor[i])
          .setMaxShardsPerNode(maxShardsPerNode)
          .process(cloudClient);
      assertEquals(0, response.getStatus());
    }
    
    ClusterState clusterState = cloudClient.getZkStateReader().getClusterState();
    for (int i = 0; i < numShards.length; i++) {
      AbstractFullDistribZkTestBase.checkForCollection(clusterState, collectionPrefix + i, numShards[i], replicationFactor[i]);
      String url = AbstractFullDistribZkTestBase.getUrlFromZk(clusterState, collectionPrefix + i);

      try (HttpSolrClient collectionClient = getHttpSolrClient(url)) {
        // poll for a second - it can take a moment before we are ready to serve
        AbstractFullDistribZkTestBase.waitForNon403or404or503(collectionClient);
      }
    }

    // sometimes we restart one of the jetty nodes
    if (random().nextBoolean()) {
      List<JettySolrRunner> jettys = cluster.getJettySolrRunners();
      JettySolrRunner jetty = jettys.get(random().nextInt(jettys.size()));
      ChaosMonkey.stop(jetty);
      log.info("============ Restarting jetty");
      ChaosMonkey.start(jetty);
      
      for (int i = 0; i < numShards.length; i++) {
        AbstractFullDistribZkTestBase.checkForCollection(clusterState, collectionPrefix + i, numShards[i], replicationFactor[i]);
        String url = AbstractFullDistribZkTestBase.getUrlFromZk(clusterState, collectionPrefix + i);

        try (HttpSolrClient collectionClient = getHttpSolrClient(url)) {
          // poll for a second - it can take a moment before we are ready to serve
          AbstractFullDistribZkTestBase.waitForNon403or404or503(collectionClient);
        }
      }
    }

    // sometimes we restart zookeeper
    if (random().nextBoolean()) {
      zkServer.shutdown();
      log.info("============ Restarting zookeeper");
      zkServer = new ZkTestServer(zkServer.getZkDir(), zkServer.getPort());
      zkServer.run();
      // make sure Zookeeper is back up
      SolrZkClient zkClient = new SolrZkClient(zkServer.getZkHost(), AbstractZkTestCase.TIMEOUT);
      assertTrue(zkClient.isConnected());
      zkClient.close();
    }
    
    // sometimes we cause a connection loss - sometimes it will hit the overseer
    if (random().nextBoolean()) {
      List<JettySolrRunner> jettys = cluster.getJettySolrRunners();
      JettySolrRunner jetty = jettys.get(random().nextInt(jettys.size()));
      ChaosMonkey.causeConnectionLoss(jetty);
    }
    
    ZkStateReader zkStateReader = cloudClient.getZkStateReader();
    for (int i = 0; i < numShards.length; i++) {
      waitForRecoveriesToFinish(collectionPrefix + i, zkStateReader, false, true, 330);
      
      if (secondConfigSet) {
        // let's see if they are using the second config set
        byte[] data = zkStateReader.getZkClient()
            .getData(ZkStateReader.COLLECTIONS_ZKNODE + "/" + collectionPrefix + i, null, null, true);
        assertNotNull(data);
        ZkNodeProps props = ZkNodeProps.load(data);
        String configName = props.getStr(ZkController.CONFIGNAME_PROP);
        assertEquals(configSet, configName);
      }
    }
    
    checkInstanceDirs(cluster.getJettySolrRunners().get(0)); 
    
    String collectionName = collectionPrefix + random().nextInt(numShards.length);
    String url = AbstractFullDistribZkTestBase.getUrlFromZk(clusterState, collectionName);

    try (HttpSolrClient collectionClient = getHttpSolrClient(url)) {
      // lets try and use the solrj client to index a couple documents
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", "foo");
      doc.addField("a_i1", -600);
      collectionClient.add(doc);

      doc.setField("id", "bar");
      doc.setField("a_i1", 1234);
      collectionClient.add(doc);

      doc.setField("id", "baz");
      doc.setField("a_i1", 23);
      collectionClient.add(doc);

      collectionClient.commit();

      assertEquals(3, collectionClient.query(new SolrQuery("*:*")).getResults().getNumFound());
    }

    // lets try a collection reload
    
    // get core open times
    Map<String,Long> urlToTimeBefore = new HashMap<>();
    collectStartTimes(collectionName, urlToTimeBefore);
    assertTrue(urlToTimeBefore.size() > 0);
    CollectionAdminResponse response = reloadCollection(collectionName)
        .process(cloudClient);
    assertEquals(0, response.getStatus());

    // reloads make take a short while
    boolean allTimesAreCorrect = waitForReloads(collectionName, urlToTimeBefore);
    assertTrue("some core start times did not change on reload", allTimesAreCorrect);
    
    waitForRecoveriesToFinish("awholynewcollection_" + (cnt - 1), zkStateReader, false, true, 330);
    
    // remove a collection
    response = deleteCollection(collectionName)
        .process(cloudClient);
    assertEquals(0, response.getStatus());
    
    // ensure its out of the state
    waitForCollectionToDisappear(collectionName, cloudClient.getZkStateReader(), false, true, 45);
    assertFalse(cloudClient.getZkStateReader().getZkClient().exists(ZkStateReader.COLLECTIONS_ZKNODE + "/" + collectionName, true));

    // remove an unknown collection
    boolean exp = false;
    try {
      response = deleteCollection("unknown_collection")
          .process(cloudClient);
    } catch (SolrException e) {
      exp = true;
    }
    assertTrue("Expected exception", exp);
    
    // create another collection should still work
    collectionName = "acollectionafterbaddelete";
    response = createCollection(collectionName, configSet, 1, 2)
        .process(cloudClient);
    assertEquals(0, response.getStatus());
    
    clusterState = cloudClient.getZkStateReader().getClusterState();
    AbstractFullDistribZkTestBase.checkForCollection(clusterState, collectionName, 1, 2);
    
    url = AbstractFullDistribZkTestBase.getUrlFromZk(clusterState, collectionName);
    try (HttpSolrClient collectionClient = getHttpSolrClient(url)) {
      // poll for a second - it can take a moment before we are ready to serve
      AbstractFullDistribZkTestBase.waitForNon403or404or503(collectionClient);
    }

    for (int i = 0; i < cnt; i++) {
      waitForRecoveriesToFinish(collectionName, zkStateReader, false, true, 330);
    }

    // test maxShardsPerNode
    int numLiveNodes = cloudClient.getZkStateReader().getClusterState().getLiveNodes().size();
    exp = false;
    try {
      response = createCollection(collectionPrefix + numShards.length, "conf1", (numLiveNodes/2) + 1, 2)
          .setMaxShardsPerNode(1)
          .process(cloudClient);
    } catch (SolrException e) {
      exp = true;
    }
    assertTrue("expected exception", exp);

    // Test createNodeSet
    numLiveNodes = cloudClient.getZkStateReader().getClusterState().getLiveNodes().size();
    List<String> createNodeList = new ArrayList<>();
    int numOfCreateNodes = numLiveNodes/2;
    assertFalse("createNodeSet test is pointless with only " + numLiveNodes + " nodes running", numOfCreateNodes == 0);
    int i = 0;
    for (String liveNode : cloudClient.getZkStateReader().getClusterState().getLiveNodes()) {
      if (i < numOfCreateNodes) {
        createNodeList.add(liveNode);
        i++;
      } else {
        break;
      }
    }

    collectionName = collectionPrefix + (numShards.length + 1);
    int maxShardsPerNode = 2;
    int newNumShards = createNodeList.size() * maxShardsPerNode;

    response = createCollection(collectionName, "conf1", newNumShards, 1)
        .setMaxShardsPerNode(maxShardsPerNode)
        .setCreateNodeSet(StrUtils.join(createNodeList, ','))
        .process(cloudClient);
    assertEquals(0, response.getStatus());

    clusterState = cloudClient.getZkStateReader().getClusterState();
    AbstractFullDistribZkTestBase.checkForCollection(clusterState, collectionName, newNumShards, 1, createNodeList);

    checkNoTwoShardsUseTheSameIndexDir();
    if(disableLegacy) {
      response = setClusterProperty(ZkStateReader.LEGACY_CLOUD, null)
          .process(cloudClient);
      assertEquals(0, response.getStatus());
    }
  }
  
  @Test
  public void testCollectionsAPIAddRemoveStress() throws Exception {
    
    class CollectionThread extends Thread {
      
      public CollectionThread(String name) {
        super(name);
      }
      
      public void run() {
        // create new collections rapid fire
        int cnt = random().nextInt(TEST_NIGHTLY ? 13 : 1) + 1;
        
        for (int i = 0; i < cnt; i++) {
          String collectionName = "awholynewstresscollection_" + getName() + "_" + i;
          int numShards = TestUtil.nextInt(random(), 0, 4 * 2) + 1; // 4 shards
          int replicationFactor = TestUtil.nextInt(random(), 0, 3) + 1;
          int maxShardsPerNode = (((numShards * 2 * replicationFactor) / cloudClient
              .getZkStateReader().getClusterState().getLiveNodes().size())) + 1;

          if (i == 0) {
            cloudClient.setDefaultCollection(null);
          } else if (i == 1) {
            cloudClient.setDefaultCollection(collectionName);
          }

          try {
            CollectionAdminResponse response = createCollection(collectionName, "conf1", numShards, replicationFactor)
                .setMaxShardsPerNode(maxShardsPerNode)
                .process(cloudClient);
            assertEquals(0, response.getStatus());

            // remove collection
            response = deleteCollection(collectionName)
                .process(cloudClient);
            assertEquals(0, response.getStatus());
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

      Path expected = Paths.get(jetty.getSolrHome()).toAbsolutePath().resolve(core.getName());
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
    ClusterState clusterState = cloudClient.getZkStateReader().getClusterState();
    if (clusterState.hasCollection(collectionName)) {
      Map<String,Slice> slices = clusterState.getCollection(collectionName).getSlicesMap();

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
          + clusterState.getCollections());
    }
  }
  
  private void checkNoTwoShardsUseTheSameIndexDir() throws Exception {
    Map<String, Set<String>> indexDirToShardNamesMap = new HashMap<>();
    
    List<MBeanServer> servers = new LinkedList<>();
    servers.add(ManagementFactory.getPlatformMBeanServer());
    servers.addAll(MBeanServerFactory.findMBeanServer(null));
    for (final MBeanServer server : servers) {
      Set<ObjectName> mbeans = new HashSet<>();
      mbeans.addAll(server.queryNames(null, null));
      for (final ObjectName mbean : mbeans) {
        try {
          Object value = server.getAttribute(mbean, "category");
          Object coreName = server.getAttribute(mbean, "coreName"); 
          Object indexDir = server.getAttribute(mbean, "indexDir");
          Object name = server.getAttribute(mbean, "name");

          if (value != null && value.toString().equals(Category.CORE.toString())
              && coreName != null && indexDir != null && name != null) {
            if (!indexDirToShardNamesMap.containsKey(indexDir.toString())) {
              indexDirToShardNamesMap.put(indexDir.toString(), new HashSet<String>());
            }
            indexDirToShardNamesMap.get(indexDir.toString()).add(name.toString());
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

  @Test
  public void addReplicaTest() throws Exception {
    String collectionName = "addReplicaColl";
    
    CollectionAdminResponse response = createCollection(collectionName, "conf1", 2, 2)
        .setMaxShardsPerNode(2)
        .process(cloudClient);
    assertEquals(0, response.getStatus());

    ClusterState state = cloudClient.getZkStateReader().getClusterState();
    ArrayList<String> nodeList = new ArrayList<>(state.getLiveNodes());
    Collections.shuffle(nodeList, random());

    String replicaName = Assign.assignNode(cloudClient.getZkStateReader().getClusterState().getCollection(collectionName));
    Replica newReplica = doAddReplica(collectionName, "shard1", replicaName, nodeList.get(0), null);

    log.info("newReplica {},\n{} ", newReplica, cloudClient.getZkStateReader().getBaseUrlForNodeName(nodeList.get(0)));

    assertEquals("Replica should be created on the right node",
        cloudClient.getZkStateReader().getBaseUrlForNodeName(nodeList.get(0)), newReplica.getStr(ZkStateReader.BASE_URL_PROP));

    Properties props = new Properties();
    String instancePathStr = createTempDir().toString();
    props.put(CoreAdminParams.INSTANCE_DIR, instancePathStr); //Use name via the property.instanceDir method
    replicaName = Assign.assignNode(cloudClient.getZkStateReader().getClusterState().getCollection(collectionName));
    newReplica = doAddReplica(collectionName, "shard2", replicaName, null, props);
    assertNotNull(newReplica);

    try (HttpSolrClient coreclient = getHttpSolrClient(newReplica.getStr(ZkStateReader.BASE_URL_PROP))) {
      CoreAdminResponse status = CoreAdminRequest.getStatus(newReplica.getStr("core"), coreclient);
      NamedList<Object> coreStatus = status.getCoreStatus(newReplica.getStr("core"));
      String instanceDirStr = (String) coreStatus.get("instanceDir");
      assertEquals(Paths.get(instanceDirStr).toString(), instancePathStr);
    }

    // Test to make sure we can't create another replica with an existing core_name of that collection
    // Note, we can set not the core name via CollectionAdminnRequest.addReplicaToShard(..)
    String coreName = newReplica.getStr(CORE_NAME_PROP);
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("action", "addreplica");
    params.set("collection", collectionName);
    params.set("shard", "shard1");
    params.set("name", coreName);
    QueryRequest request = new QueryRequest(params);
    request.setPath("/admin/collections");
    try {
      cloudClient.request(request);
      fail("AddReplica call should not have been successful");
    } catch (SolrException e) {
      assertTrue(e.getMessage().contains("Another replica with the same core name already exists for this collection"));
    }

    // Check that specifying property.name works. DO NOT remove this when the "name" property is deprecated
    // for ADDREPLICA, this is "property.name". See SOLR-7132
    props = new Properties();
    props.put(CoreAdminParams.NAME, "propertyDotName");
    replicaName = Assign.assignNode(cloudClient.getZkStateReader().getClusterState().getCollection(collectionName));
    newReplica = doAddReplica(collectionName, "shard1", replicaName, nodeList.get(0), props);
    assertEquals("'core' should be 'propertyDotName' ", "propertyDotName", newReplica.getStr("core"));
  }

  private Replica doAddReplica(String collectionName, String shard, String newReplicaName, String node,
                               Properties props) throws IOException, SolrServerException {
    CollectionAdminResponse response = addReplicaToShard(collectionName, shard)
        .setNode(node)
        .setProperties(props)
        .process(cloudClient);
    assertEquals(0, response.getStatus());

    TimeOut timeout = new TimeOut(3, TimeUnit.SECONDS);
    Replica newReplica = null;

    while (! timeout.hasTimedOut()) {
      Slice slice = cloudClient.getZkStateReader().getClusterState().getCollection(collectionName).getSlice(shard);
      newReplica = slice.getReplica(newReplicaName);
    }

    assertNotNull(newReplica);
    return newReplica;
  }
  
  @Test
  public void clusterPropTest() throws Exception {
    assertTrue("cluster property not set", setClusterProp(cloudClient, ZkStateReader.LEGACY_CLOUD, "false"));
    assertTrue("cluster property not unset ", setClusterProp(cloudClient, ZkStateReader.LEGACY_CLOUD, null));
  }
  
}
