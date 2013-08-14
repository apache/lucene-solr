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

import org.apache.lucene.util.Constants;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.lucene.util._TestUtil;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer.RemoteSolrException;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest.Create;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.CoreAdminResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams.CollectionAction;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrInfoMBean.Category;
import org.apache.solr.servlet.SolrDispatchFilter;
import org.apache.solr.update.DirectUpdateHandler2;
import org.apache.solr.update.SolrCmdDistributor.Request;
import org.apache.solr.util.DefaultSolrThreadFactory;
import org.junit.Before;
import org.junit.BeforeClass;

import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.management.ObjectName;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.apache.solr.cloud.OverseerCollectionProcessor.REPLICATION_FACTOR;

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
  
  CompletionService<Request> completionService;
  Set<Future<Request>> pending;
  
  @BeforeClass
  public static void beforeThisClass2() throws Exception {
    assumeFalse("FIXME: This test fails under Java 8 all the time, see SOLR-4711", Constants.JRE_IS_MINIMUM_JAVA8);
  }
  
  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    System.setProperty("numShards", Integer.toString(sliceCount));
    System.setProperty("solr.xml.persist", "true");
  }
  
  protected String getSolrXml() {
    return "solr-no-core.xml";
  }

  
  public CollectionsAPIDistributedZkTest() {
    fixShardCount = true;
    
    sliceCount = 2;
    shardCount = 4;
    completionService = new ExecutorCompletionService<Request>(executor);
    pending = new HashSet<Future<Request>>();
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
    testNodesUsedByCreate();
    testCollectionsAPI();
    testErrorHandling();
    deletePartiallyCreatedCollection();
    deleteCollectionRemovesStaleZkCollectionsNode();
    
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
    
    List<Integer> numShardsNumReplicaList = new ArrayList<Integer>();
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

  private void deletePartiallyCreatedCollection() throws Exception {
    final String baseUrl = getBaseUrl((HttpSolrServer) clients.get(0));
    String collectionName = "halfdeletedcollection";
    Create createCmd = new Create();
    createCmd.setCoreName("halfdeletedcollection_shard1_replica1");
    createCmd.setCollection(collectionName);
    String dataDir = SolrTestCaseJ4.dataDir.getAbsolutePath() + File.separator
        + System.currentTimeMillis() + "halfcollection" + "_hdn";
    createCmd.setDataDir(dataDir);
    createCmd.setNumShards(2);
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
    resp = createNewSolrServer("", baseUrl).request(request);
  }
  
  
  private void deleteCollectionWithDownNodes() throws Exception {
    String baseUrl = getBaseUrl((HttpSolrServer) clients.get(0));
    // now try to remove a collection when a couple of it's nodes are down
    createCollection(null, "halfdeletedcollection2", 3, 2, 6,
        createNewSolrServer("", baseUrl), null);
    
    waitForRecoveriesToFinish("halfdeletedcollection2", false);
    
    // stop a couple nodes
    ChaosMonkey.stop(jettys.get(0));
    ChaosMonkey.stop(jettys.get(1));
    
    baseUrl = getBaseUrl((HttpSolrServer) clients.get(2));
    
    // remove a collection
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("action", CollectionAction.DELETE.toString());
    params.set("name", "halfdeletedcollection2");
    QueryRequest request = new QueryRequest(params);
    request.setPath("/admin/collections");
    
    createNewSolrServer("", baseUrl).request(request);
    
    cloudClient.getZkStateReader().updateClusterState(true);
    assertFalse(cloudClient.getZkStateReader().getClusterState()
        .getCollections().contains("halfdeletedcollection2"));
    
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
    params.set(REPLICATION_FACTOR, 10);
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
    
    // No numShards should fail
    params = new ModifiableSolrParams();
    params.set("action", CollectionAction.CREATE.toString());
    collectionName = "acollection";
    params.set("name", collectionName);
    params.set(REPLICATION_FACTOR, 10);
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
    String dataDir = SolrTestCaseJ4.dataDir.getAbsolutePath() + File.separator
        + System.currentTimeMillis() + "halfcollection" + "_3n";
    createCmd.setDataDir(dataDir);
    createCmd.setNumShards(1);
    createNewSolrServer("", baseUrl).request(createCmd);
    
    createCmd = new Create();
    createCmd.setCoreName("halfcollection_shard1_replica1");
    createCmd.setCollection("halfcollectionblocker2");
    dataDir = SolrTestCaseJ4.dataDir.getAbsolutePath() + File.separator
        + System.currentTimeMillis() + "halfcollection" + "_3n";
    createCmd.setDataDir(dataDir);
    createCmd.setNumShards(1);
    createNewSolrServer("", getBaseUrl((HttpSolrServer) clients.get(1))).request(createCmd);
    
    params = new ModifiableSolrParams();
    params.set("action", CollectionAction.CREATE.toString());
    collectionName = "halfcollection";
    params.set("name", collectionName);
    params.set("numShards", 2);
    params.set("wt", "xml");
    
    String nn1 = ((SolrDispatchFilter) jettys.get(0).getDispatchFilter().getFilter()).getCores().getZkController().getNodeName();
    String nn2 =  ((SolrDispatchFilter) jettys.get(1).getDispatchFilter().getFilter()).getCores().getZkController().getNodeName();
    
    params.set(OverseerCollectionProcessor.CREATE_NODE_SET, nn1 + "," + nn2);
    request = new QueryRequest(params);
    request.setPath("/admin/collections");
    gotExp = false;
    resp = createNewSolrServer("", baseUrl).request(request);
    
    SimpleOrderedMap success = (SimpleOrderedMap) resp.get("success");
    SimpleOrderedMap failure = (SimpleOrderedMap) resp.get("failure");

    String val1 = success.getVal(0).toString();
    String val2 = failure.getVal(0).toString();
    assertTrue(val1.contains("SolrException") || val2.contains("SolrException"));
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
    QueryRequest request = new QueryRequest(params);
    request.setPath("/admin/collections");
    createNewSolrServer("", baseUrl).request(request);
    
    List<Integer> numShardsNumReplicaList = new ArrayList<Integer>();
    numShardsNumReplicaList.add(2);
    numShardsNumReplicaList.add(2);
    checkForCollection("nodes_used_collection", numShardsNumReplicaList , null);

    List<String> createNodeList = new ArrayList<String>();

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
 
    // TODO: fragile - because we dont pass collection.confName, it will only
    // find a default if a conf set with a name matching the collection name is found, or 
    // if there is only one conf set. That and the fact that other tests run first in this
    // env make this pretty fragile
    
    // create new collections rapid fire
    Map<String,List<Integer>> collectionInfos = new HashMap<String,List<Integer>>();
    int cnt = random().nextInt(6) + 1;
    
    for (int i = 0; i < cnt; i++) {
      int numShards = _TestUtil.nextInt(random(), 0, shardCount) + 1;
      int replicationFactor = _TestUtil.nextInt(random(), 0, 3) + 2;
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
        
        createCollection(collectionInfos, "awholynewcollection_" + i,
            numShards, replicationFactor, maxShardsPerNode, client, null);
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
    }
    ZkStateReader zkStateReader = getCommonCloudSolrServer().getZkStateReader();
    for (int j = 0; j < cnt; j++) {
      waitForRecoveriesToFinish("awholynewcollection_" + j, zkStateReader, false);
    }
    
    List<String> collectionNameList = new ArrayList<String>();
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
    
    // lets try a collection reload
    
    // get core open times
    Map<String,Long> urlToTimeBefore = new HashMap<String,Long>();
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
    request = new QueryRequest(params);
    request.setPath("/admin/collections");
    createNewSolrServer("", baseUrl).request(request);
    
    List<Integer> list = new ArrayList<Integer> (2);
    list.add(1);
    list.add(2);
    checkForCollection(collectionName, list, null);
    
    url = getUrlFromZk(collectionName);
    
    collectionClient = new HttpSolrServer(url);
    
    // poll for a second - it can take a moment before we are ready to serve
    waitForNon403or404or503(collectionClient);
    
    for (int j = 0; j < cnt; j++) {
      waitForRecoveriesToFinish(collectionName, zkStateReader, false);
    }

    // test maxShardsPerNode
    int numLiveNodes = getCommonCloudSolrServer().getZkStateReader().getClusterState().getLiveNodes().size();
    int numShards = (numLiveNodes/2) + 1;
    int replicationFactor = 2;
    int maxShardsPerNode = 1;
    collectionInfos = new HashMap<String,List<Integer>>();
    CloudSolrServer client = createCloudClient("awholynewcollection_" + cnt);
    try {
      exp = false;
      try {
      createCollection(collectionInfos, "awholynewcollection_" + cnt, numShards, replicationFactor, maxShardsPerNode, client, null);
      } catch (SolrException e) {
        exp = true;
      }
      assertTrue("expected exception", exp);
    } finally {
      client.shutdown();
    }

    
    // Test createNodeSet
    numLiveNodes = getCommonCloudSolrServer().getZkStateReader().getClusterState().getLiveNodes().size();
    List<String> createNodeList = new ArrayList<String>();
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
    collectionInfos = new HashMap<String,List<Integer>>();
    client = createCloudClient("awholynewcollection_" + (cnt+1));
    try {
      createCollection(collectionInfos, "awholynewcollection_" + (cnt+1), numShards, replicationFactor, maxShardsPerNode, client, StrUtils.join(createNodeList, ','));
    } finally {
      client.shutdown();
    }
    checkForCollection(collectionInfos.keySet().iterator().next(), collectionInfos.entrySet().iterator().next().getValue(), createNodeList);
    
    checkNoTwoShardsUseTheSameIndexDir();
  }

  private boolean waitForReloads(String collectionName, Map<String,Long> urlToTimeBefore) throws SolrServerException, IOException {
    
    
    long timeoutAt = System.currentTimeMillis() + 45000;

    boolean allTimesAreCorrect = false;
    while (System.currentTimeMillis() < timeoutAt) {
      Map<String,Long> urlToTimeAfter = new HashMap<String,Long>();
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
    Map<String,DocCollection> collections = getCommonCloudSolrServer().getZkStateReader()
        .getClusterState().getCollectionStates();
    if (collections.containsKey(collectionName)) {
      Map<String,Slice> slices = collections.get(collectionName).getSlicesMap();

      Iterator<Entry<String,Slice>> it = slices.entrySet().iterator();
      while (it.hasNext()) {
        Entry<String,Slice> sliceEntry = it.next();
        Map<String,Replica> sliceShards = sliceEntry.getValue().getReplicasMap();
        Iterator<Entry<String,Replica>> shardIt = sliceShards.entrySet()
            .iterator();
        while (shardIt.hasNext()) {
          Entry<String,Replica> shardEntry = shardIt.next();
          ZkCoreNodeProps coreProps = new ZkCoreNodeProps(shardEntry.getValue());
          CoreAdminResponse mcr = CoreAdminRequest.getStatus(
              coreProps.getCoreName(),
              new HttpSolrServer(coreProps.getBaseUrl()));
          long before = mcr.getStartTime(coreProps.getCoreName()).getTime();
          urlToTime.put(coreProps.getCoreUrl(), before);
        }
      }
    } else {
      throw new IllegalArgumentException("Could not find collection in :"
          + collections.keySet());
    }
  }

  private String getUrlFromZk(String collection) {
    ClusterState clusterState = getCommonCloudSolrServer().getZkStateReader().getClusterState();
    Map<String,Slice> slices = clusterState.getCollectionStates().get(collection).getSlicesMap();
    
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

  private void waitForNon403or404or503(HttpSolrServer collectionClient)
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
  }
  
  private void checkForMissingCollection(String collectionName)
      throws Exception {
    // check for a  collection - we poll the state
    long timeoutAt = System.currentTimeMillis() + 45000;
    boolean found = true;
    while (System.currentTimeMillis() < timeoutAt) {
      getCommonCloudSolrServer().getZkStateReader().updateClusterState(true);
      ClusterState clusterState = getCommonCloudSolrServer().getZkStateReader().getClusterState();
      Map<String,DocCollection> collections = clusterState
          .getCollectionStates();
      if (!collections.containsKey(collectionName)) {
        found = false;
        break;
      }
      Thread.sleep(100);
    }
    if (found) {
      fail("Found collection that should be gone " + collectionName);
    }
  }

  private void checkNoTwoShardsUseTheSameIndexDir() throws Exception {
    Map<String, Set<String>> indexDirToShardNamesMap = new HashMap<String, Set<String>>();
    
    List<MBeanServer> servers = new LinkedList<MBeanServer>();
    servers.add(ManagementFactory.getPlatformMBeanServer());
    servers.addAll(MBeanServerFactory.findMBeanServer(null));
    for (final MBeanServer server : servers) {
      Set<ObjectName> mbeans = new HashSet<ObjectName>();
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

  @Override
  protected QueryResponse queryServer(ModifiableSolrParams params) throws SolrServerException {

    if (r.nextBoolean())
      return super.queryServer(params);

    if (r.nextBoolean())
      params.set("collection",DEFAULT_COLLECTION);

    QueryResponse rsp = getCommonCloudSolrServer().query(params);
    return rsp;
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
}
