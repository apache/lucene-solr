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

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.MalformedURLException;
import java.util.ArrayList;
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

import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.management.ObjectName;

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.lucene.util._TestUtil;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
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
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrInfoMBean.Category;
import org.apache.solr.update.DirectUpdateHandler2;
import org.apache.solr.update.SolrCmdDistributor.Request;
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
  
  CompletionService<Request> completionService;
  Set<Future<Request>> pending;
  
  @BeforeClass
  public static void beforeThisClass2() throws Exception {
    // TODO: we use an fs based dir because something
    // like a ram dir will not recover correctly right now
    useFactory(null);
  }
  
  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    System.setProperty("numShards", Integer.toString(sliceCount));
    System.setProperty("solr.xml.persist", "true");
  }

  
  public CollectionsAPIDistributedZkTest() {
    fixShardCount = true;
    
    sliceCount = 2;
    shardCount = 4;
    completionService = new ExecutorCompletionService<Request>(executor);
    pending = new HashSet<Future<Request>>();
    
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
    // setLoggingLevel(null);

    ZkStateReader zkStateReader = cloudClient.getZkStateReader();
    // make sure we have leaders for each shard
    for (int j = 1; j < sliceCount; j++) {
      zkStateReader.getLeaderRetry(DEFAULT_COLLECTION, "shard" + j, 10000);
    }      // make sure we again have leaders for each shard
    
    waitForRecoveriesToFinish(false);
    
    del("*:*");

    // would be better if these where all separate tests - but much, much
    // slower

    testCollectionsAPI();

    // Thread.sleep(10000000000L);
    if (DEBUG) {
      super.printLayout();
    }
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
    final String baseUrl = ((HttpSolrServer) clients.get(0)).getBaseURL().substring(
        0,
        ((HttpSolrServer) clients.get(0)).getBaseURL().length()
            - DEFAULT_COLLECTION.length() - 1);
    
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
 
    createNewSolrServer("", baseUrl).request(request);
    
    // create another collection should still work
    params = new ModifiableSolrParams();
    params.set("action", CollectionAction.CREATE.toString());

    params.set("numShards", 1);
    params.set(OverseerCollectionProcessor.REPLICATION_FACTOR, 2);
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
      createCollection(collectionInfos, "awholynewcollection_" + cnt, numShards, replicationFactor, maxShardsPerNode, client, null);
    } finally {
      client.shutdown();
    }
    
    // TODO: REMOVE THE SLEEP IN THE METHOD CALL WHEN WE HAVE COLLECTION API 
    // RESPONSES
    checkCollectionIsNotCreated(collectionInfos.keySet().iterator().next());
    
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


  protected void createCollection(Map<String,List<Integer>> collectionInfos,
      String collectionName, int numShards, int numReplicas, int maxShardsPerNode, SolrServer client, String createNodeSetStr) throws SolrServerException, IOException {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("action", CollectionAction.CREATE.toString());

    params.set(OverseerCollectionProcessor.NUM_SLICES, numShards);
    params.set(OverseerCollectionProcessor.REPLICATION_FACTOR, numReplicas);
    params.set(OverseerCollectionProcessor.MAX_SHARDS_PER_NODE, maxShardsPerNode);
    if (createNodeSetStr != null) params.set(OverseerCollectionProcessor.CREATE_NODE_SET, createNodeSetStr);

    int clientIndex = random().nextInt(2);
    List<Integer> list = new ArrayList<Integer>();
    list.add(numShards);
    list.add(numReplicas);
    collectionInfos.put(collectionName, list);
    params.set("name", collectionName);
    SolrRequest request = new QueryRequest(params);
    request.setPath("/admin/collections");
  
    if (client == null) {
      final String baseUrl = ((HttpSolrServer) clients.get(clientIndex)).getBaseURL().substring(
          0,
          ((HttpSolrServer) clients.get(clientIndex)).getBaseURL().length()
              - DEFAULT_COLLECTION.length() - 1);
      
      createNewSolrServer("", baseUrl).request(request);
    } else {
      client.request(request);
    }
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

  private String checkCollectionExpectations(String collectionName, List<Integer> numShardsNumReplicaList, List<String> nodesAllowedToRunShards) {
    ClusterState clusterState = getCommonCloudSolrServer().getZkStateReader().getClusterState();
    
    int expectedSlices = numShardsNumReplicaList.get(0);
    // The Math.min thing is here, because we expect replication-factor to be reduced to if there are not enough live nodes to spread all shards of a collection over different nodes
    int expectedShardsPerSlice = numShardsNumReplicaList.get(1);
    int expectedTotalShards = expectedSlices * expectedShardsPerSlice;
    
      Map<String,DocCollection> collections = clusterState
          .getCollectionStates();
      if (collections.containsKey(collectionName)) {
        Map<String,Slice> slices = collections.get(collectionName).getSlicesMap();
        // did we find expectedSlices slices/shards?
      if (slices.size() != expectedSlices) {
        return "Found new collection " + collectionName + ", but mismatch on number of slices. Expected: " + expectedSlices + ", actual: " + slices.size();
      }
      int totalShards = 0;
      for (String sliceName : slices.keySet()) {
        for (Replica replica : slices.get(sliceName).getReplicas()) {
          if (nodesAllowedToRunShards != null && !nodesAllowedToRunShards.contains(replica.getStr(ZkStateReader.NODE_NAME_PROP))) {
            return "Shard " + replica.getName() + " created on node " + replica.getStr(ZkStateReader.NODE_NAME_PROP) + " not allowed to run shards for the created collection " + collectionName;
          }
        }
        totalShards += slices.get(sliceName).getReplicas().size();
      }
      if (totalShards != expectedTotalShards) {
        return "Found new collection " + collectionName + " with correct number of slices, but mismatch on number of shards. Expected: " + expectedTotalShards + ", actual: " + totalShards; 
        }
      return null;
    } else {
      return "Could not find new collection " + collectionName;
    }
  }
  
  private void checkForCollection(String collectionName, List<Integer> numShardsNumReplicaList, List<String> nodesAllowedToRunShards)
      throws Exception {
    // check for an expectedSlices new collection - we poll the state
    long timeoutAt = System.currentTimeMillis() + 120000;
    boolean success = false;
    String checkResult = "Didnt get to perform a single check";
    while (System.currentTimeMillis() < timeoutAt) {
      checkResult = checkCollectionExpectations(collectionName, numShardsNumReplicaList, nodesAllowedToRunShards);
      if (checkResult == null) {
        success = true;
        break;
      }
      Thread.sleep(500);
    }
    if (!success) {
      super.printLayout();
      fail(checkResult);
      }
    }

  private void checkCollectionIsNotCreated(String collectionName)
    throws Exception {
    // TODO: REMOVE THIS SLEEP WHEN WE HAVE COLLECTION API RESPONSES
    Thread.sleep(10000);
    assertFalse(collectionName + " not supposed to exist", getCommonCloudSolrServer().getZkStateReader().getClusterState().getCollections().contains(collectionName));
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
  
  protected SolrInputDocument getDoc(Object... fields) throws Exception {
    SolrInputDocument doc = new SolrInputDocument();
    addFields(doc, fields);
    return doc;
  }
  
  protected SolrServer createNewSolrServer(String collection, String baseUrl) {
    try {
      // setup the server...
      HttpSolrServer s = new HttpSolrServer(baseUrl + "/" + collection);
      s.setConnectionTimeout(DEFAULT_CONNECTION_TIMEOUT);
      s.setDefaultMaxConnectionsPerHost(100);
      s.setMaxTotalConnections(100);
      return s;
    }
    catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  volatile CloudSolrServer commondCloudSolrServer;
  private CloudSolrServer getCommonCloudSolrServer() {
    if (commondCloudSolrServer == null) {
      synchronized(this) {
        try {
          commondCloudSolrServer = new CloudSolrServer(zkServer.getZkAddress());
          commondCloudSolrServer.setDefaultCollection(DEFAULT_COLLECTION);
          commondCloudSolrServer.connect();
        } catch (MalformedURLException e) {
          throw new RuntimeException(e);
        }
      }
    }
    return commondCloudSolrServer;
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
    if (commondCloudSolrServer != null) {
      commondCloudSolrServer.shutdown();
    }
    System.clearProperty("numShards");
    System.clearProperty("zkHost");
    System.clearProperty("solr.xml.persist");
    
    // insurance
    DirectUpdateHandler2.commitOnClose = true;
  }
}
