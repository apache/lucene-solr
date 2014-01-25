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

import static org.apache.solr.cloud.OverseerCollectionProcessor.MAX_SHARDS_PER_NODE;
import static org.apache.solr.cloud.OverseerCollectionProcessor.NUM_SLICES;
import static org.apache.solr.cloud.OverseerCollectionProcessor.REPLICATION_FACTOR;
import static org.apache.solr.cloud.OverseerCollectionProcessor.ROUTER;
import static org.apache.solr.cloud.OverseerCollectionProcessor.SHARDS_PROP;
import static org.apache.solr.common.params.ShardParams._ROUTE_;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
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

import org.apache.lucene.util.Constants;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.lucene.util._TestUtil;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.ImplicitDocRouter;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams.CollectionAction;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.update.DirectUpdateHandler2;
import org.apache.solr.util.DefaultSolrThreadFactory;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;

/**
 * Tests the Custom Sharding API.
 */
@Slow
public class CustomCollectionTest extends AbstractFullDistribZkTestBase {

  private static final String DEFAULT_COLLECTION = "collection1";
  private static final boolean DEBUG = false;

  ThreadPoolExecutor executor = new ThreadPoolExecutor(0,
      Integer.MAX_VALUE, 5, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(),
      new DefaultSolrThreadFactory("testExecutor"));

  CompletionService<Object> completionService;
  Set<Future<Object>> pending;

  @BeforeClass
  public static void beforeThisClass2() throws Exception {
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


  public CustomCollectionTest() {
    fixShardCount = true;

    sliceCount = 2;
    shardCount = 4;
    completionService = new ExecutorCompletionService<Object>(executor);
    pending = new HashSet<Future<Object>>();
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
    testCustomCollectionsAPI();
    testRouteFieldForHashRouter();
    testCreateShardRepFactor();
    if (DEBUG) {
      super.printLayout();
    }
  }


  private void testCustomCollectionsAPI() throws Exception {
    String COLL_PREFIX = "implicitcoll";

    // TODO: fragile - because we dont pass collection.confName, it will only
    // find a default if a conf set with a name matching the collection name is found, or
    // if there is only one conf set. That and the fact that other tests run first in this
    // env make this pretty fragile

    // create new collections rapid fire
    Map<String,List<Integer>> collectionInfos = new HashMap<String,List<Integer>>();
    int replicationFactor = _TestUtil.nextInt(random(), 0, 3) + 2;

    int cnt = random().nextInt(6) + 1;

    for (int i = 0; i < cnt; i++) {
      int numShards = 3;
      int maxShardsPerNode = ((((numShards+1) * replicationFactor) / getCommonCloudSolrServer()
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
          client = createCloudClient(COLL_PREFIX + i);
        }

        Map<String, Object> props = ZkNodeProps.makeMap(
            "router.name", ImplicitDocRouter.NAME,
            REPLICATION_FACTOR, replicationFactor,
            MAX_SHARDS_PER_NODE, maxShardsPerNode,
            SHARDS_PROP,"a,b,c");

        createCollection(collectionInfos, COLL_PREFIX + i,props,client);
      } finally {
        if (client != null) client.shutdown();
      }
    }

    Set<Entry<String,List<Integer>>> collectionInfosEntrySet = collectionInfos.entrySet();
    for (Entry<String,List<Integer>> entry : collectionInfosEntrySet) {
      String collection = entry.getKey();
      List<Integer> list = entry.getValue();
      checkForCollection(collection, list, null);

      String url = getUrlFromZk(getCommonCloudSolrServer().getZkStateReader().getClusterState(), collection);

      HttpSolrServer collectionClient = new HttpSolrServer(url);

      // poll for a second - it can take a moment before we are ready to serve
      waitForNon403or404or503(collectionClient);
    }
    ZkStateReader zkStateReader = getCommonCloudSolrServer().getZkStateReader();
    for (int j = 0; j < cnt; j++) {
      waitForRecoveriesToFinish(COLL_PREFIX + j, zkStateReader, false);
    }

    ClusterState clusterState = zkStateReader.getClusterState();

    DocCollection coll = clusterState.getCollection(COLL_PREFIX + 0);
    assertEquals("implicit", ((Map)coll.get(ROUTER)).get("name") );
    assertNotNull(coll.getStr(REPLICATION_FACTOR));
    assertNotNull(coll.getStr(MAX_SHARDS_PER_NODE));
    assertNull("A shard of a Collection configured with implicit router must have null range",
        coll.getSlice("a").getRange());

    List<String> collectionNameList = new ArrayList<String>();
    collectionNameList.addAll(collectionInfos.keySet());
    log.info("Collections created : "+collectionNameList );

    String collectionName = collectionNameList.get(random().nextInt(collectionNameList.size()));

    String url = getUrlFromZk(getCommonCloudSolrServer().getZkStateReader().getClusterState(), collectionName);

    HttpSolrServer collectionClient = new HttpSolrServer(url);


    // lets try and use the solrj client to index a couple documents

    collectionClient.add(getDoc(id, 6, i1, -600, tlong, 600, t1,
        "humpty dumpy sat on a wall", _ROUTE_,"a"));

    collectionClient.add(getDoc(id, 7, i1, -600, tlong, 600, t1,
        "humpty dumpy3 sat on a walls", _ROUTE_,"a"));

    collectionClient.add(getDoc(id, 8, i1, -600, tlong, 600, t1,
        "humpty dumpy2 sat on a walled", _ROUTE_,"a"));

    collectionClient.commit();

    assertEquals(3, collectionClient.query(new SolrQuery("*:*")).getResults().getNumFound());
    assertEquals(0, collectionClient.query(new SolrQuery("*:*").setParam(_ROUTE_,"b")).getResults().getNumFound());
    assertEquals(3, collectionClient.query(new SolrQuery("*:*").setParam(_ROUTE_,"a")).getResults().getNumFound());

    collectionClient.deleteByQuery("*:*");
    collectionClient.commit(true,true);
    assertEquals(0, collectionClient.query(new SolrQuery("*:*")).getResults().getNumFound());

    UpdateRequest up = new UpdateRequest();
    up.setParam(_ROUTE_, "c");
    up.setParam("commit","true");

    up.add(getDoc(id, 9, i1, -600, tlong, 600, t1,
        "humpty dumpy sat on a wall"));
    up.add(getDoc(id, 10, i1, -600, tlong, 600, t1,
        "humpty dumpy3 sat on a walls"));
    up.add(getDoc(id, 11, i1, -600, tlong, 600, t1,
        "humpty dumpy2 sat on a walled"));

    collectionClient.request(up);

    assertEquals(3, collectionClient.query(new SolrQuery("*:*")).getResults().getNumFound());
    assertEquals(0, collectionClient.query(new SolrQuery("*:*").setParam(_ROUTE_,"a")).getResults().getNumFound());
    assertEquals(3, collectionClient.query(new SolrQuery("*:*").setParam(_ROUTE_,"c")).getResults().getNumFound());

    //Testing CREATESHARD
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("action", CollectionAction.CREATESHARD.toString());
    params.set("collection", collectionName);
    params.set("shard", "x");
    SolrRequest request = new QueryRequest(params);
    request.setPath("/admin/collections");
    createNewSolrServer("", getBaseUrl((HttpSolrServer) clients.get(0))).request(request);
    waitForCollection(zkStateReader,collectionName,4);
    //wait for all the replicas to become active
    int attempts = 0;
    while(true){
      if(attempts>30 ) fail("Not enough active replicas in the shard 'x'");
      attempts++;
      int activeReplicaCount = 0;
      for (Replica x : zkStateReader.getClusterState().getCollection(collectionName).getSlice("x").getReplicas()) {
        if("active".equals(x.getStr("state"))) activeReplicaCount++;
      }
      Thread.sleep(500);
      if(activeReplicaCount >= replicationFactor) break;
    }
    log.info(zkStateReader.getClusterState().toString());

    collectionClient.add(getDoc(id, 66, i1, -600, tlong, 600, t1,
        "humpty dumpy sat on a wall", _ROUTE_,"x"));
    collectionClient.commit();
    assertEquals(1, collectionClient.query(new SolrQuery("*:*").setParam(_ROUTE_,"x")).getResults().getNumFound());


    int numShards = 4;
    replicationFactor = _TestUtil.nextInt(random(), 0, 3) + 2;
    int maxShardsPerNode = (((numShards * replicationFactor) / getCommonCloudSolrServer()
        .getZkStateReader().getClusterState().getLiveNodes().size())) + 1;


    CloudSolrServer client = null;
    String shard_fld = "shard_s";
    try {
      client = createCloudClient(null);
      Map<String, Object> props = ZkNodeProps.makeMap(
          "router.name", ImplicitDocRouter.NAME,
          REPLICATION_FACTOR, replicationFactor,
          MAX_SHARDS_PER_NODE, maxShardsPerNode,
          SHARDS_PROP,"a,b,c,d",
          "router.field", shard_fld);

      collectionName = COLL_PREFIX + "withShardField";
      createCollection(collectionInfos, collectionName,props,client);
    } finally {
      if (client != null) client.shutdown();
    }

    List<Integer> list = collectionInfos.get(collectionName);
    checkForCollection(collectionName, list, null);


    url = getUrlFromZk(getCommonCloudSolrServer().getZkStateReader().getClusterState(), collectionName);

    collectionClient = new HttpSolrServer(url);

    // poll for a second - it can take a moment before we are ready to serve
    waitForNon403or404or503(collectionClient);




    collectionClient = new HttpSolrServer(url);


    // lets try and use the solrj client to index a couple documents

    collectionClient.add(getDoc(id, 6, i1, -600, tlong, 600, t1,
        "humpty dumpy sat on a wall", shard_fld,"a"));

    collectionClient.add(getDoc(id, 7, i1, -600, tlong, 600, t1,
        "humpty dumpy3 sat on a walls", shard_fld,"a"));

    collectionClient.add(getDoc(id, 8, i1, -600, tlong, 600, t1,
        "humpty dumpy2 sat on a walled", shard_fld,"a"));

    collectionClient.commit();

    assertEquals(3, collectionClient.query(new SolrQuery("*:*")).getResults().getNumFound());
    assertEquals(0, collectionClient.query(new SolrQuery("*:*").setParam(_ROUTE_,"b")).getResults().getNumFound());
    //TODO debug the following case
    assertEquals(3, collectionClient.query(new SolrQuery("*:*").setParam(_ROUTE_, "a")).getResults().getNumFound());


  }

  private void testRouteFieldForHashRouter()throws Exception{
    String collectionName = "routeFieldColl";
    int numShards = 4;
    int replicationFactor = 2;
    int maxShardsPerNode = (((numShards * replicationFactor) / getCommonCloudSolrServer()
        .getZkStateReader().getClusterState().getLiveNodes().size())) + 1;

    HashMap<String, List<Integer>> collectionInfos = new HashMap<String, List<Integer>>();
    CloudSolrServer client = null;
    String shard_fld = "shard_s";
    try {
      client = createCloudClient(null);
      Map<String, Object> props = ZkNodeProps.makeMap(
          REPLICATION_FACTOR, replicationFactor,
          MAX_SHARDS_PER_NODE, maxShardsPerNode,
          NUM_SLICES,numShards,
          "router.field", shard_fld);

      createCollection(collectionInfos, collectionName,props,client);
    } finally {
      if (client != null) client.shutdown();
    }

    List<Integer> list = collectionInfos.get(collectionName);
    checkForCollection(collectionName, list, null);


    String url = getUrlFromZk(getCommonCloudSolrServer().getZkStateReader().getClusterState(), collectionName);

    HttpSolrServer collectionClient = new HttpSolrServer(url);

    // poll for a second - it can take a moment before we are ready to serve
    waitForNon403or404or503(collectionClient);


    collectionClient = new HttpSolrServer(url);


    // lets try and use the solrj client to index a couple documents

    collectionClient.add(getDoc(id, 6, i1, -600, tlong, 600, t1,
        "humpty dumpy sat on a wall", shard_fld,"a"));

    collectionClient.add(getDoc(id, 7, i1, -600, tlong, 600, t1,
        "humpty dumpy3 sat on a walls", shard_fld,"a"));

    collectionClient.add(getDoc(id, 8, i1, -600, tlong, 600, t1,
        "humpty dumpy2 sat on a walled", shard_fld,"a"));

    collectionClient.commit();

    assertEquals(3, collectionClient.query(new SolrQuery("*:*")).getResults().getNumFound());
    //TODO debug the following case
    assertEquals(3, collectionClient.query(new SolrQuery("*:*").setParam(_ROUTE_, "a")).getResults().getNumFound());

    collectionClient.deleteByQuery("*:*");
    collectionClient.commit();

    collectionClient.add (getDoc( id,100,shard_fld, "b!doc1"));
    collectionClient.commit();
    assertEquals(1, collectionClient.query(new SolrQuery("*:*").setParam(_ROUTE_, "b!")).getResults().getNumFound());

  }

  private void testCreateShardRepFactor() throws Exception  {
    String collectionName = "testCreateShardRepFactor";
    HashMap<String, List<Integer>> collectionInfos = new HashMap<String, List<Integer>>();
    CloudSolrServer client = null;
    try {
      client = createCloudClient(null);
      Map<String, Object> props = ZkNodeProps.makeMap(
          REPLICATION_FACTOR, 1,
          MAX_SHARDS_PER_NODE, 5,
          NUM_SLICES, 2,
          "shards", "a,b",
          "router.name", "implicit");

      createCollection(collectionInfos, collectionName, props, client);
    } finally {
      if (client != null) client.shutdown();
    }
    ZkStateReader zkStateReader = getCommonCloudSolrServer().getZkStateReader();
    waitForRecoveriesToFinish(collectionName, zkStateReader, false);

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("action", CollectionAction.CREATESHARD.toString());
    params.set("collection", collectionName);
    params.set("shard", "x");
    SolrRequest request = new QueryRequest(params);
    request.setPath("/admin/collections");
    createNewSolrServer("", getBaseUrl((HttpSolrServer) clients.get(0))).request(request);

    waitForRecoveriesToFinish(collectionName, zkStateReader, false);

    int replicaCount = 0;
    int attempts = 0;
    while (true) {
      if (attempts > 30) fail("Not enough active replicas in the shard 'x'");
      zkStateReader.updateClusterState(true);
      attempts++;
      replicaCount = zkStateReader.getClusterState().getSlice(collectionName, "x").getReplicas().size();
      if (replicaCount >= 1) break;
      Thread.sleep(500);
    }

    assertEquals("CREATESHARD API created more than replicationFactor number of replicas", 1, replicaCount);
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
