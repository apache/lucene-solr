package org.apache.solr.client.solrj.impl;

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

import com.carrotsearch.randomizedtesting.annotations.Seed;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.AbstractFullDistribZkTestBase;
import org.apache.solr.cloud.AbstractZkTestCase;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import static org.apache.solr.cloud.OverseerCollectionProcessor.NUM_SLICES;
import static org.apache.solr.common.cloud.ZkNodeProps.makeMap;
import static org.apache.solr.common.cloud.ZkStateReader.MAX_SHARDS_PER_NODE;
import static org.apache.solr.common.cloud.ZkStateReader.REPLICATION_FACTOR;


/**
 * This test would be faster if we simulated the zk state instead.
 */
@Slow
@Seed("1CB6EE5E0046C651:94E2D184AEBAABA9")
public class CloudSolrClientTest extends AbstractFullDistribZkTestBase {
  static Logger log = LoggerFactory.getLogger(CloudSolrClientTest.class);

  private static final String SOLR_HOME = getFile("solrj" + File.separator + "solr").getAbsolutePath();

  @BeforeClass
  public static void beforeSuperClass() {
      AbstractZkTestCase.SOLRHOME = new File(SOLR_HOME());
  }
  
  @AfterClass
  public static void afterSuperClass() {
    
  }
  
  protected String getCloudSolrConfig() {
    return "solrconfig.xml";
  }
  
  @Override
  public String getSolrHome() {
    return SOLR_HOME;
  }
  
  public static String SOLR_HOME() {
    return SOLR_HOME;
  }
  
  @Override
  public void distribSetUp() throws Exception {
    super.distribSetUp();
    // we expect this time of exception as shards go up and down...
    //ignoreException(".*");
    
    System.setProperty("numShards", Integer.toString(sliceCount));
  }
  
  public CloudSolrClientTest() {
    super();
    sliceCount = 2;
    fixShardCount(3);
  }

  @Test
  public void test() throws Exception {
    checkCollectionParameters();
    allTests();
    stateVersionParamTest();
    customHttpClientTest();
    testOverwriteOption();
    preferLocalShardsTest();
  }

  private void testOverwriteOption() throws Exception, SolrServerException,
      IOException {
    String collectionName = "overwriteCollection";
    createCollection(collectionName, controlClientCloud, 1, 1);
    waitForRecoveriesToFinish(collectionName, false);
    try (CloudSolrClient cloudClient = createCloudClient(collectionName)) {
      SolrInputDocument doc1 = new SolrInputDocument();
      doc1.addField(id, "0");
      doc1.addField("a_t", "hello1");
      SolrInputDocument doc2 = new SolrInputDocument();
      doc2.addField(id, "0");
      doc2.addField("a_t", "hello2");
      
      UpdateRequest request = new UpdateRequest();
      request.add(doc1);
      request.add(doc2);
      request.setAction(AbstractUpdateRequest.ACTION.COMMIT, false, false);
      NamedList<Object> response = cloudClient.request(request);
      QueryResponse resp = cloudClient.query(new SolrQuery("*:*"));
      
      assertEquals("There should be one document because overwrite=true", 1, resp.getResults().getNumFound());
      
      doc1 = new SolrInputDocument();
      doc1.addField(id, "1");
      doc1.addField("a_t", "hello1");
      doc2 = new SolrInputDocument();
      doc2.addField(id, "1");
      doc2.addField("a_t", "hello2");
      
      request = new UpdateRequest();
      // overwrite=false
      request.add(doc1, false);
      request.add(doc2, false);
      request.setAction(AbstractUpdateRequest.ACTION.COMMIT, false, false);
      response = cloudClient.request(request);
      
      resp = cloudClient.query(new SolrQuery("*:*"));

      assertEquals("There should be 3 documents because there should be two id=1 docs due to overwrite=false", 3, resp.getResults().getNumFound());
    }
  }

  private void allTests() throws Exception {

    String collectionName = "clientTestExternColl";
    createCollection(collectionName, controlClientCloud, 2, 2);
    waitForRecoveriesToFinish(collectionName, false);
    CloudSolrClient cloudClient = createCloudClient(collectionName);

    assertNotNull(cloudClient);
    
    handle.clear();
    handle.put("timestamp", SKIPVAL);
    
    waitForThingsToLevelOut(30);

    controlClient.deleteByQuery("*:*");
    cloudClient.deleteByQuery("*:*");


    controlClient.commit();
    this.cloudClient.commit();

    SolrInputDocument doc1 = new SolrInputDocument();
    doc1.addField(id, "0");
    doc1.addField("a_t", "hello1");
    SolrInputDocument doc2 = new SolrInputDocument();
    doc2.addField(id, "2");
    doc2.addField("a_t", "hello2");
    
    UpdateRequest request = new UpdateRequest();
    request.add(doc1);
    request.add(doc2);
    request.setAction(AbstractUpdateRequest.ACTION.COMMIT, false, false);
    
    // Test single threaded routed updates for UpdateRequest
    NamedList<Object> response = cloudClient.request(request);
    CloudSolrClient.RouteResponse rr = (CloudSolrClient.RouteResponse) response;
    Map<String,LBHttpSolrClient.Req> routes = rr.getRoutes();
    Iterator<Map.Entry<String,LBHttpSolrClient.Req>> it = routes.entrySet()
        .iterator();
    while (it.hasNext()) {
      Map.Entry<String,LBHttpSolrClient.Req> entry = it.next();
      String url = entry.getKey();
      UpdateRequest updateRequest = (UpdateRequest) entry.getValue()
          .getRequest();
      SolrInputDocument doc = updateRequest.getDocuments().get(0);
      String id = doc.getField("id").getValue().toString();
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.add("q", "id:" + id);
      params.add("distrib", "false");
      QueryRequest queryRequest = new QueryRequest(params);
      try (HttpSolrClient solrClient = new HttpSolrClient(url)) {
        QueryResponse queryResponse = queryRequest.process(solrClient);
        SolrDocumentList docList = queryResponse.getResults();
        assertTrue(docList.getNumFound() == 1);
      }
    }
    
    // Test the deleteById routing for UpdateRequest
    
    UpdateRequest delRequest = new UpdateRequest();
    delRequest.deleteById("0");
    delRequest.deleteById("2");
    delRequest.setAction(AbstractUpdateRequest.ACTION.COMMIT, false, false);
    cloudClient.request(delRequest);
    ModifiableSolrParams qParams = new ModifiableSolrParams();
    qParams.add("q", "*:*");
    QueryRequest qRequest = new QueryRequest(qParams);
    QueryResponse qResponse = qRequest.process(cloudClient);
    SolrDocumentList docs = qResponse.getResults();
    assertTrue(docs.getNumFound() == 0);
    
    // Test Multi-Threaded routed updates for UpdateRequest
    try (CloudSolrClient threadedClient = new CloudSolrClient(zkServer.getZkAddress())) {
      threadedClient.setParallelUpdates(true);
      threadedClient.setDefaultCollection(collectionName);
      response = threadedClient.request(request);
      rr = (CloudSolrClient.RouteResponse) response;
      routes = rr.getRoutes();
      it = routes.entrySet()
          .iterator();
      while (it.hasNext()) {
        Map.Entry<String,LBHttpSolrClient.Req> entry = it.next();
        String url = entry.getKey();
        UpdateRequest updateRequest = (UpdateRequest) entry.getValue()
            .getRequest();
        SolrInputDocument doc = updateRequest.getDocuments().get(0);
        String id = doc.getField("id").getValue().toString();
        ModifiableSolrParams params = new ModifiableSolrParams();
        params.add("q", "id:" + id);
        params.add("distrib", "false");
        QueryRequest queryRequest = new QueryRequest(params);
        try (HttpSolrClient solrClient = new HttpSolrClient(url)) {
          QueryResponse queryResponse = queryRequest.process(solrClient);
          SolrDocumentList docList = queryResponse.getResults();
          assertTrue(docList.getNumFound() == 1);
        }
      }
    }

    // Test that queries with _route_ params are routed by the client

    // Track request counts on each node before query calls
    ClusterState clusterState = cloudClient.getZkStateReader().getClusterState();
    DocCollection col = clusterState.getCollection(collectionName);
    Map<String, Long> requestCountsMap = Maps.newHashMap();
    for (Slice slice : col.getSlices()) {
      for (Replica replica : slice.getReplicas()) {
        String baseURL = (String) replica.get(ZkStateReader.BASE_URL_PROP);
        requestCountsMap.put(baseURL, getNumRequests(baseURL,collectionName));
      }
    }

    // Collect the base URLs of the replicas of shard that's expected to be hit
    DocRouter router = col.getRouter();
    Collection<Slice> expectedSlices = router.getSearchSlicesSingle("0", null, col);
    Set<String> expectedBaseURLs = Sets.newHashSet();
    for (Slice expectedSlice : expectedSlices) {
      for (Replica replica : expectedSlice.getReplicas()) {
        String baseURL = (String) replica.get(ZkStateReader.BASE_URL_PROP);
        expectedBaseURLs.add(baseURL);
      }
    }

    assertTrue("expected urls is not fewer than all urls! expected=" + expectedBaseURLs
        + "; all=" + requestCountsMap.keySet(),
        expectedBaseURLs.size() < requestCountsMap.size());

    // Calculate a number of shard keys that route to the same shard.
    int n;
    if (TEST_NIGHTLY) {
      n = random().nextInt(999) + 2;
    } else {
      n = random().nextInt(9) + 2;
    }
    
    List<String> sameShardRoutes = Lists.newArrayList();
    sameShardRoutes.add("0");
    for (int i = 1; i < n; i++) {
      String shardKey = Integer.toString(i);
      Collection<Slice> slices = router.getSearchSlicesSingle(shardKey, null, col);
      log.info("Expected Slices {}", slices);
      if (expectedSlices.equals(slices)) {
        sameShardRoutes.add(shardKey);
      }
    }

    assertTrue(sameShardRoutes.size() > 1);

    // Do N queries with _route_ parameter to the same shard
    for (int i = 0; i < n; i++) {
      ModifiableSolrParams solrParams = new ModifiableSolrParams();
      solrParams.set(CommonParams.Q, "*:*");
      solrParams.set(ShardParams._ROUTE_, sameShardRoutes.get(random().nextInt(sameShardRoutes.size())));
      log.info("output  : {}" ,cloudClient.query(solrParams));
    }

    // Request counts increase from expected nodes should aggregate to 1000, while there should be
    // no increase in unexpected nodes.
    int increaseFromExpectedUrls = 0;
    int increaseFromUnexpectedUrls = 0;
    Map<String, Long> numRequestsToUnexpectedUrls = Maps.newHashMap();
    for (Slice slice : col.getSlices()) {
      for (Replica replica : slice.getReplicas()) {
        String baseURL = (String) replica.get(ZkStateReader.BASE_URL_PROP);

        Long prevNumRequests = requestCountsMap.get(baseURL);
        Long curNumRequests = getNumRequests(baseURL, collectionName);

        long delta = curNumRequests - prevNumRequests;
        if (expectedBaseURLs.contains(baseURL)) {
          increaseFromExpectedUrls += delta;
        } else {
          increaseFromUnexpectedUrls += delta;
          numRequestsToUnexpectedUrls.put(baseURL, delta);
        }
      }
    }

    assertEquals("Unexpected number of requests to expected URLs", n, increaseFromExpectedUrls);
    assertEquals("Unexpected number of requests to unexpected URLs: " + numRequestsToUnexpectedUrls,
        0, increaseFromUnexpectedUrls);

    controlClient.deleteByQuery("*:*");
    cloudClient.deleteByQuery("*:*");

    controlClient.commit();
    cloudClient.commit();
    cloudClient.close();
  }

  /**
   * Tests if the specification of 'preferLocalShards' in the query-params
   * limits the distributed query to locally hosted shards only
   */
  private void preferLocalShardsTest() throws Exception {

    String collectionName = "localShardsTestColl";

    int liveNodes = getCommonCloudSolrClient()
        .getZkStateReader().getClusterState().getLiveNodes().size();

    // For preferLocalShards to succeed in a test, every shard should have
    // all its cores on the same node.
    // Hence the below configuration for our collection
    Map<String, Object> props = makeMap(
        REPLICATION_FACTOR, liveNodes,
        MAX_SHARDS_PER_NODE, liveNodes,
        NUM_SLICES, liveNodes);
    Map<String,List<Integer>> collectionInfos = new HashMap<String,List<Integer>>();
    createCollection(collectionInfos, collectionName, props, controlClientCloud);
    waitForRecoveriesToFinish(collectionName, false);

    CloudSolrClient cloudClient = createCloudClient(collectionName);
    assertNotNull(cloudClient);
    handle.clear();
    handle.put("timestamp", SKIPVAL);
    waitForThingsToLevelOut(30);

    // Remove any documents from previous test (if any)
    controlClient.deleteByQuery("*:*");
    cloudClient.deleteByQuery("*:*");
    controlClient.commit();
    cloudClient.commit();

    // Add some new documents
    SolrInputDocument doc1 = new SolrInputDocument();
    doc1.addField(id, "0");
    doc1.addField("a_t", "hello1");
    SolrInputDocument doc2 = new SolrInputDocument();
    doc2.addField(id, "2");
    doc2.addField("a_t", "hello2");
    SolrInputDocument doc3 = new SolrInputDocument();
    doc3.addField(id, "3");
    doc3.addField("a_t", "hello2");

    UpdateRequest request = new UpdateRequest();
    request.add(doc1);
    request.add(doc2);
    request.add(doc3);
    request.setAction(AbstractUpdateRequest.ACTION.COMMIT, false, false);

    // Run the actual test for 'preferLocalShards'
    queryWithPreferLocalShards(cloudClient, true, collectionName);

    // Cleanup
    controlClient.deleteByQuery("*:*");
    cloudClient.deleteByQuery("*:*");
    controlClient.commit();
    cloudClient.commit();
    cloudClient.close();
  }

  private void queryWithPreferLocalShards(CloudSolrClient cloudClient,
                                          boolean preferLocalShards,
                                          String collectionName)
      throws Exception
  {
    SolrQuery qRequest = new SolrQuery();
    qRequest.setQuery("*:*");

    ModifiableSolrParams qParams = new ModifiableSolrParams();
    qParams.add("preferLocalShards", Boolean.toString(preferLocalShards));
    qParams.add("shards.info", "true");
    qRequest.add(qParams);

    // CloudSolrClient sends the request to some node.
    // And since all the nodes are hosting cores from all shards, the
    // distributed query formed by this node will select cores from the
    // local shards only
    QueryResponse qResponse = cloudClient.query (qRequest);

    Object shardsInfo = qResponse.getResponse().get("shards.info");
    assertNotNull("Unable to obtain shards.info", shardsInfo);

    // Iterate over shards-info and check what cores responded
    SimpleOrderedMap<?> shardsInfoMap = (SimpleOrderedMap<?>)shardsInfo;
    Iterator<Map.Entry<String, ?>> itr = shardsInfoMap.asMap(100).entrySet().iterator();
    List<String> shardAddresses = new ArrayList<String>();
    while (itr.hasNext()) {
      Map.Entry<String, ?> e = itr.next();
      assertTrue("Did not find map-type value in shards.info", e.getValue() instanceof Map);
      String shardAddress = (String)((Map)e.getValue()).get("shardAddress");
      assertNotNull("shards.info did not return 'shardAddress' parameter", shardAddress);
      shardAddresses.add(shardAddress);
    }
    log.info("Shards giving the response: " + Arrays.toString(shardAddresses.toArray()));

    // Make sure the distributed queries were directed to a single node only
    if (preferLocalShards) {
      Set<Integer> ports = new HashSet<Integer>();
      for (String shardAddr: shardAddresses) {
        URL url = new URL (shardAddr);
        ports.add(url.getPort());
      }

      // This assertion would hold true as long as every shard has a core on each node
      assertTrue ("Response was not received from shards on a single node",
          shardAddresses.size() > 1 && ports.size()==1);
    }
  }

  private Long getNumRequests(String baseUrl, String collectionName) throws
      SolrServerException, IOException {

    NamedList<Object> resp;
    try (HttpSolrClient client = new HttpSolrClient(baseUrl + "/"+ collectionName)) {
      client.setConnectionTimeout(15000);
      client.setSoTimeout(60000);
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set("qt", "/admin/mbeans");
      params.set("stats", "true");
      params.set("key", "standard");
      params.set("cat", "QUERYHANDLER");
      // use generic request to avoid extra processing of queries
      QueryRequest req = new QueryRequest(params);
      resp = client.request(req);
    }
    return (Long) resp.findRecursive("solr-mbeans", "QUERYHANDLER",
        "standard", "stats", "requests");
  }
  
  @Override
  protected void indexr(Object... fields) throws Exception {
    SolrInputDocument doc = getDoc(fields);
    indexDoc(doc);
  }

  private void checkCollectionParameters() throws Exception {

    try (CloudSolrClient client = createCloudClient("multicollection1")) {

      createCollection("multicollection1", client, 2, 2);
      createCollection("multicollection2", client, 2, 2);
      waitForRecoveriesToFinish("multicollection1", false);
      waitForRecoveriesToFinish("multicollection2", false);

      List<SolrInputDocument> docs = new ArrayList<>(3);
      for (int i = 0; i < 3; i++) {
        SolrInputDocument doc = new SolrInputDocument();
        doc.addField(id, Integer.toString(i));
        doc.addField("a_t", "hello");
        docs.add(doc);
      }

      client.add(docs);     // default - will add them to multicollection1
      client.commit();

      ModifiableSolrParams queryParams = new ModifiableSolrParams();
      queryParams.add("q", "*:*");
      assertEquals(3, client.query(queryParams).getResults().size());
      assertEquals(0, client.query("multicollection2", queryParams).getResults().size());

      SolrQuery query = new SolrQuery("*:*");
      query.set("collection", "multicollection2");
      assertEquals(0, client.query(query).getResults().size());

      client.add("multicollection2", docs);
      client.commit("multicollection2");

      assertEquals(3, client.query("multicollection2", queryParams).getResults().size());

    }

  }

  private void stateVersionParamTest() throws Exception {

    try (CloudSolrClient client = createCloudClient(null)) {
      String collectionName = "checkStateVerCol";
      createCollection(collectionName, client, 1, 3);
      waitForRecoveriesToFinish(collectionName, false);
      DocCollection coll = client.getZkStateReader().getClusterState().getCollection(collectionName);
      Replica r = coll.getSlices().iterator().next().getReplicas().iterator().next();

      SolrQuery q = new SolrQuery().setQuery("*:*");
      HttpSolrClient.RemoteSolrException sse = null;

      try (HttpSolrClient solrClient = new HttpSolrClient(r.getStr(ZkStateReader.BASE_URL_PROP) + "/"+collectionName)) {

        log.info("should work query, result {}", solrClient.query(q));
        //no problem
        q.setParam(CloudSolrClient.STATE_VERSION, collectionName + ":" + coll.getZNodeVersion());
        log.info("2nd query , result {}", solrClient.query(q));
        //no error yet good

        q.setParam(CloudSolrClient.STATE_VERSION, collectionName + ":" + (coll.getZNodeVersion() - 1)); //an older version expect error

        QueryResponse rsp = solrClient.query(q);
        Map m = (Map) rsp.getResponse().get(CloudSolrClient.STATE_VERSION, rsp.getResponse().size()-1);
        assertNotNull("Expected an extra information from server with the list of invalid collection states", m);
        assertNotNull(m.get(collectionName));
      }

      //now send the request to another node that does not serve the collection

      Set<String> allNodesOfColl = new HashSet<>();
      for (Slice slice : coll.getSlices()) {
        for (Replica replica : slice.getReplicas()) {
          allNodesOfColl.add(replica.getStr(ZkStateReader.BASE_URL_PROP));
        }
      }
      String theNode = null;
      Set<String> liveNodes = client.getZkStateReader().getClusterState().getLiveNodes();
      for (String s : liveNodes) {
        String n = client.getZkStateReader().getBaseUrlForNodeName(s);
        if(!allNodesOfColl.contains(n)){
          theNode = n;
          break;
        }
      }
      log.info("the node which does not serve this collection{} ",theNode);
      assertNotNull(theNode);

      try (SolrClient solrClient = new HttpSolrClient(theNode + "/"+collectionName)) {

        q.setParam(CloudSolrClient.STATE_VERSION, collectionName + ":" + (coll.getZNodeVersion()-1));
        try {
          QueryResponse rsp = solrClient.query(q);
          log.info("error was expected");
        } catch (HttpSolrClient.RemoteSolrException e) {
          sse = e;
        }
        assertNotNull(sse);
        assertEquals(" Error code should be 510", SolrException.ErrorCode.INVALID_STATE.code, sse.code());
      }
    }

  }

  public void testShutdown() throws IOException {
    try (CloudSolrClient client = new CloudSolrClient("[ff01::114]:33332")) {
      client.setZkConnectTimeout(100);
      client.connect();
      fail("Expected exception");
    } catch (SolrException e) {
      assertTrue(e.getCause() instanceof TimeoutException);
    }
  }

  public void testWrongZkChrootTest() throws IOException {
    try (CloudSolrClient client = new CloudSolrClient(zkServer.getZkAddress() + "/xyz/foo")) {
      client.setDefaultCollection(DEFAULT_COLLECTION);
      client.setZkClientTimeout(1000 * 60);
      client.connect();
      fail("Expected exception");
    } catch(SolrException e) {
      assertTrue(e.getCause() instanceof KeeperException);
    }
    // see SOLR-6146 - this test will fail by virtue of the zkClient tracking performed
    // in the afterClass method of the base class
  }

  public void customHttpClientTest() throws IOException {

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(HttpClientUtil.PROP_SO_TIMEOUT, 1000);

    try (CloseableHttpClient client = HttpClientUtil.createClient(params);
         CloudSolrClient solrClient = new CloudSolrClient(zkServer.getZkAddress(), client)) {

      assertTrue(solrClient.getLbClient().getHttpClient() == client);

    }
  }
}
