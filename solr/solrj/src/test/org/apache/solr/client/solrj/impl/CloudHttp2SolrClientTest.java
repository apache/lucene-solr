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
package org.apache.solr.client.solrj.impl;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.RequestStatusState;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.cloud.AbstractDistribZkTestBase;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrDocument;
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
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.handler.admin.CollectionsHandler;
import org.apache.solr.handler.admin.ConfigSetsHandler;
import org.apache.solr.handler.admin.CoreAdminHandler;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.client.solrj.impl.BaseCloudSolrClient.*;


/**
 * This test would be faster if we simulated the zk state instead.
 */
@Slow
public class CloudHttp2SolrClientTest extends SolrCloudTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  private static final String COLLECTION = "collection1";
  private static final String COLLECTION2 = "2nd_collection";

  private static final String id = "id";

  private static final int TIMEOUT = 30;
  private static final int NODE_COUNT = 3;

  private static CloudHttp2SolrClient httpBasedCloudSolrClient = null;
  private static CloudHttp2SolrClient zkBasedCloudSolrClient = null;

  @Before
  public void setupCluster() throws Exception {
    configureCluster(NODE_COUNT)
        .addConfig("conf", getFile("solrj").toPath().resolve("solr").resolve("configsets").resolve("streaming").resolve("conf"))
        .configure();

    final List<String> solrUrls = new ArrayList<>();
    solrUrls.add(cluster.getJettySolrRunner(0).getBaseUrl().toString());
    httpBasedCloudSolrClient = new CloudHttp2SolrClient.Builder(solrUrls).build();
    zkBasedCloudSolrClient = new CloudHttp2SolrClient.Builder(Collections.singletonList(cluster.getZkServer().getZkAddress()), Optional.empty()).build();
  }

  
  @After 
  public void tearDown() throws Exception {
    if (httpBasedCloudSolrClient != null) {
      try {
        httpBasedCloudSolrClient.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    if (zkBasedCloudSolrClient != null) {
      try {
        zkBasedCloudSolrClient.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    
    shutdownCluster();
    super.tearDown();
  }

  /**
   * Randomly return the cluster's ZK based CSC, or HttpClusterProvider based CSC.
   */
  private CloudHttp2SolrClient getRandomClient() {
//    return random().nextBoolean()? zkBasedCloudSolrClient : httpBasedCloudSolrClient;
    return httpBasedCloudSolrClient;
  }

  @Test
  public void testParallelUpdateQTime() throws Exception {
    CollectionAdminRequest.createCollection(COLLECTION, "conf", 2, 1).process(cluster.getSolrClient());
    cluster.waitForActiveCollection(COLLECTION, 2, 2);
    UpdateRequest req = new UpdateRequest();
    for (int i=0; i<10; i++)  {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", String.valueOf(TestUtil.nextInt(random(), 1000, 1100)));
      req.add(doc);
    }
    UpdateResponse response = req.process(getRandomClient(), COLLECTION);
    // See SOLR-6547, we just need to ensure that no exception is thrown here
    assertTrue(response.getQTime() >= 0);
  }

  @Test
  public void testOverwriteOption() throws Exception {

    CollectionAdminRequest.createCollection("overwrite", "conf", 1, 1)
        .processAndWait(cluster.getSolrClient(), TIMEOUT);
    cluster.waitForActiveCollection("overwrite", 1, 1);
    
    new UpdateRequest()
        .add("id", "0", "a_t", "hello1")
        .add("id", "0", "a_t", "hello2")
        .commit(cluster.getSolrClient(), "overwrite");

    QueryResponse resp = cluster.getSolrClient().query("overwrite", new SolrQuery("*:*"));
    assertEquals("There should be one document because overwrite=true", 1, resp.getResults().getNumFound());

    new UpdateRequest()
        .add(new SolrInputDocument(id, "1", "a_t", "hello1"), /* overwrite = */ false)
        .add(new SolrInputDocument(id, "1", "a_t", "hello2"), false)
        .commit(cluster.getSolrClient(), "overwrite");
      
    resp = getRandomClient().query("overwrite", new SolrQuery("*:*"));
    assertEquals("There should be 3 documents because there should be two id=1 docs due to overwrite=false", 3, resp.getResults().getNumFound());

  }

  @Test
  public void testAliasHandling() throws Exception {
    CollectionAdminRequest.createCollection(COLLECTION, "conf", 2, 1).process(cluster.getSolrClient());
    cluster.waitForActiveCollection(COLLECTION, 2, 2);

    CollectionAdminRequest.createCollection(COLLECTION2, "conf", 2, 1).process(cluster.getSolrClient());
    cluster.waitForActiveCollection(COLLECTION2, 2, 2);

    CloudHttp2SolrClient client = getRandomClient();
    SolrInputDocument doc = new SolrInputDocument("id", "1", "title_s", "my doc");
    client.add(COLLECTION, doc);
    client.commit(COLLECTION);
    CollectionAdminRequest.createAlias("testalias", COLLECTION).process(cluster.getSolrClient());

    SolrInputDocument doc2 = new SolrInputDocument("id", "2", "title_s", "my doc too");
    client.add(COLLECTION2, doc2);
    client.commit(COLLECTION2);
    CollectionAdminRequest.createAlias("testalias2", COLLECTION2).process(cluster.getSolrClient());

    CollectionAdminRequest.createAlias("testaliascombined", COLLECTION + "," + COLLECTION2).process(cluster.getSolrClient());

    // ensure that the aliases have been registered
    Map<String, String> aliases = new CollectionAdminRequest.ListAliases().process(cluster.getSolrClient()).getAliases();
    assertEquals(COLLECTION, aliases.get("testalias"));
    assertEquals(COLLECTION2, aliases.get("testalias2"));
    assertEquals(COLLECTION + "," + COLLECTION2, aliases.get("testaliascombined"));

    assertEquals(1, client.query(COLLECTION, params("q", "*:*")).getResults().getNumFound());
    assertEquals(1, client.query("testalias", params("q", "*:*")).getResults().getNumFound());

    assertEquals(1, client.query(COLLECTION2, params("q", "*:*")).getResults().getNumFound());
    assertEquals(1, client.query("testalias2", params("q", "*:*")).getResults().getNumFound());

    assertEquals(2, client.query("testaliascombined", params("q", "*:*")).getResults().getNumFound());

    ModifiableSolrParams paramsWithBothCollections = params("q", "*:*", "collection", COLLECTION + "," + COLLECTION2);
    assertEquals(2, client.query(null, paramsWithBothCollections).getResults().getNumFound());

    ModifiableSolrParams paramsWithBothAliases = params("q", "*:*", "collection", "testalias,testalias2");
    assertEquals(2, client.query(null, paramsWithBothAliases).getResults().getNumFound());

    ModifiableSolrParams paramsWithCombinedAlias = params("q", "*:*", "collection", "testaliascombined");
    assertEquals(2, client.query(null, paramsWithCombinedAlias).getResults().getNumFound());

    ModifiableSolrParams paramsWithMixedCollectionAndAlias = params("q", "*:*", "collection", "testalias," + COLLECTION2);
    assertEquals(2, client.query(null, paramsWithMixedCollectionAndAlias).getResults().getNumFound());
  }

  @Test
  public void testRouting() throws Exception {
    CollectionAdminRequest.createCollection("routing_collection", "conf", 2, 1).process(cluster.getSolrClient());
    cluster.waitForActiveCollection("routing_collection", 2, 2);
    
    AbstractUpdateRequest request = new UpdateRequest()
        .add(id, "0", "a_t", "hello1")
        .add(id, "2", "a_t", "hello2")
        .setAction(AbstractUpdateRequest.ACTION.COMMIT, true, true);
    
    // Test single threaded routed updates for UpdateRequest
    NamedList<Object> response = getRandomClient().request(request, "routing_collection");
    if (getRandomClient().isDirectUpdatesToLeadersOnly()) {
      checkSingleServer(response);
    }
    RouteResponse rr = (RouteResponse) response;
    Map<String,LBSolrClient.Req> routes = rr.getRoutes();
    Iterator<Map.Entry<String,LBSolrClient.Req>> it = routes.entrySet()
        .iterator();
    while (it.hasNext()) {
      Map.Entry<String,LBSolrClient.Req> entry = it.next();
      String url = entry.getKey();
      UpdateRequest updateRequest = (UpdateRequest) entry.getValue()
          .getRequest();
      SolrInputDocument doc = updateRequest.getDocuments().get(0);
      String id = doc.getField("id").getValue().toString();
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.add("q", "id:" + id);
      params.add("distrib", "false");
      QueryRequest queryRequest = new QueryRequest(params);
      try (HttpSolrClient solrClient = getHttpSolrClient(url)) {
        QueryResponse queryResponse = queryRequest.process(solrClient);
        SolrDocumentList docList = queryResponse.getResults();
        assertTrue(docList.getNumFound() == 1);
      }
    }
    
    // Test the deleteById routing for UpdateRequest
    
    final UpdateResponse uResponse = new UpdateRequest()
        .deleteById("0")
        .deleteById("2")
        .commit(cluster.getSolrClient(), "routing_collection");
    if (getRandomClient().isDirectUpdatesToLeadersOnly()) {
      checkSingleServer(uResponse.getResponse());
    }

    QueryResponse qResponse = getRandomClient().query("routing_collection", new SolrQuery("*:*"));
    SolrDocumentList docs = qResponse.getResults();
    assertEquals(0, docs.getNumFound());
    
    // Test Multi-Threaded routed updates for UpdateRequest
    try (CloudSolrClient threadedClient = new CloudSolrClientBuilder
        (Collections.singletonList(cluster.getZkServer().getZkAddress()), Optional.empty())
        .withParallelUpdates(true)
        .build()) {
      threadedClient.setDefaultCollection("routing_collection");
      response = threadedClient.request(request);
      if (threadedClient.isDirectUpdatesToLeadersOnly()) {
        checkSingleServer(response);
      }
      rr = (RouteResponse) response;
      routes = rr.getRoutes();
      it = routes.entrySet()
          .iterator();
      while (it.hasNext()) {
        Map.Entry<String,LBSolrClient.Req> entry = it.next();
        String url = entry.getKey();
        UpdateRequest updateRequest = (UpdateRequest) entry.getValue()
            .getRequest();
        SolrInputDocument doc = updateRequest.getDocuments().get(0);
        String id = doc.getField("id").getValue().toString();
        ModifiableSolrParams params = new ModifiableSolrParams();
        params.add("q", "id:" + id);
        params.add("distrib", "false");
        QueryRequest queryRequest = new QueryRequest(params);
        try (HttpSolrClient solrClient = getHttpSolrClient(url)) {
          QueryResponse queryResponse = queryRequest.process(solrClient);
          SolrDocumentList docList = queryResponse.getResults();
          assertTrue(docList.getNumFound() == 1);
        }
      }
    }

    // Test that queries with _route_ params are routed by the client

    // Track request counts on each node before query calls
    ClusterState clusterState = cluster.getSolrClient().getZkStateReader().getClusterState();
    DocCollection col = clusterState.getCollection("routing_collection");
    Map<String, Long> requestCountsMap = Maps.newHashMap();
    for (Slice slice : col.getSlices()) {
      for (Replica replica : slice.getReplicas()) {
        String baseURL = (String) replica.get(ZkStateReader.BASE_URL_PROP);
        requestCountsMap.put(baseURL, getNumRequests(baseURL, "routing_collection"));
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
      log.info("output: {}", getRandomClient().query("routing_collection", solrParams));
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
        Long curNumRequests = getNumRequests(baseURL, "routing_collection");

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

  }

  /**
   * Tests if the specification of 'preferLocalShards' in the query-params
   * limits the distributed query to locally hosted shards only
   */
  @Test
  // commented 4-Sep-2018 @LuceneTestCase.BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 2-Aug-2018
  public void preferLocalShardsTest() throws Exception {

    String collectionName = "localShardsTestColl";

    int liveNodes = cluster.getJettySolrRunners().size();

    // For preferLocalShards to succeed in a test, every shard should have
    // all its cores on the same node.
    // Hence the below configuration for our collection
    CollectionAdminRequest.createCollection(collectionName, "conf", liveNodes, liveNodes)
        .setMaxShardsPerNode(liveNodes * liveNodes)
        .processAndWait(cluster.getSolrClient(), TIMEOUT);
    cluster.waitForActiveCollection(collectionName, liveNodes, liveNodes * liveNodes);
    // Add some new documents
    new UpdateRequest()
        .add(id, "0", "a_t", "hello1")
        .add(id, "2", "a_t", "hello2")
        .add(id, "3", "a_t", "hello2")
        .commit(getRandomClient(), collectionName);

    // Run the actual test for 'preferLocalShards'
    queryWithShardsPreferenceRules(getRandomClient(), false, collectionName);
    queryWithShardsPreferenceRules(getRandomClient(), true, collectionName);
  }

  @SuppressWarnings("deprecation")
  private void queryWithShardsPreferenceRules(CloudHttp2SolrClient cloudClient,
                                          boolean useShardsPreference,
                                          String collectionName)
      throws Exception
  {
    SolrQuery qRequest = new SolrQuery("*:*");

    ModifiableSolrParams qParams = new ModifiableSolrParams();
    if (useShardsPreference) {
      qParams.add(ShardParams.SHARDS_PREFERENCE, ShardParams.SHARDS_PREFERENCE_REPLICA_LOCATION + ":" + ShardParams.REPLICA_LOCAL);
    } else {
      qParams.add(CommonParams.PREFER_LOCAL_SHARDS, "true");
    }
    qParams.add(ShardParams.SHARDS_INFO, "true");
    qRequest.add(qParams);

    // CloudSolrClient sends the request to some node.
    // And since all the nodes are hosting cores from all shards, the
    // distributed query formed by this node will select cores from the
    // local shards only
    QueryResponse qResponse = cloudClient.query(collectionName, qRequest);

    Object shardsInfo = qResponse.getResponse().get(ShardParams.SHARDS_INFO);
    assertNotNull("Unable to obtain "+ShardParams.SHARDS_INFO, shardsInfo);

    // Iterate over shards-info and check what cores responded
    SimpleOrderedMap<?> shardsInfoMap = (SimpleOrderedMap<?>)shardsInfo;
    Iterator<Map.Entry<String, ?>> itr = shardsInfoMap.asMap(100).entrySet().iterator();
    List<String> shardAddresses = new ArrayList<String>();
    while (itr.hasNext()) {
      Map.Entry<String, ?> e = itr.next();
      assertTrue("Did not find map-type value in "+ShardParams.SHARDS_INFO, e.getValue() instanceof Map);
      String shardAddress = (String)((Map)e.getValue()).get("shardAddress");
      assertNotNull(ShardParams.SHARDS_INFO+" did not return 'shardAddress' parameter", shardAddress);
      shardAddresses.add(shardAddress);
    }
    log.info("Shards giving the response: " + Arrays.toString(shardAddresses.toArray()));

    // Make sure the distributed queries were directed to a single node only
    Set<Integer> ports = new HashSet<Integer>();
    for (String shardAddr: shardAddresses) {
      URL url = new URL (shardAddr);
      ports.add(url.getPort());
    }

    // This assertion would hold true as long as every shard has a core on each node
    assertTrue ("Response was not received from shards on a single node",
        shardAddresses.size() > 1 && ports.size()==1);
  }

  private Long getNumRequests(String baseUrl, String collectionName) throws
      SolrServerException, IOException {
    return getNumRequests(baseUrl, collectionName, "QUERY", "/select", null, false);
  }

  private Long getNumRequests(String baseUrl, String collectionName, String category, String key, String scope, boolean returnNumErrors) throws
      SolrServerException, IOException {

    NamedList<Object> resp;
    try (HttpSolrClient client = getHttpSolrClient(baseUrl + "/"+ collectionName, 15000, 60000)) {
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set("qt", "/admin/mbeans");
      params.set("stats", "true");
      params.set("key", key);
      params.set("cat", category);
      // use generic request to avoid extra processing of queries
      QueryRequest req = new QueryRequest(params);
      resp = client.request(req);
    }
    String name;
    if (returnNumErrors) {
      name = category + "." + (scope != null ? scope : key) + ".errors";
    } else {
      name = category + "." + (scope != null ? scope : key) + ".requests";
    }
    Map<String,Object> map = (Map<String,Object>)resp.findRecursive("solr-mbeans", category, key, "stats");
    if (map == null) {
      return null;
    }
    if (scope != null) { // admin handler uses a meter instead of counter here
      return (Long)map.get(name + ".count");
    } else {
      return (Long) map.get(name);
    }
  }

  @Test
  public void testNonRetryableRequests() throws Exception {
    try (CloudSolrClient client = getCloudSolrClient(cluster.getZkServer().getZkAddress())) {
      // important to have one replica on each node
      RequestStatusState state = CollectionAdminRequest.createCollection("foo", "conf", 1, NODE_COUNT).processAndWait(client, 60);
      if (state == RequestStatusState.COMPLETED) {
        cluster.waitForActiveCollection("foo", 1, NODE_COUNT);
        client.setDefaultCollection("foo");

        Map<String, String> adminPathToMbean = new HashMap<>(CommonParams.ADMIN_PATHS.size());
        adminPathToMbean.put(CommonParams.COLLECTIONS_HANDLER_PATH, CollectionsHandler.class.getName());
        adminPathToMbean.put(CommonParams.CORES_HANDLER_PATH, CoreAdminHandler.class.getName());
        adminPathToMbean.put(CommonParams.CONFIGSETS_HANDLER_PATH, ConfigSetsHandler.class.getName());
        // we do not add the authc/authz handlers because they do not currently expose any mbeans

        for (String adminPath : adminPathToMbean.keySet()) {
          long errorsBefore = 0;
          for (JettySolrRunner runner : cluster.getJettySolrRunners()) {
            Long numRequests = getNumRequests(runner.getBaseUrl().toString(), "foo", "ADMIN", adminPathToMbean.get(adminPath), adminPath, true);
            errorsBefore += numRequests;
            log.info("Found {} requests to {} on {}", numRequests, adminPath, runner.getBaseUrl());
          }

          ModifiableSolrParams params = new ModifiableSolrParams();
          params.set("qt", adminPath);
          params.set("action", "foobar"); // this should cause an error
          QueryRequest req = new QueryRequest(params);
          try {
            NamedList<Object> resp = client.request(req);
            fail("call to foo for admin path " + adminPath + " should have failed");
          } catch (Exception e) {
            // expected
          }
          long errorsAfter = 0;
          for (JettySolrRunner runner : cluster.getJettySolrRunners()) {
            Long numRequests = getNumRequests(runner.getBaseUrl().toString(), "foo", "ADMIN", adminPathToMbean.get(adminPath), adminPath, true);
            errorsAfter += numRequests;
            log.info("Found {} requests to {} on {}", numRequests, adminPath, runner.getBaseUrl());
          }
          assertEquals(errorsBefore + 1, errorsAfter);
        }
      } else {
        fail("Collection could not be created within 60 seconds");
      }
    }
  }

  @Test
  public void checkCollectionParameters() throws Exception {

    try (CloudSolrClient client = getCloudSolrClient(cluster.getZkServer().getZkAddress())) {

      String async1 = CollectionAdminRequest.createCollection("multicollection1", "conf", 2, 1)
          .processAsync(client);
      String async2 = CollectionAdminRequest.createCollection("multicollection2", "conf", 2, 1)
          .processAsync(client);

      CollectionAdminRequest.waitForAsyncRequest(async1, client, TIMEOUT);
      CollectionAdminRequest.waitForAsyncRequest(async2, client, TIMEOUT);
      cluster.waitForActiveCollection("multicollection1", 2, 2);
      cluster.waitForActiveCollection("multicollection2", 2, 2);
      client.setDefaultCollection("multicollection1");

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

  @Test
  @SuppressWarnings("deprecation")
  public void stateVersionParamTest() throws Exception {
    CollectionAdminRequest.createCollection(COLLECTION, "conf", 2, 1).process(cluster.getSolrClient());
    cluster.waitForActiveCollection(COLLECTION, 2, 2);

    DocCollection coll = cluster.getSolrClient().getZkStateReader().getClusterState().getCollection(COLLECTION);
    Replica r = coll.getSlices().iterator().next().getReplicas().iterator().next();

    SolrQuery q = new SolrQuery().setQuery("*:*");
    HttpSolrClient.RemoteSolrException sse = null;

    final String url = r.getStr(ZkStateReader.BASE_URL_PROP) + "/" + COLLECTION;
    try (HttpSolrClient solrClient = getHttpSolrClient(url)) {

      log.info("should work query, result {}", solrClient.query(q));
      //no problem
      q.setParam(CloudSolrClient.STATE_VERSION, COLLECTION + ":" + coll.getZNodeVersion());
      log.info("2nd query , result {}", solrClient.query(q));
      //no error yet good

      q.setParam(CloudSolrClient.STATE_VERSION, COLLECTION + ":" + (coll.getZNodeVersion() - 1)); //an older version expect error

      QueryResponse rsp = solrClient.query(q);
      Map m = (Map) rsp.getResponse().get(CloudSolrClient.STATE_VERSION, rsp.getResponse().size()-1);
      assertNotNull("Expected an extra information from server with the list of invalid collection states", m);
      assertNotNull(m.get(COLLECTION));
    }

    //now send the request to another node that does not serve the collection

    Set<String> allNodesOfColl = new HashSet<>();
    for (Slice slice : coll.getSlices()) {
      for (Replica replica : slice.getReplicas()) {
        allNodesOfColl.add(replica.getStr(ZkStateReader.BASE_URL_PROP));
      }
    }
    String theNode = null;
    Set<String> liveNodes = cluster.getSolrClient().getZkStateReader().getClusterState().getLiveNodes();
    for (String s : liveNodes) {
      String n = cluster.getSolrClient().getZkStateReader().getBaseUrlForNodeName(s);
      if(!allNodesOfColl.contains(n)){
        theNode = n;
        break;
      }
    }
    log.info("the node which does not serve this collection{} ",theNode);
    assertNotNull(theNode);


    final String solrClientUrl = theNode + "/" + COLLECTION;
    try (SolrClient solrClient = getHttpSolrClient(solrClientUrl)) {

      q.setParam(CloudSolrClient.STATE_VERSION, COLLECTION + ":" + (coll.getZNodeVersion()-1));
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

  @Test
  public void testShutdown() throws IOException {
    try (CloudSolrClient client = getCloudSolrClient("[ff01::114]:33332")) {
      client.setZkConnectTimeout(100);
      client.connect();
      fail("Expected exception");
    } catch (SolrException e) {
      assertTrue(e.getCause() instanceof TimeoutException);
    }
  }

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Test
  public void testWrongZkChrootTest() throws IOException {

    exception.expect(SolrException.class);
    exception.expectMessage("cluster not found/not ready");

    try (CloudSolrClient client = getCloudSolrClient(cluster.getZkServer().getZkAddress() + "/xyz/foo")) {
      client.setZkClientTimeout(1000 * 60);
      client.connect();
      fail("Expected exception");
    }
  }

  @Test
  public void customHttpClientTest() throws IOException {
    CloseableHttpClient client = HttpClientUtil.createClient(null);
    try (CloudSolrClient solrClient = getCloudSolrClient(cluster.getZkServer().getZkAddress(), client)) {

      assertTrue(solrClient.getLbClient().getHttpClient() == client);

    } finally {
      HttpClientUtil.close(client);
    }
  }

  @Test
  public void testVersionsAreReturned() throws Exception {
    CollectionAdminRequest.createCollection("versions_collection", "conf", 2, 1).process(cluster.getSolrClient());
    cluster.waitForActiveCollection("versions_collection", 2, 2);
    
    // assert that "adds" are returned
    UpdateRequest updateRequest = new UpdateRequest()
        .add("id", "1", "a_t", "hello1")
        .add("id", "2", "a_t", "hello2");
    updateRequest.setParam(UpdateParams.VERSIONS, Boolean.TRUE.toString());

    NamedList<Object> response = updateRequest.commit(getRandomClient(), "versions_collection").getResponse();
    Object addsObject = response.get("adds");
    
    assertNotNull("There must be a adds parameter", addsObject);
    assertTrue(addsObject instanceof NamedList<?>);
    NamedList<?> adds = (NamedList<?>) addsObject;
    assertEquals("There must be 2 versions (one per doc)", 2, adds.size());

    Map<String, Long> versions = new HashMap<>();
    Object object = adds.get("1");
    assertNotNull("There must be a version for id 1", object);
    assertTrue("Version for id 1 must be a long", object instanceof Long);
    versions.put("1", (Long) object);

    object = adds.get("2");
    assertNotNull("There must be a version for id 2", object);
    assertTrue("Version for id 2 must be a long", object instanceof Long);
    versions.put("2", (Long) object);

    QueryResponse resp = getRandomClient().query("versions_collection", new SolrQuery("*:*"));
    assertEquals("There should be one document because overwrite=true", 2, resp.getResults().getNumFound());

    for (SolrDocument doc : resp.getResults()) {
      Long version = versions.get(doc.getFieldValue("id"));
      assertEquals("Version on add must match _version_ field", version, doc.getFieldValue("_version_"));
    }

    // assert that "deletes" are returned
    UpdateRequest deleteRequest = new UpdateRequest().deleteById("1");
    deleteRequest.setParam(UpdateParams.VERSIONS, Boolean.TRUE.toString());
    response = deleteRequest.commit(getRandomClient(), "versions_collection").getResponse();
    Object deletesObject = response.get("deletes");
    assertNotNull("There must be a deletes parameter", deletesObject);
    NamedList deletes = (NamedList) deletesObject;
    assertEquals("There must be 1 version", 1, deletes.size());
  }
  
  @Test
  public void testInitializationWithSolrUrls() throws Exception {
    CollectionAdminRequest.createCollection(COLLECTION, "conf", 2, 1).process(cluster.getSolrClient());
    cluster.waitForActiveCollection(COLLECTION, 2, 2);
    CloudHttp2SolrClient client = httpBasedCloudSolrClient;
    SolrInputDocument doc = new SolrInputDocument("id", "1", "title_s", "my doc");
    client.add(COLLECTION, doc);
    client.commit(COLLECTION);
    assertEquals(1, client.query(COLLECTION, params("q", "*:*")).getResults().getNumFound());
  }

  @Test
  public void testCollectionDoesntExist() throws Exception {
    CloudHttp2SolrClient client = getRandomClient();
    SolrInputDocument doc = new SolrInputDocument("id", "1", "title_s", "my doc");
    try {
      client.add("boguscollectionname", doc);
      fail();
    } catch (SolrException ex) {
      if (ex.getMessage().equals("Collection not found: boguscollectionname")) {
        // pass
      } else {
        throw ex;
      }
    }
  }

  public void testRetryUpdatesWhenClusterStateIsStale() throws Exception {
    final String COL = "stale_state_test_col";
    assert cluster.getJettySolrRunners().size() >= 2;

    final JettySolrRunner old_leader_node = cluster.getJettySolrRunners().get(0);
    final JettySolrRunner new_leader_node = cluster.getJettySolrRunners().get(1);
    
    // start with exactly 1 shard/replica...
    assertEquals("Couldn't create collection", 0,
                 CollectionAdminRequest.createCollection(COL, "conf", 1, 1)
                 .setCreateNodeSet(old_leader_node.getNodeName())
                 .process(cluster.getSolrClient()).getStatus());
    cluster.waitForActiveCollection(COL, 1, 1);

    // determine the coreNodeName of only current replica
    Collection<Slice> slices = cluster.getSolrClient().getZkStateReader().getClusterState().getCollection(COL).getSlices();
    assertEquals(1, slices.size()); // sanity check
    Slice slice = slices.iterator().next();
    assertEquals(1, slice.getReplicas().size()); // sanity check
    final String old_leader_core_node_name = slice.getLeader().getName();

    // NOTE: creating our own CloudSolrClient whose settings we can muck with...
    try (CloudSolrClient stale_client = new CloudSolrClientBuilder
        (Collections.singletonList(cluster.getZkServer().getZkAddress()), Optional.empty())
        .sendDirectUpdatesToAnyShardReplica()
        .withParallelUpdates(true)
        .build()) {
      // don't let collection cache entries get expired, even on a slow machine...
      stale_client.setCollectionCacheTTl(Integer.MAX_VALUE);
      stale_client.setDefaultCollection(COL);
     
      // do a query to populate stale_client's cache...
      assertEquals(0, stale_client.query(new SolrQuery("*:*")).getResults().getNumFound());
      
      // add 1 replica on a diff node...
      assertEquals("Couldn't create collection", 0,
                   CollectionAdminRequest.addReplicaToShard(COL, "shard1")
                   .setNode(new_leader_node.getNodeName())
                   // NOTE: don't use our stale_client for this -- don't tip it off of a collection change
                   .process(cluster.getSolrClient()).getStatus());
      AbstractDistribZkTestBase.waitForRecoveriesToFinish
        (COL, cluster.getSolrClient().getZkStateReader(), true, true, 330);
      
      // ...and delete our original leader.
      assertEquals("Couldn't create collection", 0,
                   CollectionAdminRequest.deleteReplica(COL, "shard1", old_leader_core_node_name)
                   // NOTE: don't use our stale_client for this -- don't tip it off of a collection change
                   .process(cluster.getSolrClient()).getStatus());
      AbstractDistribZkTestBase.waitForRecoveriesToFinish
        (COL, cluster.getSolrClient().getZkStateReader(), true, true, 330);

      // stale_client's collection state cache should now only point at a leader that no longer exists.
      
      // attempt a (direct) update that should succeed in spite of cached cluster state
      // pointing solely to a node that's no longer part of our collection...
      assertEquals(0, (new UpdateRequest().add("id", "1").commit(stale_client, COL)).getStatus());
      assertEquals(1, stale_client.query(new SolrQuery("*:*")).getResults().getNumFound());
      
    }
  }
  

  private static void checkSingleServer(NamedList<Object> response) {
    final RouteResponse rr = (RouteResponse) response;
    final Map<String,LBSolrClient.Req> routes = rr.getRoutes();
    final Iterator<Map.Entry<String,LBSolrClient.Req>> it =
        routes.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<String,LBSolrClient.Req> entry = it.next();
        assertEquals("wrong number of servers: "+entry.getValue().getServers(),
            1, entry.getValue().getServers().size());
    }
  }

  /**
   * Tests if the specification of 'preferReplicaTypes` in the query-params
   * limits the distributed query to locally hosted shards only
   */
  @Test
  // commented 15-Sep-2018 @LuceneTestCase.BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 2-Aug-2018
  public void preferReplicaTypesTest() throws Exception {

    String collectionName = "replicaTypesTestColl";

    int liveNodes = cluster.getJettySolrRunners().size();

    // For these tests we need to have multiple replica types.
    // Hence the below configuration for our collection
    int pullReplicas = Math.max(1, liveNodes - 2);
    CollectionAdminRequest.createCollection(collectionName, "conf", liveNodes, 1, 1, pullReplicas)
        .setMaxShardsPerNode(liveNodes)
        .processAndWait(cluster.getSolrClient(), TIMEOUT);
    cluster.waitForActiveCollection(collectionName, liveNodes, liveNodes * (2 + pullReplicas));
    
    // Add some new documents
    new UpdateRequest()
        .add(id, "0", "a_t", "hello1")
        .add(id, "2", "a_t", "hello2")
        .add(id, "3", "a_t", "hello2")
        .commit(getRandomClient(), collectionName);

    // Run the actual tests for 'shards.preference=replica.type:*'
    queryWithPreferReplicaTypes(getRandomClient(), "PULL", false, collectionName);
    queryWithPreferReplicaTypes(getRandomClient(), "PULL|TLOG", false, collectionName);
    queryWithPreferReplicaTypes(getRandomClient(), "TLOG", false, collectionName);
    queryWithPreferReplicaTypes(getRandomClient(), "TLOG|PULL", false, collectionName);
    queryWithPreferReplicaTypes(getRandomClient(), "NRT", false, collectionName);
    queryWithPreferReplicaTypes(getRandomClient(), "NRT|PULL", false, collectionName);
    // Test to verify that preferLocalShards=true doesn't break this
    queryWithPreferReplicaTypes(getRandomClient(), "PULL", true, collectionName);
    queryWithPreferReplicaTypes(getRandomClient(), "PULL|TLOG", true, collectionName);
    queryWithPreferReplicaTypes(getRandomClient(), "TLOG", true, collectionName);
    queryWithPreferReplicaTypes(getRandomClient(), "TLOG|PULL", true, collectionName);
    queryWithPreferReplicaTypes(getRandomClient(), "NRT", false, collectionName);
    queryWithPreferReplicaTypes(getRandomClient(), "NRT|PULL", true, collectionName);
    CollectionAdminRequest.deleteCollection(collectionName)
        .processAndWait(cluster.getSolrClient(), TIMEOUT);
  }

  private void queryWithPreferReplicaTypes(CloudHttp2SolrClient cloudClient,
                                           String preferReplicaTypes,
                                           boolean preferLocalShards,
                                           String collectionName)
      throws Exception
  {
    SolrQuery qRequest = new SolrQuery("*:*");
    ModifiableSolrParams qParams = new ModifiableSolrParams();

    final List<String> preferredTypes = Arrays.asList(preferReplicaTypes.split("\\|"));
    StringBuilder rule = new StringBuilder();
    preferredTypes.forEach(type -> {
      if (rule.length() != 0) {
        rule.append(',');
      }
      rule.append(ShardParams.SHARDS_PREFERENCE_REPLICA_TYPE);
      rule.append(':');
      rule.append(type);
    });
    if (preferLocalShards) {
      if (rule.length() != 0) {
        rule.append(',');
      }
      rule.append(ShardParams.SHARDS_PREFERENCE_REPLICA_LOCATION);
      rule.append(":local");
    }
    qParams.add(ShardParams.SHARDS_PREFERENCE, rule.toString());  
    qParams.add(ShardParams.SHARDS_INFO, "true");
    qRequest.add(qParams);

    // CloudSolrClient sends the request to some node.
    // And since all the nodes are hosting cores from all shards, the
    // distributed query formed by this node will select cores from the
    // local shards only
    QueryResponse qResponse = cloudClient.query(collectionName, qRequest);

    Object shardsInfo = qResponse.getResponse().get(ShardParams.SHARDS_INFO);
    assertNotNull("Unable to obtain "+ShardParams.SHARDS_INFO, shardsInfo);

    Map<String, String> replicaTypeMap = new HashMap<>();
    DocCollection collection = getCollectionState(collectionName);
    for (Slice slice : collection.getSlices()) {
      for (Replica replica : slice.getReplicas()) {
        String coreUrl = replica.getCoreUrl();
        // It seems replica reports its core URL with a trailing slash while shard
        // info returned from the query doesn't. Oh well.
        if (coreUrl.endsWith("/")) {
          coreUrl = coreUrl.substring(0, coreUrl.length() - 1);
        }
        replicaTypeMap.put(coreUrl, replica.getType().toString());
      }
    }

    // Iterate over shards-info and check that replicas of correct type responded
    SimpleOrderedMap<?> shardsInfoMap = (SimpleOrderedMap<?>)shardsInfo;
    Iterator<Map.Entry<String, ?>> itr = shardsInfoMap.asMap(100).entrySet().iterator();
    List<String> shardAddresses = new ArrayList<String>();
    while (itr.hasNext()) {
      Map.Entry<String, ?> e = itr.next();
      assertTrue("Did not find map-type value in "+ShardParams.SHARDS_INFO, e.getValue() instanceof Map);
      String shardAddress = (String)((Map)e.getValue()).get("shardAddress");
      assertNotNull(ShardParams.SHARDS_INFO+" did not return 'shardAddress' parameter", shardAddress);
      assertTrue(replicaTypeMap.containsKey(shardAddress));
      assertTrue(preferredTypes.indexOf(replicaTypeMap.get(shardAddress)) == 0);
      shardAddresses.add(shardAddress);
    }
    assertTrue("No responses", shardAddresses.size() > 0);
    log.info("Shards giving the response: " + Arrays.toString(shardAddresses.toArray()));
  }

}
