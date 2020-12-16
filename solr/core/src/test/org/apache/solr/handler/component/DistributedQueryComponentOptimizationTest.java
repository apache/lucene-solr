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
package org.apache.solr.handler.component;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.solr.BaseDistributedSearchTestCase;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.StrUtils;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test for QueryComponent's distributed querying optimization.
 * If the "fl" param is just "id" or just "id,score", all document data to return is already fetched by STAGE_EXECUTE_QUERY.
 * The second STAGE_GET_FIELDS query is completely unnecessary.
 * Eliminating that 2nd HTTP request can make a big difference in overall performance.
 *
 * @see QueryComponent
 */
public class DistributedQueryComponentOptimizationTest extends SolrCloudTestCase {

  private static final String COLLECTION = "optimize";
  private static final String SHARD1 = "shard1";
  private static final String SHARD2 = "shard2";

  private static final int sliceCount = 3;

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(3)
        .withSolrXml(TEST_PATH().resolve("solr-trackingshardhandler.xml"))
        .addConfig("conf", configset("cloud-dynamic"))
        .configure();

    CollectionAdminRequest.createCollection(COLLECTION, "conf", 3, 1)
        .setPerReplicaState(SolrCloudTestCase.USE_PER_REPLICA_STATE)
        .setMaxShardsPerNode(1)
        .processAndWait(cluster.getSolrClient(), DEFAULT_TIMEOUT);
    cluster.getSolrClient().waitForState(COLLECTION, DEFAULT_TIMEOUT, TimeUnit.SECONDS,
        (n, c) -> DocCollection.isFullyActive(n, c, sliceCount, 1));

    new UpdateRequest()
        .add(sdoc(id, "1", "text", "a", "test_sS", "21", "payload", ByteBuffer.wrap(new byte[]{0x12, 0x62, 0x15})))
        .add(sdoc(id, "2", "text", "b", "test_sS", "22", "payload", ByteBuffer.wrap(new byte[]{0x25, 0x21, 0x16})))                  //  5
        .add(sdoc(id, "3", "text", "a", "test_sS", "23", "payload", ByteBuffer.wrap(new byte[]{0x35, 0x32, 0x58})))                  //  8
        .add(sdoc(id, "4", "text", "b", "test_sS", "24", "payload", ByteBuffer.wrap(new byte[]{0x25, 0x21, 0x15})))                    //  4
        .add(sdoc(id, "5", "text", "a", "test_sS", "25", "payload", ByteBuffer.wrap(new byte[]{0x35, 0x35, 0x10, 0x00})))              //  9
        .add(sdoc(id, "6", "text", "c", "test_sS", "26", "payload", ByteBuffer.wrap(new byte[]{0x1a, 0x2b, 0x3c, 0x00, 0x00, 0x03})))  //  3
        .add(sdoc(id, "7", "text", "c", "test_sS", "27", "payload", ByteBuffer.wrap(new byte[]{0x00, 0x3c, 0x73})))                    //  1
        .add(sdoc(id, "8", "text", "c", "test_sS", "28", "payload", ByteBuffer.wrap(new byte[]{0x59, 0x2d, 0x4d})))                    // 11
        .add(sdoc(id, "9", "text", "a", "test_sS", "29", "payload", ByteBuffer.wrap(new byte[]{0x39, 0x79, 0x7a})))                    // 10
        .add(sdoc(id, "10", "text", "b", "test_sS", "30", "payload", ByteBuffer.wrap(new byte[]{0x31, 0x39, 0x7c})))                   //  6
        .add(sdoc(id, "11", "text", "d", "test_sS", "31", "payload", ByteBuffer.wrap(new byte[]{(byte) 0xff, (byte) 0xaf, (byte) 0x9c}))) // 13
        .add(sdoc(id, "12", "text", "d", "test_sS", "32", "payload", ByteBuffer.wrap(new byte[]{0x34, (byte) 0xdd, 0x4d})))             //  7
        .add(sdoc(id, "13", "text", "d", "test_sS", "33", "payload", ByteBuffer.wrap(new byte[]{(byte) 0x80, 0x11, 0x33})))             // 12
        // SOLR-6545, wild card field list
        .add(sdoc(id, "19", "text", "d", "cat_a_sS", "1", "dynamic_s", "2", "payload", ByteBuffer.wrap(new byte[]{(byte) 0x80, 0x11, 0x34})))
        .commit(cluster.getSolrClient(), COLLECTION);

  }

  private static final String id = "id";

  @Test
  public void testBasics() throws Exception {

    QueryResponse rsp;
    rsp = cluster.getSolrClient().query(COLLECTION,
        new SolrQuery("q", "*:*", "fl", "id,test_sS,score", "sort", "payload asc", "rows", "20"));
    assertFieldValues(rsp.getResults(), id, "7", "1", "6", "4", "2", "10", "12", "3", "5", "9", "8", "13", "19", "11");
    assertFieldValues(rsp.getResults(), "test_sS", "27", "21", "26", "24", "22", "30", "32", "23", "25", "29", "28", "33", null, "31");
    rsp = cluster.getSolrClient().query(COLLECTION, new SolrQuery("q", "*:*", "fl", "id,score", "sort", "payload desc", "rows", "20"));
    assertFieldValues(rsp.getResults(), id, "11", "19", "13", "8", "9", "5", "3", "12", "10", "2", "4", "6", "1", "7");

  }

  @Test
  public void testFieldList() throws Exception {

    // works with just fl=id as well
    QueryResponse rsp = cluster.getSolrClient().query(COLLECTION,
        new SolrQuery("q", "*:*", "fl", "id", "sort", "payload desc", "rows", "20"));
    assertFieldValues(rsp.getResults(), id, "11", "19", "13", "8", "9", "5", "3", "12", "10", "2", "4", "6", "1", "7");

    rsp = cluster.getSolrClient().query(COLLECTION,
        new SolrQuery("q", "*:*", "fl", "id,score", "sort", "payload asc", "rows", "20"));
    assertFieldValues(rsp.getResults(), id, "7", "1", "6", "4", "2", "10", "12", "3", "5", "9", "8", "13", "19", "11");
  }

  @Test
  public void testDistribSinglePass() throws Exception {

    QueryResponse rsp = cluster.getSolrClient().query(COLLECTION,
        new SolrQuery("q", "*:*", "fl", "id,test_sS,score", "sort", "payload asc", "rows", "20", "distrib.singlePass", "true"));
    assertFieldValues(rsp.getResults(), id, "7", "1", "6", "4", "2", "10", "12", "3", "5", "9", "8", "13", "19", "11");
    assertFieldValues(rsp.getResults(), "test_sS", "27", "21", "26", "24", "22", "30", "32", "23", "25", "29", "28", "33", null, "31");


    QueryResponse nonDistribRsp = cluster.getSolrClient().query(COLLECTION,
        new SolrQuery("q", "*:*", "fl", "id,test_sS,score", "sort", "payload asc", "rows", "20"));
    compareResponses(rsp, nonDistribRsp); // make sure distrib and distrib.singlePass return the same thing

    nonDistribRsp = cluster.getSolrClient().query(COLLECTION,
        new SolrQuery("q", "*:*", "fl", "score", "sort", "payload asc", "rows", "20"));
    rsp = cluster.getSolrClient().query(COLLECTION,
        new SolrQuery("q", "*:*", "fl", "score", "sort", "payload asc", "rows", "20", "distrib.singlePass", "true"));
    compareResponses(rsp, nonDistribRsp); // make sure distrib and distrib.singlePass return the same thing

  }

  @Test
  public void testOptimizations() throws Exception {

    // verify that the optimization actually works
    queryWithAsserts("q", "*:*", "fl", "id", "sort", "payload desc", "rows", "20"); // id only is optimized by default
    queryWithAsserts("q", "*:*", "fl", "id,score", "sort", "payload desc", "rows", "20"); // id,score only is optimized by default
    queryWithAsserts("q", "*:*", "fl", "score", "sort", "payload asc", "rows", "20", "distrib.singlePass", "true");

  }

  @Test
  public void testWildcardFieldList() throws Exception {

    QueryResponse nonDistribRsp = queryWithAsserts("q", "id:19", "fl", "id,*a_sS", "sort", "payload asc");
    QueryResponse rsp = queryWithAsserts("q", "id:19", "fl", "id,*a_sS", "sort", "payload asc", "distrib.singlePass", "true");

    assertFieldValues(nonDistribRsp.getResults(), "id", "19");
    assertFieldValues(rsp.getResults(), "id", "19");

    nonDistribRsp = queryWithAsserts("q", "id:19", "fl", "id,dynamic_s,cat*", "sort", "payload asc");
    rsp = queryWithAsserts("q", "id:19", "fl", "id,dynamic_s,cat*", "sort", "payload asc", "distrib.singlePass", "true");
    assertFieldValues(nonDistribRsp.getResults(), "id", "19");
    assertFieldValues(rsp.getResults(), "id", "19");

    queryWithAsserts("q", "id:19", "fl", "id,*a_sS", "sort", "payload asc", "distrib.singlePass", "true");
    queryWithAsserts("q", "id:19", "fl", "id,dynamic_s,cat*", "sort", "payload asc", "distrib.singlePass", "true");

    // fl=*
    queryWithAsserts("q", "*:*", "fl", "*", "sort", "payload desc", ShardParams.DISTRIB_SINGLE_PASS, "true");
    queryWithAsserts("q", "*:*", "fl", "*", "sort", "payload desc");

    // fl=*,score
    queryWithAsserts("q", "*:*", "fl", "*,score", "sort", "payload desc", ShardParams.DISTRIB_SINGLE_PASS, "true");
    queryWithAsserts("q", "*:*", "fl", "*,score", "sort", "payload desc");
  }

  @Test
  public void testScoreAlwaysReturned() throws Exception {
    // see SOLR-6795, distrib.singlePass=true would return score even when not asked for
    queryWithAsserts("q", "id:19", ShardParams.DISTRIB_SINGLE_PASS, "true");
  }

  @Test
  public void testMultipleFlParams() throws Exception {
    // fix for a bug where not all fields are returned if using multiple fl parameters, see SOLR-6796
    queryWithAsserts("q", "*:*", "fl", "id", "fl", "dynamic_s", "sort", "payload desc", ShardParams.DISTRIB_SINGLE_PASS, "true");
  }

  @Test
  public void testMissingFieldListWithSort() throws Exception {
    // missing fl with sort
    queryWithAsserts("q", "*:*", "sort", "payload desc", ShardParams.DISTRIB_SINGLE_PASS, "true");
    queryWithAsserts("q", "*:*", "sort", "payload desc");
  }

  private static void compareResponses(QueryResponse rsp1, QueryResponse rsp2) {
    Map<String, Integer> skipValues = new HashMap<>();
    skipValues.put("timestamp", BaseDistributedSearchTestCase.SKIPVAL);
    skipValues.put("_version_", BaseDistributedSearchTestCase.SKIPVAL);
    skipValues.put("maxScore", BaseDistributedSearchTestCase.SKIPVAL);
    BaseDistributedSearchTestCase.compare(rsp1.getResponse(), rsp2.getResponse(), 0, skipValues);
  }

  /**
   * This test now asserts that every distrib.singlePass query:
   * <ol>
   * <li>Makes exactly 'numSlices' number of shard requests</li>
   * <li>Makes no GET_FIELDS requests</li>
   * <li>Must request the unique key field from shards</li>
   * <li>Must request the score if 'fl' has score or sort by score is requested</li>
   * <li>Requests all fields that are present in 'fl' param</li>
   * </ol>
   * <p>
   * It also asserts that every regular two phase distribtued search:
   * <ol>
   * <li>Makes at most 2 * 'numSlices' number of shard requests</li>
   * <li>Must request the unique key field from shards</li>
   * <li>Must request the score if 'fl' has score or sort by score is requested</li>
   * <li>Requests no fields other than id and score in GET_TOP_IDS request</li>
   * <li>Requests exactly the fields that are present in 'fl' param in GET_FIELDS request and no others</li>
   * </ol>
   * <p>
   * and also asserts that each query which requests id or score or both behaves exactly like a single pass query
   */
  private QueryResponse queryWithAsserts(String... q) throws Exception {
    TrackingShardHandlerFactory.RequestTrackingQueue trackingQueue = new TrackingShardHandlerFactory.RequestTrackingQueue();
    // the jettys doesn't include the control jetty which is exactly what we need here
    TrackingShardHandlerFactory.setTrackingQueue(cluster, trackingQueue);

    // let's add debug=track to such requests so we can use DebugComponent responses for assertions
    QueryResponse response = cluster.getSolrClient().query(COLLECTION, new SolrQuery("debug", "track", q));

    Map<String, List<TrackingShardHandlerFactory.ShardRequestAndParams>> requests = trackingQueue.getAllRequests();
    int numRequests = getNumRequests(requests);

    boolean distribSinglePass = false;

    Set<String> fls = new HashSet<>();
    Set<String> sortFields = new HashSet<>();
    for (int i = 0; i < q.length; i += 2) {
      if (ShardParams.DISTRIB_SINGLE_PASS.equals(q[i].toString()) && Boolean.parseBoolean(q[i + 1].toString())) {
        assertTrue("distrib.singlePass=true made more requests than number of shards",
            numRequests == sliceCount);
        distribSinglePass = true;
      }
      if (CommonParams.FL.equals(q[i].toString())) {
        fls.addAll(StrUtils.splitSmart(q[i + 1].toString(), ','));
      }
      if (CommonParams.SORT.equals(q[i].toString())) {
        String val = q[i + 1].toString().trim();
        // take care of asc/desc decorators
        sortFields.addAll(StrUtils.splitSmart(StrUtils.splitSmart(val, ' ').get(0), ','));
      }
    }

    Set<String> idScoreFields = new HashSet<>(2);
    idScoreFields.add("id"); // id is always requested in GET_TOP_IDS phase
    // score is optional, requested only if sorted by score
    if (fls.contains("score") || sortFields.contains("score")) idScoreFields.add("score");

    if (idScoreFields.containsAll(fls) && !fls.isEmpty()) {
      // if id and/or score are the only fields being requested then we implicitly turn on distribSinglePass=true
      distribSinglePass = true;
    }

    if (distribSinglePass) {
      Map<String, Object> debugMap = response.getDebugMap();
      @SuppressWarnings({"unchecked"})
      SimpleOrderedMap<Object> track = (SimpleOrderedMap<Object>) debugMap.get("track");
      assertNotNull(track);
      assertNotNull(track.get("EXECUTE_QUERY"));
      assertNull("A single pass request should not have a GET_FIELDS phase", track.get("GET_FIELDS"));

      // all fields should be requested in one go but even if 'id' is not requested by user
      // it must still be fetched in this phase to merge correctly
      Set<String> reqAndIdScoreFields = new HashSet<>(fls);
      reqAndIdScoreFields.addAll(idScoreFields);
      assertParamsEquals(trackingQueue, COLLECTION, SHARD1,
          CommonParams.FL, ShardRequest.PURPOSE_GET_TOP_IDS, reqAndIdScoreFields.toArray(new String[reqAndIdScoreFields.size()]));
      assertParamsEquals(trackingQueue, COLLECTION, SHARD2,
          CommonParams.FL, ShardRequest.PURPOSE_GET_TOP_IDS, reqAndIdScoreFields.toArray(new String[reqAndIdScoreFields.size()]));
    } else {
      // we are assuming there are facet refinement or distributed idf requests here
      assertTrue("distrib.singlePass=false made more requests than 2 * number of shards." +
              " Actual: " + numRequests + " but expected <= " + sliceCount * 2,
          numRequests <= sliceCount * 2);

      // only id and/or score should be requested
      assertParamsEquals(trackingQueue, COLLECTION, SHARD1,
          CommonParams.FL, ShardRequest.PURPOSE_GET_TOP_IDS, idScoreFields.toArray(new String[idScoreFields.size()]));
      assertParamsEquals(trackingQueue, COLLECTION, SHARD2,
          CommonParams.FL, ShardRequest.PURPOSE_GET_TOP_IDS, idScoreFields.toArray(new String[idScoreFields.size()]));

      // only originally requested fields must be requested in GET_FIELDS request
      assertParamsEquals(trackingQueue, COLLECTION, SHARD1,
          CommonParams.FL, ShardRequest.PURPOSE_GET_FIELDS, fls.toArray(new String[fls.size()]));
      assertParamsEquals(trackingQueue, COLLECTION, SHARD2,
          CommonParams.FL, ShardRequest.PURPOSE_GET_FIELDS, fls.toArray(new String[fls.size()]));
    }

    return response;
  }

  private int getNumRequests(Map<String, List<TrackingShardHandlerFactory.ShardRequestAndParams>> requests) {
    int beforeNumRequests = 0;
    for (Map.Entry<String, List<TrackingShardHandlerFactory.ShardRequestAndParams>> entry : requests.entrySet()) {
      beforeNumRequests += entry.getValue().size();
    }
    return beforeNumRequests;
  }

  private void assertParamsEquals(TrackingShardHandlerFactory.RequestTrackingQueue trackingQueue, String collection, String shard, String paramName, int purpose, String... values) {
    TrackingShardHandlerFactory.ShardRequestAndParams getByIdRequest
        = trackingQueue.getShardRequestByPurpose(cluster.getSolrClient().getZkStateReader(), collection, shard, purpose);
    assertParamsEquals(getByIdRequest, paramName, values);
  }

  private void assertParamsEquals(TrackingShardHandlerFactory.ShardRequestAndParams requestAndParams, String paramName, String... values) {
    if (requestAndParams == null) return;
    int expectedCount = values.length;
    String[] params = requestAndParams.params.getParams(paramName);
    if (expectedCount > 0 && (params == null || params.length == 0)) {
      fail("Expected non-zero number of '" + paramName + "' parameters in request");
    }
    Set<String> requestedFields = new HashSet<>();
    if (params != null) {
      for (String p : params) {
        List<String> list = StrUtils.splitSmart(p, ',');
        for (String s : list) {
          // make sure field names aren't duplicated in the parameters
          assertTrue("Field name " + s + " was requested multiple times: params = " + requestAndParams.params,
              requestedFields.add(s));
        }
      }
    }
    // if a wildcard ALL field is requested then we don't need to match exact number of params
    if (!requestedFields.contains("*"))  {
      assertEquals("Number of requested fields do not match with expectations", expectedCount, requestedFields.size());
      for (String field : values) {
        if (!requestedFields.contains(field)) {
          fail("Field " + field + " not found in param: " + paramName + " request had " + paramName + "=" + requestedFields);
        }
      }
    }
  }
}
