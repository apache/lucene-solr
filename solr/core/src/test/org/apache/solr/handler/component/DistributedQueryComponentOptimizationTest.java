package org.apache.solr.handler.component;

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

import org.apache.solr.BaseDistributedSearchTestCase;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.junit.BeforeClass;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Test for QueryComponent's distributed querying optimization.
 * If the "fl" param is just "id" or just "id,score", all document data to return is already fetched by STAGE_EXECUTE_QUERY.
 * The second STAGE_GET_FIELDS query is completely unnecessary.
 * Eliminating that 2nd HTTP request can make a big difference in overall performance.
 *
 * @see QueryComponent
 */
public class DistributedQueryComponentOptimizationTest extends BaseDistributedSearchTestCase {

  public DistributedQueryComponentOptimizationTest() {
    fixShardCount = true;
    shardCount = 3;
    stress = 0;
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    initCore("solrconfig.xml", "schema-custom-field.xml");
  }

  @Override
  public void doTest() throws Exception {
    del("*:*");

    index(id, "1", "text", "a", "test_sS", "21", "payload", ByteBuffer.wrap(new byte[] { 0x12, 0x62, 0x15 }),                     //  2
          // quick check to prove "*" dynamicField hasn't been broken by somebody mucking with schema
          "asdfasdf_field_should_match_catchall_dynamic_field_adsfasdf", "value");
    index(id, "2", "text", "b", "test_sS", "22", "payload", ByteBuffer.wrap(new byte[] { 0x25, 0x21, 0x16 }));                    //  5
    index(id, "3", "text", "a", "test_sS", "23", "payload", ByteBuffer.wrap(new byte[] { 0x35, 0x32, 0x58 }));                    //  8
    index(id, "4", "text", "b", "test_sS", "24", "payload", ByteBuffer.wrap(new byte[] { 0x25, 0x21, 0x15 }));                    //  4
    index(id, "5", "text", "a", "test_sS", "25", "payload", ByteBuffer.wrap(new byte[] { 0x35, 0x35, 0x10, 0x00 }));              //  9
    index(id, "6", "text", "c", "test_sS", "26", "payload", ByteBuffer.wrap(new byte[] { 0x1a, 0x2b, 0x3c, 0x00, 0x00, 0x03 }));  //  3
    index(id, "7", "text", "c", "test_sS", "27", "payload", ByteBuffer.wrap(new byte[] { 0x00, 0x3c, 0x73 }));                    //  1
    index(id, "8", "text", "c", "test_sS", "28", "payload", ByteBuffer.wrap(new byte[] { 0x59, 0x2d, 0x4d }));                    // 11
    index(id, "9", "text", "a", "test_sS", "29", "payload", ByteBuffer.wrap(new byte[] { 0x39, 0x79, 0x7a }));                    // 10
    index(id, "10", "text", "b", "test_sS", "30", "payload", ByteBuffer.wrap(new byte[] { 0x31, 0x39, 0x7c }));                   //  6
    index(id, "11", "text", "d", "test_sS", "31", "payload", ByteBuffer.wrap(new byte[] { (byte)0xff, (byte)0xaf, (byte)0x9c })); // 13
    index(id, "12", "text", "d", "test_sS", "32", "payload", ByteBuffer.wrap(new byte[] { 0x34, (byte)0xdd, 0x4d }));             //  7
    index(id, "13", "text", "d", "test_sS", "33", "payload", ByteBuffer.wrap(new byte[] { (byte)0x80, 0x11, 0x33 }));             // 12
    commit();

    QueryResponse rsp;
    rsp = query("q", "*:*", "fl", "id,test_sS,score", "sort", "payload asc", "rows", "20");
    assertFieldValues(rsp.getResults(), id, 7, 1, 6, 4, 2, 10, 12, 3, 5, 9, 8, 13, 11);
    assertFieldValues(rsp.getResults(), "test_sS", "27", "21", "26", "24", "22", "30", "32", "23", "25", "29", "28", "33", "31");
    rsp = query("q", "*:*", "fl", "id,score", "sort", "payload desc", "rows", "20");
    assertFieldValues(rsp.getResults(), id, 11, 13, 8, 9, 5, 3, 12, 10, 2, 4, 6, 1, 7);
    // works with just fl=id as well
    rsp = query("q", "*:*", "fl", "id", "sort", "payload desc", "rows", "20");
    assertFieldValues(rsp.getResults(), id, 11, 13, 8, 9, 5, 3, 12, 10, 2, 4, 6, 1, 7);

    rsp = query("q", "*:*", "fl", "id,score", "sort", "payload asc", "rows", "20");
    assertFieldValues(rsp.getResults(), id, 7, 1, 6, 4, 2, 10, 12, 3, 5, 9, 8, 13, 11);

    rsp = query("q", "*:*", "fl", "id,test_sS,score", "sort", "payload asc", "rows", "20", "distrib.singlePass", "true");
    assertFieldValues(rsp.getResults(), id, 7, 1, 6, 4, 2, 10, 12, 3, 5, 9, 8, 13, 11);
    assertFieldValues(rsp.getResults(), "test_sS", "27", "21", "26", "24", "22", "30", "32", "23", "25", "29", "28", "33", "31");

    QueryResponse nonDistribRsp = query("q", "*:*", "fl", "id,test_sS,score", "sort", "payload asc", "rows", "20");
    compareResponses(rsp, nonDistribRsp); // make sure distrib and distrib.singlePass return the same thing

    nonDistribRsp = query("q", "*:*", "fl", "score", "sort", "payload asc", "rows", "20");
    rsp = query("q", "*:*", "fl", "score", "sort", "payload asc", "rows", "20", "distrib.singlePass", "true");
    compareResponses(rsp, nonDistribRsp); // make sure distrib and distrib.singlePass return the same thing

    // verify that the optimization actually works
    verifySinglePass("q", "*:*", "fl", "id", "sort", "payload desc", "rows", "20"); // id only is optimized by default
    verifySinglePass("q", "*:*", "fl", "id,score", "sort", "payload desc", "rows", "20"); // id,score only is optimized by default
    verifySinglePass("q", "*:*", "fl", "score", "sort", "payload asc", "rows", "20", "distrib.singlePass", "true");

    // SOLR-6545, wild card field list
    index(id, "19", "text", "d", "cat_a_sS", "1" ,"dynamic", "2", "payload", ByteBuffer.wrap(new byte[] { (byte)0x80, 0x11, 0x33 }));
    commit();

    nonDistribRsp = query("q", "id:19", "fl", "id,*a_sS", "sort", "payload asc");
    rsp = query("q", "id:19", "fl", "id,*a_sS", "sort", "payload asc", "distrib.singlePass", "true");

    assertFieldValues(nonDistribRsp.getResults(), "id", 19);
    assertFieldValues(rsp.getResults(), "id", 19);

    nonDistribRsp = query("q", "id:19", "fl", "id,dynamic,cat*", "sort", "payload asc");
    rsp = query("q", "id:19", "fl", "id,dynamic,cat*", "sort", "payload asc", "distrib.singlePass", "true");
    assertFieldValues(nonDistribRsp.getResults(), "id", 19);
    assertFieldValues(rsp.getResults(), "id", 19);

    verifySinglePass("q", "id:19", "fl", "id,*a_sS", "sort", "payload asc", "distrib.singlePass", "true");
    verifySinglePass("q", "id:19", "fl", "id,dynamic,cat*", "sort", "payload asc", "distrib.singlePass", "true");
  }

  private void verifySinglePass(String... q) throws SolrServerException {
    QueryResponse rsp;ModifiableSolrParams params = new ModifiableSolrParams();
    for (int i = 0; i < q.length; i += 2) {
      params.add(q[i].toString(), q[i + 1].toString());
    }
    params.add("shards", getShardsString());
    params.add("debug", "track");
    rsp = queryServer(new ModifiableSolrParams(params));
    Map<String, Object> debugMap = rsp.getDebugMap();
    SimpleOrderedMap<Object> track = (SimpleOrderedMap<Object>) debugMap.get("track");
    assertNotNull(track);
    assertNotNull(track.get("EXECUTE_QUERY"));
    assertNull("A single pass request should not have a GET_FIELDS phase", track.get("GET_FIELDS"));
  }
}
