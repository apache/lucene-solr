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

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.junit.After;
import org.junit.Before;

import org.apache.commons.lang.StringUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.hamcrest.CoreMatchers.*;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.HashSet;
import java.util.Set;

/**
 * Tests using fromIndex that points to a collection in SolrCloud mode.
 */
public class DistribJoinFromCollectionTest extends AbstractFullDistribZkTestBase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public DistribJoinFromCollectionTest() {
    super();
  }
  
  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    System.setProperty("numShards", Integer.toString(sliceCount));
  }
  
  @Override
  @After
  public void tearDown() throws Exception {    
    try {
      super.tearDown();
    } catch (Exception exc) {}
    resetExceptionIgnores();
  }

  @Test
  public void test() throws Exception {
    // create a collection holding data for the "to" side of the JOIN
    String toColl = "to_2x2";
    createCollection(toColl, 2, 2, 2);
    ensureAllReplicasAreActive(toColl, "shard1", 2, 2, 30);
    ensureAllReplicasAreActive(toColl, "shard2", 2, 2, 30);

    // get the set of nodes where replicas for the "to" collection exist
    Set<String> nodeSet = new HashSet<>();
    ClusterState cs = cloudClient.getZkStateReader().getClusterState();
    for (Slice slice : cs.getActiveSlices(toColl))
      for (Replica replica : slice.getReplicas())
        nodeSet.add(replica.getNodeName());
    assertTrue(nodeSet.size() > 0);

    // deploy the "from" collection to all nodes where the "to" collection exists
    String fromColl = "from_1x2";
    createCollection(null, fromColl, 1, nodeSet.size(), 1, null, StringUtils.join(nodeSet,","));
    ensureAllReplicasAreActive(fromColl, "shard1", 1, nodeSet.size(), 30);

    // both to and from collections are up and active, index some docs ...
    Integer toDocId = indexDoc(toColl, 1001, "a", null, "b");
    indexDoc(fromColl, 2001, "a", "c", null);

    Thread.sleep(1000); // so the commits fire

    //without score
    testJoins(toColl, fromColl, toDocId, false);

    //with score
    testJoins(toColl, fromColl, toDocId, true);

    log.info("DistribJoinFromCollectionTest logic complete ... deleting the " + toColl + " and " + fromColl + " collections");

    // try to clean up
    for (String c : new String[]{ toColl, fromColl }) {
      try {
        CollectionAdminRequest.Delete req = new CollectionAdminRequest.Delete()
                .setCollectionName(c);
        req.process(cloudClient);
      } catch (Exception e) {
        // don't fail the test
        log.warn("Could not delete collection {} after test completed due to: " + e, c);
      }
    }

    log.info("DistribJoinFromCollectionTest succeeded ... shutting down now!");
  }

  private void testJoins(String toColl, String fromColl, Integer toDocId, boolean isScoresTest)
      throws SolrServerException, IOException {
    // verify the join with fromIndex works
    final String[] scoreModes = {"avg","max","min","total"};
    String joinQ = "{!join " + anyScoreMode(isScoresTest, scoreModes)
                   + "from=join_s fromIndex=" + fromColl + " to=join_s}match_s:c";
    QueryRequest qr = new QueryRequest(params("collection", toColl, "q", joinQ, "fl", "id,get_s,score"));
    QueryResponse rsp = new QueryResponse(cloudClient.request(qr), cloudClient);
    SolrDocumentList hits = rsp.getResults();
    assertTrue("Expected 1 doc", hits.getNumFound() == 1);
    SolrDocument doc = hits.get(0);
    assertEquals(toDocId, doc.getFirstValue("id"));
    assertEquals("b", doc.getFirstValue("get_s"));
    assertScore(isScoresTest, doc);

    //negative test before creating an alias
    checkAbsentFromIndex(fromColl, toColl, isScoresTest, scoreModes);

    // create an alias for the fromIndex and then query through the alias
    String alias = fromColl+"Alias";
    CollectionAdminRequest.CreateAlias request = new CollectionAdminRequest.CreateAlias();
    request.setAliasName(alias);
    request.setAliasedCollections(fromColl);
    request.process(cloudClient);

    joinQ = "{!join " + anyScoreMode(isScoresTest, scoreModes)
            + "from=join_s fromIndex=" + alias + " to=join_s}match_s:c";
    qr = new QueryRequest(params("collection", toColl, "q", joinQ, "fl", "id,get_s,score"));
    rsp = new QueryResponse(cloudClient.request(qr), cloudClient);
    hits = rsp.getResults();
    assertTrue("Expected 1 doc", hits.getNumFound() == 1);
    doc = hits.get(0);
    assertEquals(toDocId, doc.getFirstValue("id"));
    assertEquals("b", doc.getFirstValue("get_s"));
    assertScore(isScoresTest, doc);

    //negative test after creating an alias
    checkAbsentFromIndex(fromColl, toColl, isScoresTest, scoreModes);

    // verify join doesn't work if no match in the "from" index
    joinQ = "{!join " + (anyScoreMode(isScoresTest, scoreModes))
            + "from=join_s fromIndex=" + fromColl + " to=join_s}match_s:d";
    qr = new QueryRequest(params("collection", toColl, "q", joinQ, "fl", "id,get_s,score"));
    rsp = new QueryResponse(cloudClient.request(qr), cloudClient);
    hits = rsp.getResults();
    assertTrue("Expected no hits", hits.getNumFound() == 0);
    assertScore(isScoresTest, doc);
  }

  private void assertScore(boolean isScoresTest, SolrDocument doc) {
    if (isScoresTest) {
      assertThat(doc.getFirstValue("score").toString(), not("1.0"));
    } else {
      assertEquals("1.0", doc.getFirstValue("score").toString());
    }
  }

  private String anyScoreMode(boolean isScoresTest, String[] scoreModes) {
    return isScoresTest ? "score=" + (scoreModes[random().nextInt(scoreModes.length)]) + " " : "";
  }

  private void checkAbsentFromIndex(String fromColl, String toColl, boolean isScoresTest, String[] scoreModes) throws SolrServerException, IOException {
    final String wrongName = fromColl + "WrongName";
    final String joinQ = "{!join " + (anyScoreMode(isScoresTest, scoreModes))
        + "from=join_s fromIndex=" + wrongName + " to=join_s}match_s:c";
    final QueryRequest qr = new QueryRequest(params("collection", toColl, "q", joinQ, "fl", "id,get_s,score"));
    try {
      cloudClient.request(qr);
    } catch (HttpSolrClient.RemoteSolrException ex) {
      assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, ex.code());
      assertTrue(ex.getMessage().contains(wrongName));
    }
  }

  protected Integer indexDoc(String collection, int id, String joinField, String matchField, String getField) throws Exception {
    UpdateRequest up = new UpdateRequest();
    up.setCommitWithin(50);
    up.setParam("collection", collection);
    SolrInputDocument doc = new SolrInputDocument();
    Integer docId = new Integer(id);
    doc.addField("id", docId);
    doc.addField("join_s", joinField);
    if (matchField != null)
      doc.addField("match_s", matchField);
    if (getField != null)
      doc.addField("get_s", getField);
    up.add(doc);
    cloudClient.request(up);
    return docId;
  }
}
