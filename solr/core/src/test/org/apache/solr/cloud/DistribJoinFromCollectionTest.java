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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.hamcrest.CoreMatchers.not;

/**
 * Tests using fromIndex that points to a collection in SolrCloud mode.
 */
public class DistribJoinFromCollectionTest extends SolrCloudTestCase{

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  final private static String[] scoreModes = {"avg","max","min","total"};

//    resetExceptionIgnores();
  private static String toColl = "to_2x2";
  private static String fromColl = "from_1x4";

  private static String toDocId;
  
  @BeforeClass
  public static void setupCluster() throws Exception {
    final Path configDir = Paths.get(TEST_HOME(), "collection1", "conf");

    String configName = "solrCloudCollectionConfig";
    int nodeCount = 5;
    configureCluster(nodeCount)
       .addConfig(configName, configDir)
       .configure();
    
    
    Map<String, String> collectionProperties = new HashMap<>();
    collectionProperties.put("config", "solrconfig-tlog.xml" );
    collectionProperties.put("schema", "schema.xml"); 
    
    // create a collection holding data for the "to" side of the JOIN
    
    int shards = 2;
    int replicas = 2 ;
    CollectionAdminRequest.createCollection(toColl, configName, shards, replicas)
        .setProperties(collectionProperties)
        .process(cluster.getSolrClient());

    // get the set of nodes where replicas for the "to" collection exist
    Set<String> nodeSet = new HashSet<>();
    ZkStateReader zkStateReader = cluster.getSolrClient().getZkStateReader();
    ClusterState cs = zkStateReader.getClusterState();
    for (Slice slice : cs.getCollection(toColl).getActiveSlices())
      for (Replica replica : slice.getReplicas())
        nodeSet.add(replica.getNodeName());
    assertTrue(nodeSet.size() > 0);

    // deploy the "from" collection to all nodes where the "to" collection exists
    CollectionAdminRequest.createCollection(fromColl, configName, 1, 4)
        .setCreateNodeSet(String.join(",", nodeSet))
        .setProperties(collectionProperties)
        .process(cluster.getSolrClient());

    toDocId = indexDoc(toColl, 1001, "a", null, "b");
    indexDoc(fromColl, 2001, "a", "c", null);

    Thread.sleep(1000); // so the commits fire

  }

  @Test
  public void testScore() throws Exception {
    //without score
    testJoins(toColl, fromColl, toDocId, true);
  }
  
  @Test
  public void testNoScore() throws Exception {
    //with score
    testJoins(toColl, fromColl, toDocId, false);
    
  }
  
  @AfterClass
  public static void shutdown() {
    log.info("DistribJoinFromCollectionTest logic complete ... deleting the {} and {} collections", toColl, fromColl);

    // try to clean up
    for (String c : new String[]{ toColl, fromColl }) {
      try {
        CollectionAdminRequest.Delete req =  CollectionAdminRequest.deleteCollection(c);
        req.process(cluster.getSolrClient());
      } catch (Exception e) {
        // don't fail the test
        log.warn("Could not delete collection {} after test completed due to:", c, e);
      }
    }

    log.info("DistribJoinFromCollectionTest succeeded ... shutting down now!");
  }

  private void testJoins(String toColl, String fromColl, String toDocId, boolean isScoresTest)
      throws SolrServerException, IOException, InterruptedException {
    // verify the join with fromIndex works
    final String fromQ = "match_s:c^2";
    CloudSolrClient client = cluster.getSolrClient();
    {
    final String joinQ = "{!join " + anyScoreMode(isScoresTest)
                   + "from=join_s fromIndex=" + fromColl + 
                   " to=join_s}" + fromQ;
    QueryRequest qr = new QueryRequest(params("collection", toColl, "q", joinQ, "fl", "id,get_s,score"));
    QueryResponse rsp = new QueryResponse(client.request(qr), client);
    SolrDocumentList hits = rsp.getResults();
    assertTrue("Expected 1 doc, got "+hits, hits.getNumFound() == 1);
    SolrDocument doc = hits.get(0);
    assertEquals(toDocId, doc.getFirstValue("id"));
    assertEquals("b", doc.getFirstValue("get_s"));
    assertScore(isScoresTest, doc);
    }

    //negative test before creating an alias
    checkAbsentFromIndex(fromColl, toColl, isScoresTest);

    // create an alias for the fromIndex and then query through the alias
    String alias = fromColl+"Alias";
    CollectionAdminRequest.createAlias(alias, fromColl).process(client);

    {
      final String joinQ = "{!join " + anyScoreMode(isScoresTest)
              + "from=join_s fromIndex=" + alias + " to=join_s}"+fromQ;
      final QueryRequest qr = new QueryRequest(params("collection", toColl, "q", joinQ, "fl", "id,get_s,score"));
      final QueryResponse rsp = new QueryResponse(client.request(qr), client);
      final SolrDocumentList hits = rsp.getResults();
      assertTrue("Expected 1 doc", hits.getNumFound() == 1);
      SolrDocument doc = hits.get(0);
      assertEquals(toDocId, doc.getFirstValue("id"));
      assertEquals("b", doc.getFirstValue("get_s"));
      assertScore(isScoresTest, doc);
    }

    //negative test after creating an alias
    checkAbsentFromIndex(fromColl, toColl, isScoresTest);

    {
      // verify join doesn't work if no match in the "from" index
      final String joinQ = "{!join " + (anyScoreMode(isScoresTest))
              + "from=join_s fromIndex=" + fromColl + " to=join_s}match_s:d";
      final QueryRequest  qr = new QueryRequest(params("collection", toColl, "q", joinQ, "fl", "id,get_s,score"));
      final QueryResponse  rsp = new QueryResponse(client.request(qr), client);
      final SolrDocumentList hits = rsp.getResults();
      assertTrue("Expected no hits", hits.getNumFound() == 0);
    }
  }

  private void assertScore(boolean isScoresTest, SolrDocument doc) {
    if (isScoresTest) {
      assertThat("score join doesn't return 1.0",doc.getFirstValue("score").toString(), not("1.0"));
    } else {
      assertEquals("Solr join has constant score", "1.0", doc.getFirstValue("score").toString());
    }
  }

  private String anyScoreMode(boolean isScoresTest) {
    return isScoresTest ? "score=" + (scoreModes[random().nextInt(scoreModes.length)]) + " " : "";
  }

  private void checkAbsentFromIndex(String fromColl, String toColl, boolean isScoresTest) throws SolrServerException, IOException {
    final String wrongName = fromColl + "WrongName";
    final String joinQ = "{!join " + (anyScoreMode(isScoresTest))
        + "from=join_s fromIndex=" + wrongName + " to=join_s}match_s:c";
    final QueryRequest qr = new QueryRequest(params("collection", toColl, "q", joinQ, "fl", "id,get_s,score"));
    try {
      cluster.getSolrClient().request(qr);
    } catch (HttpSolrClient.RemoteSolrException ex) {
      assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, ex.code());
      assertTrue(ex.getMessage().contains(wrongName));
    }
  }

  protected static String indexDoc(String collection, int id, String joinField, String matchField, String getField) throws Exception {
    UpdateRequest up = new UpdateRequest();
    up.setCommitWithin(50);
    up.setParam("collection", collection);
    SolrInputDocument doc = new SolrInputDocument();
    String docId = "" + id;
    doc.addField("id", docId);
    doc.addField("join_s", joinField);
    if (matchField != null)
      doc.addField("match_s", matchField);
    if (getField != null)
      doc.addField("get_s", getField);
    up.add(doc);
    cluster.getSolrClient().request(up);
    return docId;
  }
}
