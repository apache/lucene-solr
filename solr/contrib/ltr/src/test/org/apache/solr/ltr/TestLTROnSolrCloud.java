/* * Licensed to the Apache Software Foundation (ASF) under one or more
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
package org.apache.solr.ltr;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.SortedMap;
import java.util.stream.IntStream;

import org.apache.commons.io.FileUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.embedded.JettyConfig;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.ltr.feature.FieldValueFeature;
import org.apache.solr.ltr.feature.OriginalScoreFeature;
import org.apache.solr.ltr.feature.SolrFeature;
import org.apache.solr.ltr.feature.ValueFeature;
import org.apache.solr.ltr.model.LinearModel;
import org.apache.solr.util.RestTestHarness;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.AfterClass;
import org.junit.Test;

import static java.util.stream.Collectors.toList;

public class TestLTROnSolrCloud extends TestRerankBase {

  private MiniSolrCloudCluster solrCluster;
  String solrconfig = "solrconfig-ltr.xml";
  String schema = "schema.xml";

  SortedMap<ServletHolder,String> extraServlets = null;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    extraServlets = setupTestInit(solrconfig, schema, true);
    System.setProperty("enable.update.log", "true");

    int numberOfShards = random().nextInt(4)+1;
    int numberOfReplicas = random().nextInt(2)+1;
    int maxShardsPerNode = random().nextInt(4)+1;

    int numberOfNodes = (numberOfShards*numberOfReplicas + (maxShardsPerNode-1))/maxShardsPerNode;

    setupSolrCluster(numberOfShards, numberOfReplicas, numberOfNodes, maxShardsPerNode);


  }


  @Override
  public void tearDown() throws Exception {
    restTestHarness.close();
    restTestHarness = null;
    solrCluster.shutdown();
    super.tearDown();
  }

  @Test
  // commented 4-Sep-2018 @LuceneTestCase.BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 2-Aug-2018
  // commented out on: 24-Dec-2018   @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 14-Oct-2018
  public void testSimpleQuery() throws Exception {
    // will randomly pick a configuration with [1..5] shards and [1..3] replicas

    // Test regular query, it will sort the documents by inverse
    // popularity (the less popular, docid == 1, will be in the first
    // position
    SolrQuery query = new SolrQuery("{!func}sub(8,field(popularity))");

    query.setRequestHandler("/query");
    query.setFields("*,score");
    query.setParam("rows", "8");

    QueryResponse queryResponse =
        solrCluster.getSolrClient().query(COLLECTION,query);
    assertEquals(8, queryResponse.getResults().getNumFound());
    assertEquals("1", queryResponse.getResults().get(0).get("id").toString());
    assertEquals("2", queryResponse.getResults().get(1).get("id").toString());
    assertEquals("3", queryResponse.getResults().get(2).get("id").toString());
    assertEquals("4", queryResponse.getResults().get(3).get("id").toString());
    assertEquals("5", queryResponse.getResults().get(4).get("id").toString());
    assertEquals("6", queryResponse.getResults().get(5).get("id").toString());
    assertEquals("7", queryResponse.getResults().get(6).get("id").toString());
    assertEquals("8", queryResponse.getResults().get(7).get("id").toString());

    final Float original_result0_score = (Float)queryResponse.getResults().get(0).get("score");
    final Float original_result1_score = (Float)queryResponse.getResults().get(1).get("score");
    final Float original_result2_score = (Float)queryResponse.getResults().get(2).get("score");
    final Float original_result3_score = (Float)queryResponse.getResults().get(3).get("score");
    final Float original_result4_score = (Float)queryResponse.getResults().get(4).get("score");
    final Float original_result5_score = (Float)queryResponse.getResults().get(5).get("score");
    final Float original_result6_score = (Float)queryResponse.getResults().get(6).get("score");
    final Float original_result7_score = (Float)queryResponse.getResults().get(7).get("score");

    final String result0_features = FeatureLoggerTestUtils.toFeatureVector(
        "powpularityS", "64.0", "c3", "2.0", "original", "0.0", "dvIntFieldFeature", "8.0",
        "dvLongFieldFeature", "8.0", "dvFloatFieldFeature", "0.8", "dvDoubleFieldFeature", "0.8",
        "dvStrNumFieldFeature", "0.0", "dvStrBoolFieldFeature", "1.0");
    final String result1_features = FeatureLoggerTestUtils.toFeatureVector(
        "powpularityS", "49.0", "c3", "2.0", "original", "1.0", "dvIntFieldFeature", "7.0",
        "dvLongFieldFeature", "7.0", "dvFloatFieldFeature", "0.7", "dvDoubleFieldFeature", "0.7",
        "dvStrNumFieldFeature", "1.0", "dvStrBoolFieldFeature", "0.0");
    final String result2_features = FeatureLoggerTestUtils.toFeatureVector(
        "powpularityS", "36.0", "c3", "2.0", "original", "2.0", "dvIntFieldFeature", "6.0",
        "dvLongFieldFeature", "6.0", "dvFloatFieldFeature", "0.6", "dvDoubleFieldFeature", "0.6",
        "dvStrNumFieldFeature", "0.0", "dvStrBoolFieldFeature", "1.0");
    final String result3_features = FeatureLoggerTestUtils.toFeatureVector(
        "powpularityS", "25.0", "c3", "2.0", "original", "3.0", "dvIntFieldFeature", "5.0",
        "dvLongFieldFeature", "5.0", "dvFloatFieldFeature", "0.5", "dvDoubleFieldFeature", "0.5",
        "dvStrNumFieldFeature", "1.0", "dvStrBoolFieldFeature", "0.0");
    final String result4_features = FeatureLoggerTestUtils.toFeatureVector(
        "powpularityS", "16.0", "c3", "2.0", "original", "4.0", "dvIntFieldFeature", "4.0",
        "dvLongFieldFeature", "4.0", "dvFloatFieldFeature", "0.4", "dvDoubleFieldFeature", "0.4",
        "dvStrNumFieldFeature", "0.0", "dvStrBoolFieldFeature", "1.0");
    final String result5_features = FeatureLoggerTestUtils.toFeatureVector(
        "powpularityS", "9.0", "c3", "2.0", "original", "5.0", "dvIntFieldFeature", "3.0",
        "dvLongFieldFeature", "3.0", "dvFloatFieldFeature", "0.3", "dvDoubleFieldFeature", "0.3",
        "dvStrNumFieldFeature", "1.0", "dvStrBoolFieldFeature", "0.0");
    final String result6_features = FeatureLoggerTestUtils.toFeatureVector(
        "powpularityS", "4.0", "c3", "2.0", "original", "6.0", "dvIntFieldFeature", "2.0",
        "dvLongFieldFeature", "2.0", "dvFloatFieldFeature", "0.2", "dvDoubleFieldFeature", "0.2",
        "dvStrNumFieldFeature", "0.0", "dvStrBoolFieldFeature", "1.0");
    final String result7_features = FeatureLoggerTestUtils.toFeatureVector(
        "powpularityS", "1.0", "c3", "2.0", "original", "7.0", "dvIntFieldFeature", "-1.0",
        "dvLongFieldFeature", "-2.0", "dvFloatFieldFeature", "-3.0", "dvDoubleFieldFeature", "-4.0",
        "dvStrNumFieldFeature", "-5.0", "dvStrBoolFieldFeature", "0.0");


    // Test feature vectors returned (without re-ranking)
    query.setFields("*,score,features:[fv store=test]");
    queryResponse =
        solrCluster.getSolrClient().query(COLLECTION,query);
    assertEquals(8, queryResponse.getResults().getNumFound());
    assertEquals("1", queryResponse.getResults().get(0).get("id").toString());
    assertEquals("2", queryResponse.getResults().get(1).get("id").toString());
    assertEquals("3", queryResponse.getResults().get(2).get("id").toString());
    assertEquals("4", queryResponse.getResults().get(3).get("id").toString());
    assertEquals("5", queryResponse.getResults().get(4).get("id").toString());
    assertEquals("6", queryResponse.getResults().get(5).get("id").toString());
    assertEquals("7", queryResponse.getResults().get(6).get("id").toString());
    assertEquals("8", queryResponse.getResults().get(7).get("id").toString());

    assertEquals(original_result0_score, queryResponse.getResults().get(0).get("score"));
    assertEquals(original_result1_score, queryResponse.getResults().get(1).get("score"));
    assertEquals(original_result2_score, queryResponse.getResults().get(2).get("score"));
    assertEquals(original_result3_score, queryResponse.getResults().get(3).get("score"));
    assertEquals(original_result4_score, queryResponse.getResults().get(4).get("score"));
    assertEquals(original_result5_score, queryResponse.getResults().get(5).get("score"));
    assertEquals(original_result6_score, queryResponse.getResults().get(6).get("score"));
    assertEquals(original_result7_score, queryResponse.getResults().get(7).get("score"));

    assertEquals(result7_features,
        queryResponse.getResults().get(0).get("features").toString());
    assertEquals(result6_features,
        queryResponse.getResults().get(1).get("features").toString());
    assertEquals(result5_features,
        queryResponse.getResults().get(2).get("features").toString());
    assertEquals(result4_features,
        queryResponse.getResults().get(3).get("features").toString());
    assertEquals(result3_features,
        queryResponse.getResults().get(4).get("features").toString());
    assertEquals(result2_features,
        queryResponse.getResults().get(5).get("features").toString());
    assertEquals(result1_features,
        queryResponse.getResults().get(6).get("features").toString());
    assertEquals(result0_features,
        queryResponse.getResults().get(7).get("features").toString());

    // Test feature vectors returned (with re-ranking)
    query.setFields("*,score,features:[fv]");
    query.add("rq", "{!ltr model=powpularityS-model reRankDocs=8}");
    queryResponse =
        solrCluster.getSolrClient().query(COLLECTION,query);
    assertEquals(8, queryResponse.getResults().getNumFound());
    assertEquals("8", queryResponse.getResults().get(0).get("id").toString());
    assertEquals(result0_features,
        queryResponse.getResults().get(0).get("features").toString());
    assertEquals("7", queryResponse.getResults().get(1).get("id").toString());
    assertEquals(result1_features,
        queryResponse.getResults().get(1).get("features").toString());
    assertEquals("6", queryResponse.getResults().get(2).get("id").toString());
    assertEquals(result2_features,
        queryResponse.getResults().get(2).get("features").toString());
    assertEquals("5", queryResponse.getResults().get(3).get("id").toString());
    assertEquals(result3_features,
        queryResponse.getResults().get(3).get("features").toString());
    assertEquals("4", queryResponse.getResults().get(4).get("id").toString());
    assertEquals(result4_features,
        queryResponse.getResults().get(4).get("features").toString());
    assertEquals("3", queryResponse.getResults().get(5).get("id").toString());
    assertEquals(result5_features,
        queryResponse.getResults().get(5).get("features").toString());
    assertEquals("2", queryResponse.getResults().get(6).get("id").toString());
    assertEquals(result6_features,
        queryResponse.getResults().get(6).get("features").toString());
    assertEquals("1", queryResponse.getResults().get(7).get("id").toString());
    assertEquals(result7_features,
        queryResponse.getResults().get(7).get("features").toString());
  }

  private void setupSolrCluster(int numShards, int numReplicas, int numServers, int maxShardsPerNode) throws Exception {
    JettyConfig jc = buildJettyConfig("/solr");
    jc = JettyConfig.builder(jc).withServlets(extraServlets).build();
    solrCluster = new MiniSolrCloudCluster(numServers, tmpSolrHome.toPath(), jc);
    File configDir = tmpSolrHome.toPath().resolve("collection1/conf").toFile();
    solrCluster.uploadConfigSet(configDir.toPath(), "conf1");

    solrCluster.getSolrClient().setDefaultCollection(COLLECTION);

    createCollection(COLLECTION, "conf1", numShards, numReplicas, maxShardsPerNode);
    indexDocuments(COLLECTION);
    for (JettySolrRunner solrRunner : solrCluster.getJettySolrRunners()) {
      if (!solrRunner.getCoreContainer().getCores().isEmpty()){
        String coreName = solrRunner.getCoreContainer().getCores().iterator().next().getName();
        restTestHarness = new RestTestHarness(() -> solrRunner.getBaseUrl().toString() + "/" + coreName);
        break;
      }
    }
    loadModelsAndFeatures();
  }


  private void createCollection(String name, String config, int numShards, int numReplicas, int maxShardsPerNode)
      throws Exception {
    CollectionAdminResponse response;
    CollectionAdminRequest.Create create =
        CollectionAdminRequest.createCollection(name, config, numShards, numReplicas);
    create.setMaxShardsPerNode(maxShardsPerNode);
    response = create.process(solrCluster.getSolrClient());

    if (response.getStatus() != 0 || response.getErrorMessages() != null) {
      fail("Could not create collection. Response" + response.toString());
    }
    ZkStateReader zkStateReader = solrCluster.getSolrClient().getZkStateReader();
    solrCluster.waitForActiveCollection(name, numShards, numShards * numReplicas);
  }


  void indexDocument(String collection, String id, String title, String description, int popularity)
    throws Exception{
    SolrInputDocument doc = new SolrInputDocument();
    doc.setField("id", id);
    doc.setField("title", title);
    doc.setField("description", description);
    doc.setField("popularity", popularity);
    if (popularity != 1) {
      // check that empty values will be read as default
      doc.setField("dvIntField", popularity);
      doc.setField("dvLongField", popularity);
      doc.setField("dvFloatField", ((float) popularity) / 10);
      doc.setField("dvDoubleField", ((double) popularity) / 10);
      doc.setField("dvStrNumField", popularity % 2 == 0 ? "F" : "T");
      doc.setField("dvStrBoolField", popularity % 2 == 0 ? "T" : "F");
    }
    solrCluster.getSolrClient().add(collection, doc);
  }

  private void indexDocuments(final String collection)
       throws Exception {
    final int collectionSize = 8;
    // put documents in random order to check that advanceExact is working correctly
    List<Integer> docIds = IntStream.rangeClosed(1, collectionSize).boxed().collect(toList());
    Collections.shuffle(docIds, random());

    int docCounter = 1;
    for (int docId : docIds) {
      final int popularity = docId;
      indexDocument(collection, String.valueOf(docId), "a1", "bloom", popularity);
      // maybe commit in the middle in order to check that everything works fine for multi-segment case
      if (docCounter == collectionSize / 2 && random().nextBoolean()) {
        solrCluster.getSolrClient().commit(collection);
      }
      docCounter++;
    }
    solrCluster.getSolrClient().commit(collection, true, true);
  }

  private void loadModelsAndFeatures() throws Exception {
    final String featureStore = "test";
    final String[] featureNames = new String[]{"powpularityS", "c3", "original", "dvIntFieldFeature",
        "dvLongFieldFeature", "dvFloatFieldFeature", "dvDoubleFieldFeature", "dvStrNumFieldFeature", "dvStrBoolFieldFeature"};
    final String jsonModelParams = "{\"weights\":{\"powpularityS\":1.0,\"c3\":1.0,\"original\":0.1," +
        "\"dvIntFieldFeature\":0.1,\"dvLongFieldFeature\":0.1," +
        "\"dvFloatFieldFeature\":0.1,\"dvDoubleFieldFeature\":0.1,\"dvStrNumFieldFeature\":0.1,\"dvStrBoolFieldFeature\":0.1}}";

    loadFeature(
        featureNames[0],
        SolrFeature.class.getName(),
        featureStore,
        "{\"q\":\"{!func}pow(popularity,2)\"}"
    );
    loadFeature(
        featureNames[1],
        ValueFeature.class.getName(),
        featureStore,
        "{\"value\":2}"
    );
    loadFeature(
        featureNames[2],
        OriginalScoreFeature.class.getName(),
        featureStore,
        null
    );
    loadFeature(
        featureNames[3],
        FieldValueFeature.class.getName(),
        featureStore,
        "{\"field\":\"dvIntField\"}"
    );
    loadFeature(
        featureNames[4],
        FieldValueFeature.class.getName(),
        featureStore,
        "{\"field\":\"dvLongField\"}"
    );
    loadFeature(
        featureNames[5],
        FieldValueFeature.class.getName(),
        featureStore,
        "{\"field\":\"dvFloatField\"}"
    );
    loadFeature(
        featureNames[6],
        FieldValueFeature.class.getName(),
        featureStore,
        "{\"field\":\"dvDoubleField\",\"defaultValue\":-4.0}"
    );
    loadFeature(
        featureNames[7],
        FieldValueFeature.class.getName(),
        featureStore,
        "{\"field\":\"dvStrNumField\",\"defaultValue\":-5}"
    );
    loadFeature(
        featureNames[8],
        FieldValueFeature.class.getName(),
        featureStore,
        "{\"field\":\"dvStrBoolField\"}"
    );

    loadModel(
        "powpularityS-model",
        LinearModel.class.getName(),
        featureNames,
        featureStore,
        jsonModelParams
    );
    reloadCollection(COLLECTION);
  }

  private void reloadCollection(String collection) throws Exception {
    CollectionAdminRequest.Reload reloadRequest = CollectionAdminRequest.reloadCollection(collection);
    CollectionAdminResponse response = reloadRequest.process(solrCluster.getSolrClient());
    assertEquals(0, response.getStatus());
    assertTrue(response.isSuccess());
  }

  @AfterClass
  public static void after() throws Exception {
    if (null != tmpSolrHome) {
      FileUtils.deleteDirectory(tmpSolrHome);
      tmpSolrHome = null;
    }
    System.clearProperty("managed.schema.mutable");
  }

}
