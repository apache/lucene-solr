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
package org.apache.solr.search.stats;

import java.io.IOException;
import java.lang.invoke.MethodHandles;

import org.apache.lucene.util.TestUtil;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.CompositeIdRouter;
import org.apache.solr.common.cloud.ImplicitDocRouter;
import org.apache.solr.common.params.ShardParams;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestDistribIDF extends SolrTestCaseJ4 {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private MiniSolrCloudCluster solrCluster;

  @Override
  public void setUp() throws Exception {
    if (random().nextBoolean()) {
      System.setProperty("solr.statsCache", ExactStatsCache.class.getName());
    } else {
      System.setProperty("solr.statsCache", LRUStatsCache.class.getName());
    }

    super.setUp();
    solrCluster = new MiniSolrCloudCluster(3, createTempDir(), buildJettyConfig("/solr"));
    // set some system properties for use by tests
    System.setProperty("solr.test.sys.prop1", "propone");
    System.setProperty("solr.test.sys.prop2", "proptwo");
    solrCluster.uploadConfigSet(TEST_PATH().resolve("collection1/conf"), "conf1");
    solrCluster.uploadConfigSet(configset("configset-2"), "conf2");
  }

  @Override
  public void tearDown() throws Exception {
    solrCluster.shutdown();
    System.clearProperty("solr.statsCache");
    System.clearProperty("solr.test.sys.prop1");
    System.clearProperty("solr.test.sys.prop2");
    super.tearDown();
  }

  @Test
  public void testSimpleQuery() throws Exception {
    //3 shards. 3rd shard won't have any data.
    createCollection("onecollection", "conf1", ImplicitDocRouter.NAME);
    createCollection("onecollection_local", "conf2", ImplicitDocRouter.NAME);

    SolrInputDocument doc = new SolrInputDocument();
    doc.setField("id", "1");
    doc.setField("cat", "football");
    doc.addField(ShardParams._ROUTE_, "a");
    solrCluster.getSolrClient().add("onecollection", doc);
    solrCluster.getSolrClient().add("onecollection_local", doc);

    doc = new SolrInputDocument();
    doc.setField("id", "2");
    doc.setField("cat", "football");
    doc.addField(ShardParams._ROUTE_, "b");
    solrCluster.getSolrClient().add("onecollection", doc);
    solrCluster.getSolrClient().add("onecollection_local", doc);

    int nDocs = TestUtil.nextInt(random(), 10, 100);
    for (int i=0; i<nDocs; i++) {
      doc = new SolrInputDocument();
      doc.setField("id", "" + (3 + i));
      String cat = TestUtil.randomSimpleString(random());
      if (!cat.equals("football")) { //Making sure no other document has the query term in it.
        doc.setField("cat", cat);
        if (rarely()) { //Put most documents in shard b so that 'football' becomes 'rare' in shard b
          doc.addField(ShardParams._ROUTE_, "a");
        } else {
          doc.addField(ShardParams._ROUTE_, "b");
        }
        solrCluster.getSolrClient().add("onecollection", doc);
        solrCluster.getSolrClient().add("onecollection_local", doc);
      }
    }

    solrCluster.getSolrClient().commit("onecollection");
    solrCluster.getSolrClient().commit("onecollection_local");

    //Test against all nodes
    for (JettySolrRunner jettySolrRunner : solrCluster.getJettySolrRunners()) {
      try (SolrClient solrClient = getHttpSolrClient(jettySolrRunner.getBaseUrl().toString())) {
        try (SolrClient solrClient_local = getHttpSolrClient(jettySolrRunner.getBaseUrl().toString())) {

          SolrQuery query = new SolrQuery("cat:football");
          query.setFields("*,score");
          QueryResponse queryResponse = solrClient.query("onecollection", query);
          assertEquals(2, queryResponse.getResults().getNumFound());
          float score1 = (float) queryResponse.getResults().get(0).get("score");
          float score2 = (float) queryResponse.getResults().get(1).get("score");
          assertEquals("Doc1 score=" + score1 + " Doc2 score=" + score2, 0, Float.compare(score1, score2));

          query = new SolrQuery("cat:football");
          query.setShowDebugInfo(true);
          query.setFields("*,score");
          queryResponse = solrClient_local.query("onecollection_local", query);
          assertEquals(2, queryResponse.getResults().getNumFound());
          assertEquals("2", queryResponse.getResults().get(0).get("id"));
          assertEquals("1", queryResponse.getResults().get(1).get("id"));
          float score1_local = (float) queryResponse.getResults().get(0).get("score");
          float score2_local = (float) queryResponse.getResults().get(1).get("score");
          assertEquals("Doc1 score=" + score1_local + " Doc2 score=" + score2_local, 1,
              Float.compare(score1_local, score2_local));
        }
      }
    }
  }

  @Test
  // commented 4-Sep-2018   @LuceneTestCase.BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 2-Aug-2018
  // commented out on: 17-Feb-2019   @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 14-Oct-2018
  public void testMultiCollectionQuery() throws Exception {
    // collection1 and collection2 are collections which have distributed idf enabled
    // collection1_local and collection2_local don't have distributed idf available
    // Only one doc has cat:football in each collection
    // When doing queries across collections we want to test that the query takes into account
    // distributed idf for the collection=collection1,collection2 query.
    // The way we verify is that score should be the same when querying across collection1 and collection2
    // But should be different when querying across collection1_local and collection2_local
    // since the idf is calculated per shard

    createCollection("collection1", "conf1");
    createCollection("collection1_local", "conf2");
    createCollection("collection2", "conf1");
    createCollection("collection2_local", "conf2");

    addDocsRandomly();

    //Test against all nodes
    for (JettySolrRunner jettySolrRunner : solrCluster.getJettySolrRunners()) {

      try (SolrClient solrClient = getHttpSolrClient(jettySolrRunner.getBaseUrl().toString())) {

        try (SolrClient solrClient_local = getHttpSolrClient(jettySolrRunner.getBaseUrl().toString())) {
          SolrQuery query = new SolrQuery("cat:football");
          query.setFields("*,score").add("collection", "collection1,collection2");
          QueryResponse queryResponse = solrClient.query("collection1", query);
          assertEquals(2, queryResponse.getResults().getNumFound());
          float score1 = (float) queryResponse.getResults().get(0).get("score");
          float score2 = (float) queryResponse.getResults().get(1).get("score");
          assertEquals("Doc1 score=" + score1 + " Doc2 score=" + score2, 0, Float.compare(score1, score2));

          query = new SolrQuery("cat:football");
          query.setFields("*,score").add("collection", "collection1_local,collection2_local");
          queryResponse = solrClient_local.query("collection1_local", query);
          assertEquals(2, queryResponse.getResults().getNumFound());
          assertEquals("2", queryResponse.getResults().get(0).get("id"));
          assertEquals("1", queryResponse.getResults().get(1).get("id"));
          float score1_local = (float) queryResponse.getResults().get(0).get("score");
          float score2_local = (float) queryResponse.getResults().get(1).get("score");
          assertEquals("Doc1 score=" + score1_local + " Doc2 score=" + score2_local, 1,
              Float.compare(score1_local, score2_local));
        }

      }
    }
    
  }

  private void createCollection(String name, String config) throws Exception {
    createCollection(name, config, CompositeIdRouter.NAME);
  }

  private void createCollection(String name, String config, String router) throws Exception {
    CollectionAdminResponse response;
    if (router.equals(ImplicitDocRouter.NAME)) {
      CollectionAdminRequest.Create create = CollectionAdminRequest.createCollectionWithImplicitRouter(name,config,"a,b,c",1);
      create.setMaxShardsPerNode(1);
      response = create.process(solrCluster.getSolrClient());
      solrCluster.waitForActiveCollection(name, 3, 3);
    } else {
      CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(name,config,2,1).setPerReplicaState(SolrCloudTestCase.USE_PER_REPLICA_STATE);
      create.setMaxShardsPerNode(1);
      response = create.process(solrCluster.getSolrClient());
      solrCluster.waitForActiveCollection(name, 2, 2);
    }

    if (response.getStatus() != 0 || response.getErrorMessages() != null) {
      fail("Could not create collection. Response" + response.toString());
    }
  }

  private void addDocsRandomly() throws IOException, SolrServerException {
    SolrInputDocument doc = new SolrInputDocument();
    doc.setField("id", 1);
    doc.setField("cat", "football");
    solrCluster.getSolrClient().add("collection1", doc);
    solrCluster.getSolrClient().add("collection1_local", doc);

    doc = new SolrInputDocument();
    doc.setField("id", 2);
    doc.setField("cat", "football");
    solrCluster.getSolrClient().add("collection2", doc);
    solrCluster.getSolrClient().add("collection2_local", doc);

    int nDocs = TestUtil.nextInt(random(), 10, 100);
    int collection1Count = 1;
    int collection2Count = 1;
    for (int i=0; i<nDocs; i++) {
      doc = new SolrInputDocument();
      doc.setField("id", 3 + i);
      String cat = TestUtil.randomSimpleString(random());
      if (!cat.equals("football")) { //Making sure no other document has the query term in it.
        doc.setField("cat", cat);
        if (rarely()) { //Put most documents in collection2* so that 'football' becomes 'rare' in collection2*
          solrCluster.getSolrClient().add("collection1", doc);
          solrCluster.getSolrClient().add("collection1_local", doc);
          collection1Count++;
        } else {
          solrCluster.getSolrClient().add("collection2", doc);
          solrCluster.getSolrClient().add("collection2_local", doc);
          collection2Count++;
        }
      }
    }
    log.info("numDocs={}. collection1Count={} collection2Count={}", nDocs, collection1Count, collection2Count);

    solrCluster.getSolrClient().commit("collection1");
    solrCluster.getSolrClient().commit("collection2");
    solrCluster.getSolrClient().commit("collection1_local");
    solrCluster.getSolrClient().commit("collection2_local");
  }
}
