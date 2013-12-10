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

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.RoutingRule;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.update.DirectUpdateHandler2;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.solr.cloud.OverseerCollectionProcessor.MAX_SHARDS_PER_NODE;
import static org.apache.solr.cloud.OverseerCollectionProcessor.NUM_SLICES;
import static org.apache.solr.cloud.OverseerCollectionProcessor.REPLICATION_FACTOR;

public class MigrateRouteKeyTest extends BasicDistributedZkTest {

  public MigrateRouteKeyTest() {
    schemaString = "schema15.xml";      // we need a string id
  }

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    System.setProperty("numShards", Integer.toString(sliceCount));
    System.setProperty("solr.xml.persist", "true");
  }

  @Override
  @After
  public void tearDown() throws Exception {
    super.tearDown();

    if (VERBOSE || printLayoutOnTearDown) {
      super.printLayout();
    }
    if (controlClient != null) {
      controlClient.shutdown();
    }
    if (cloudClient != null) {
      cloudClient.shutdown();
    }
    if (controlClientCloud != null) {
      controlClientCloud.shutdown();
    }
    super.tearDown();

    System.clearProperty("zkHost");
    System.clearProperty("numShards");
    System.clearProperty("solr.xml.persist");

    // insurance
    DirectUpdateHandler2.commitOnClose = true;
  }

  @Override
  public void doTest() throws Exception {
    waitForThingsToLevelOut(15);

    multipleShardMigrateTest();
    printLayout();
  }

  private boolean waitForRuleToExpire(String splitKey, long finishTime) throws KeeperException, InterruptedException, SolrServerException, IOException {
    ClusterState state;Slice slice;
    boolean ruleRemoved = false;
    while (System.currentTimeMillis() - finishTime < 60000) {
      getCommonCloudSolrServer().getZkStateReader().updateClusterState(true);
      state = getCommonCloudSolrServer().getZkStateReader().getClusterState();
      slice = state.getSlice(AbstractDistribZkTestBase.DEFAULT_COLLECTION, SHARD2);
      Map<String,RoutingRule> routingRules = slice.getRoutingRules();
      if (routingRules == null || routingRules.isEmpty() || !routingRules.containsKey(splitKey)) {
        ruleRemoved = true;
        break;
      }
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", splitKey + System.currentTimeMillis());
      cloudClient.add(doc);
      Thread.sleep(1000);
    }
    return ruleRemoved;
  }

  private void invokeMigrateApi(String sourceCollection, String splitKey, String targetCollection) throws SolrServerException, IOException {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("action", CollectionParams.CollectionAction.MIGRATE.toString());
    params.set("collection", sourceCollection);
    params.set("target.collection", targetCollection);
    params.set("split.key", splitKey);
    params.set("forward.timeout", 45);

    invoke(params);
  }

  private void invoke(ModifiableSolrParams params) throws SolrServerException, IOException {
    SolrRequest request = new QueryRequest(params);
    request.setPath("/admin/collections");

    String baseUrl = ((HttpSolrServer) shardToJetty.get(SHARD1).get(0).client.solrClient)
        .getBaseURL();
    baseUrl = baseUrl.substring(0, baseUrl.length() - "collection1".length());

    HttpSolrServer baseServer = new HttpSolrServer(baseUrl);
    baseServer.setConnectionTimeout(15000);
    baseServer.setSoTimeout(60000 * 5);
    baseServer.request(request);
    baseServer.shutdown();
  }

  private void createCollection(String targetCollection) throws Exception {
    HashMap<String, List<Integer>> collectionInfos = new HashMap<String, List<Integer>>();
    CloudSolrServer client = null;
    try {
      client = createCloudClient(null);
      Map<String, Object> props = ZkNodeProps.makeMap(
          REPLICATION_FACTOR, 1,
          MAX_SHARDS_PER_NODE, 5,
          NUM_SLICES, 1);

      createCollection(collectionInfos, targetCollection, props, client);
    } finally {
      if (client != null) client.shutdown();
    }

    List<Integer> list = collectionInfos.get(targetCollection);
    checkForCollection(targetCollection, list, null);

    waitForRecoveriesToFinish(targetCollection, false);
  }

  private void multipleShardMigrateTest() throws Exception  {
    del("*:*");
    commit();
    assertTrue(cloudClient.query(new SolrQuery("*:*")).getResults().getNumFound() == 0);
    final String splitKey = "a";
    final int BIT_SEP = 1;
    final int[] splitKeyCount = new int[1];
    for (int id = 0; id < 26*3; id++) {
      String shardKey = "" + (char) ('a' + (id % 26)); // See comment in ShardRoutingTest for hash distribution
      String key = shardKey;
      if (splitKey.equals(shardKey))  {
        key += "/" + BIT_SEP;  // spread it over half the collection
      }
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", key + "!" + id);
      doc.addField("n_ti", id);
      cloudClient.add(doc);
      if (splitKey.equals(shardKey))
        splitKeyCount[0]++;
    }
    assertTrue(splitKeyCount[0] > 0);

    String targetCollection = "migrate_multipleshardtest_targetCollection";
    createCollection(targetCollection);

    Indexer indexer = new Indexer(cloudClient, splitKey, 1, 30);
    indexer.start();

    String url = CustomCollectionTest.getUrlFromZk(getCommonCloudSolrServer().getZkStateReader().getClusterState(), targetCollection);
    HttpSolrServer collectionClient = new HttpSolrServer(url);

    SolrQuery solrQuery = new SolrQuery("*:*");
    assertEquals("DocCount on target collection does not match", 0, collectionClient.query(solrQuery).getResults().getNumFound());

    invokeMigrateApi(AbstractDistribZkTestBase.DEFAULT_COLLECTION, splitKey + "/" + BIT_SEP + "!", targetCollection);
    long finishTime = System.currentTimeMillis();

    indexer.join();
    splitKeyCount[0] += indexer.getSplitKeyCount();

    try {
      cloudClient.deleteById("a/" + BIT_SEP + "!104");
      splitKeyCount[0]--;
    } catch (Exception e) {
      log.warn("Error deleting document a/" + BIT_SEP + "!104", e);
    }
    cloudClient.commit();
    collectionClient.commit();

    solrQuery = new SolrQuery("*:*").setRows(1000);
    QueryResponse response = collectionClient.query(solrQuery);
    log.info("Response from target collection: " + response);
    assertEquals("DocCount on target collection does not match", splitKeyCount[0], response.getResults().getNumFound());

    getCommonCloudSolrServer().getZkStateReader().updateClusterState(true);
    ClusterState state = getCommonCloudSolrServer().getZkStateReader().getClusterState();
    Slice slice = state.getSlice(AbstractDistribZkTestBase.DEFAULT_COLLECTION, SHARD2);
    assertNotNull("Routing rule map is null", slice.getRoutingRules());
    assertFalse("Routing rule map is empty", slice.getRoutingRules().isEmpty());
    assertNotNull("No routing rule exists for route key: " + splitKey, slice.getRoutingRules().get(splitKey + "!"));

    boolean ruleRemoved = waitForRuleToExpire(splitKey, finishTime);
    assertTrue("Routing rule was not expired", ruleRemoved);
  }

  static class Indexer extends Thread {
    final int seconds;
    final CloudSolrServer cloudClient;
    final String splitKey;
    int splitKeyCount = 0;
    final int bitSep;

    public Indexer(CloudSolrServer cloudClient, String splitKey, int bitSep, int seconds) {
      this.seconds = seconds;
      this.cloudClient = cloudClient;
      this.splitKey = splitKey;
      this.bitSep = bitSep;
    }

    @Override
    public void run() {
      long start = System.currentTimeMillis();
      for (int id = 26*3; id < 500 && System.currentTimeMillis() - start <= seconds*1000; id++) {
        String shardKey = "" + (char) ('a' + (id % 26)); // See comment in ShardRoutingTest for hash distribution
        SolrInputDocument doc = new SolrInputDocument();
        doc.addField("id", shardKey + (bitSep != -1 ? "/" + bitSep : "") + "!" + id);
        doc.addField("n_ti", id);
        try {
          cloudClient.add(doc);
          if (splitKey.equals(shardKey))
            splitKeyCount++;
        } catch (Exception e) {
          log.error("Exception while adding document id: " + doc.getField("id"), e);
        }
        try {
          Thread.sleep(50);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    }

    public int getSplitKeyCount() {
      return splitKeyCount;
    }
  }
}
