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
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.RoutingRule;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.Utils;
import org.apache.solr.util.TimeOut;
import org.apache.zookeeper.KeeperException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.solr.cloud.OverseerCollectionMessageHandler.NUM_SLICES;
import static org.apache.solr.common.cloud.ZkStateReader.MAX_SHARDS_PER_NODE;
import static org.apache.solr.common.cloud.ZkStateReader.REPLICATION_FACTOR;

public class MigrateRouteKeyTest extends BasicDistributedZkTest {

  public MigrateRouteKeyTest() {
    schemaString = "schema15.xml";      // we need a string id
  }

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Test
  public void test() throws Exception {
    waitForThingsToLevelOut(15);

    if (usually()) {
      log.info("Using legacyCloud=false for cluster");
      CollectionsAPIDistributedZkTest.setClusterProp(cloudClient, "legacyCloud", "false");
    }
    multipleShardMigrateTest();
    printLayout();
  }

  private boolean waitForRuleToExpire(String splitKey, long finishTime) throws KeeperException, InterruptedException, SolrServerException, IOException {
    ClusterState state;Slice slice;
    boolean ruleRemoved = false;
    long expiryTime = finishTime + TimeUnit.NANOSECONDS.convert(60, TimeUnit.SECONDS);
    while (System.nanoTime() < expiryTime) {
      getCommonCloudSolrClient().getZkStateReader().updateClusterState();
      state = getCommonCloudSolrClient().getZkStateReader().getClusterState();
      slice = state.getSlice(AbstractDistribZkTestBase.DEFAULT_COLLECTION, SHARD2);
      Map<String,RoutingRule> routingRules = slice.getRoutingRules();
      if (routingRules == null || routingRules.isEmpty() || !routingRules.containsKey(splitKey)) {
        ruleRemoved = true;
        break;
      }
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", splitKey + random().nextInt());
      cloudClient.add(doc);
      Thread.sleep(1000);
    }
    return ruleRemoved;
  }

  protected void invokeMigrateApi(String sourceCollection, String splitKey, String targetCollection) throws SolrServerException, IOException {
    cloudClient.setDefaultCollection(sourceCollection);
    CollectionAdminRequest.Migrate migrateRequest = new CollectionAdminRequest.Migrate();
    migrateRequest.setCollectionName(sourceCollection);
    migrateRequest.setTargetCollection(targetCollection);
    migrateRequest.setSplitKey(splitKey);
    migrateRequest.setForwardTimeout(45);
    migrateRequest.process(cloudClient);
  }

  protected void invoke(ModifiableSolrParams params) throws SolrServerException, IOException {
    SolrRequest request = new QueryRequest(params);
    request.setPath("/admin/collections");

    String baseUrl = ((HttpSolrClient) shardToJetty.get(SHARD1).get(0).client.solrClient)
        .getBaseURL();
    baseUrl = baseUrl.substring(0, baseUrl.length() - "collection1".length());

    try (HttpSolrClient baseClient = new HttpSolrClient(baseUrl)) {
      baseClient.setConnectionTimeout(15000);
      baseClient.setSoTimeout(60000 * 5);
      baseClient.request(request);
    }
  }

  private void createCollection(String targetCollection) throws Exception {
    HashMap<String, List<Integer>> collectionInfos = new HashMap<>();

    try (CloudSolrClient client = createCloudClient(null)) {
      Map<String, Object> props = Utils.makeMap(
          REPLICATION_FACTOR, 1,
          MAX_SHARDS_PER_NODE, 5,
          NUM_SLICES, 1);

      createCollection(collectionInfos, targetCollection, props, client);
    }

    List<Integer> list = collectionInfos.get(targetCollection);
    checkForCollection(targetCollection, list, null);

    waitForRecoveriesToFinish(targetCollection, false);
  }

  protected void multipleShardMigrateTest() throws Exception  {
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

    String url = getUrlFromZk(getCommonCloudSolrClient().getZkStateReader().getClusterState(), targetCollection);

    try (HttpSolrClient collectionClient = new HttpSolrClient(url)) {

      SolrQuery solrQuery = new SolrQuery("*:*");
      assertEquals("DocCount on target collection does not match", 0, collectionClient.query(solrQuery).getResults().getNumFound());

      invokeMigrateApi(AbstractDistribZkTestBase.DEFAULT_COLLECTION, splitKey + "/" + BIT_SEP + "!", targetCollection);
      long finishTime = System.nanoTime();

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

      getCommonCloudSolrClient().getZkStateReader().updateClusterState();
      ClusterState state = getCommonCloudSolrClient().getZkStateReader().getClusterState();
      Slice slice = state.getSlice(AbstractDistribZkTestBase.DEFAULT_COLLECTION, SHARD2);
      assertNotNull("Routing rule map is null", slice.getRoutingRules());
      assertFalse("Routing rule map is empty", slice.getRoutingRules().isEmpty());
      assertNotNull("No routing rule exists for route key: " + splitKey, slice.getRoutingRules().get(splitKey + "!"));

      boolean ruleRemoved = waitForRuleToExpire(splitKey, finishTime);
      assertTrue("Routing rule was not expired", ruleRemoved);
    }
  }

  static class Indexer extends Thread {
    final int seconds;
    final CloudSolrClient cloudClient;
    final String splitKey;
    int splitKeyCount = 0;
    final int bitSep;

    public Indexer(CloudSolrClient cloudClient, String splitKey, int bitSep, int seconds) {
      this.seconds = seconds;
      this.cloudClient = cloudClient;
      this.splitKey = splitKey;
      this.bitSep = bitSep;
    }

    @Override
    public void run() {
      TimeOut timeout = new TimeOut(seconds, TimeUnit.SECONDS);
      for (int id = 26*3; id < 500 && ! timeout.hasTimedOut(); id++) {
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
