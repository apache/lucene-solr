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
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.RoutingRule;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.util.TimeOut;
import org.apache.zookeeper.KeeperException;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@LuceneTestCase.Slow
public class MigrateRouteKeyTest extends SolrCloudTestCase {

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(2)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();

    if (usually()) {
      CollectionAdminRequest.setClusterProperty("legacyCloud", "false").process(cluster.getSolrClient());
      log.info("Using legacyCloud=false for cluster");
    }
  }

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private boolean waitForRuleToExpire(String collection, String shard, String splitKey, long finishTime) throws KeeperException, InterruptedException, SolrServerException, IOException {
    DocCollection state;
    Slice slice;
    boolean ruleRemoved = false;
    long expiryTime = finishTime + TimeUnit.NANOSECONDS.convert(60, TimeUnit.SECONDS);
    while (System.nanoTime() < expiryTime) {
      cluster.getSolrClient().getZkStateReader().forceUpdateCollection(collection);
      state = getCollectionState(collection);
      slice = state.getSlice(shard);
      Map<String,RoutingRule> routingRules = slice.getRoutingRules();
      if (routingRules == null || routingRules.isEmpty() || !routingRules.containsKey(splitKey)) {
        ruleRemoved = true;
        break;
      }
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", splitKey + random().nextInt());
      cluster.getSolrClient().add(collection, doc);
      Thread.sleep(1000);
    }
    return ruleRemoved;
  }

  protected void invokeCollectionMigration(CollectionAdminRequest.AsyncCollectionAdminRequest request) throws IOException, SolrServerException, InterruptedException {
    request.processAndWait(cluster.getSolrClient(), 60000);
  }

  @Test
  public void testMissingSplitKey() throws Exception  {
    String sourceCollection = "testMissingSplitKey-source";
    CollectionAdminRequest.createCollection(sourceCollection, "conf", 1, 1)
        .process(cluster.getSolrClient());
    String targetCollection = "testMissingSplitKey-target";
    CollectionAdminRequest.createCollection(targetCollection, "conf", 1, 1)
        .process(cluster.getSolrClient());

    HttpSolrClient.RemoteSolrException remoteSolrException = expectThrows(HttpSolrClient.RemoteSolrException.class,
        "Expected an exception in case split.key is not specified", () -> {
          CollectionAdminRequest.migrateData(sourceCollection, targetCollection, "")
              .setForwardTimeout(45)
              .process(cluster.getSolrClient());
        });
    assertTrue(remoteSolrException.getMessage().contains("split.key cannot be null or empty"));
  }

  @Test
  public void multipleShardMigrateTest() throws Exception  {

    CollectionAdminRequest.createCollection("sourceCollection", "conf", 2, 1).process(cluster.getSolrClient());
    cluster.getSolrClient().setDefaultCollection("sourceCollection");

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
      cluster.getSolrClient().add("sourceCollection", doc);
      if (splitKey.equals(shardKey))
        splitKeyCount[0]++;
    }
    assertTrue(splitKeyCount[0] > 0);

    String targetCollection = "migrate_multipleshardtest_targetCollection";
    CollectionAdminRequest.createCollection(targetCollection, "conf", 1, 1).process(cluster.getSolrClient());

    Indexer indexer = new Indexer(cluster.getSolrClient(), splitKey, 1, 30);
    indexer.start();

    DocCollection state = getCollectionState(targetCollection);
    Replica replica = state.getReplicas().get(0);
    try (HttpSolrClient collectionClient = getHttpSolrClient(replica.getCoreUrl())) {

      SolrQuery solrQuery = new SolrQuery("*:*");
      assertEquals("DocCount on target collection does not match", 0, collectionClient.query(solrQuery).getResults().getNumFound());

      invokeCollectionMigration(
          CollectionAdminRequest.migrateData("sourceCollection", targetCollection, splitKey + "/" + BIT_SEP + "!")
          .setForwardTimeout(45));

      long finishTime = System.nanoTime();

      indexer.join();
      splitKeyCount[0] += indexer.getSplitKeyCount();

      try {
        cluster.getSolrClient().deleteById("a/" + BIT_SEP + "!104");
        splitKeyCount[0]--;
      } catch (Exception e) {
        log.warn("Error deleting document a/{}!104", BIT_SEP, e);
      }
      cluster.getSolrClient().commit();
      collectionClient.commit();

      solrQuery = new SolrQuery("*:*").setRows(1000);
      QueryResponse response = collectionClient.query(solrQuery);
      log.info("Response from target collection: {}", response);
      assertEquals("DocCount on target collection does not match", splitKeyCount[0], response.getResults().getNumFound());

      waitForState("Expected to find routing rule for split key " + splitKey, "sourceCollection", (n, c) -> {
        if (c == null)
          return false;
        Slice shard = c.getSlice("shard2");
        if (shard == null)
          return false;
        if (shard.getRoutingRules() == null || shard.getRoutingRules().isEmpty())
          return false;
        if (shard.getRoutingRules().get(splitKey + "!") == null)
          return false;
        return true;
      });

      boolean ruleRemoved = waitForRuleToExpire("sourceCollection", "shard2", splitKey, finishTime);
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
      TimeOut timeout = new TimeOut(seconds, TimeUnit.SECONDS, TimeSource.NANO_TIME);
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
          log.error("Exception while adding document id: {}", doc.getField("id"), e);
        }
        try {
          Thread.sleep(50);
        } catch (InterruptedException e) {
          return;
        }
      }
    }

    public int getSplitKeyCount() {
      return splitKeyCount;
    }
  }
}
