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

import org.apache.http.params.CoreConnectionPNames;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.CompositeIdRouter;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.cloud.HashBasedRouter;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.apache.solr.cloud.OverseerCollectionProcessor.NUM_SLICES;
import static org.apache.solr.common.cloud.ZkStateReader.MAX_SHARDS_PER_NODE;
import static org.apache.solr.common.cloud.ZkStateReader.REPLICATION_FACTOR;

@Slow
public class ShardSplitTest extends BasicDistributedZkTest {

  public static final String SHARD1_0 = SHARD1 + "_0";
  public static final String SHARD1_1 = SHARD1 + "_1";

  public ShardSplitTest() {
    schemaString = "schema15.xml";      // we need a string id
  }

  @Override
  public void distribSetUp() throws Exception {
    super.distribSetUp();
    System.setProperty("numShards", Integer.toString(sliceCount));
    System.setProperty("solr.xml.persist", "true");
  }

  @Test
  public void test() throws Exception {

    waitForThingsToLevelOut(15);

    if (usually()) {
      log.info("Using legacyCloud=false for cluster");
      CollectionsAPIDistributedZkTest.setClusterProp(cloudClient, "legacyCloud", "false");
    }
    incompleteOrOverlappingCustomRangeTest();
    splitByUniqueKeyTest();
    splitByRouteFieldTest();
    splitByRouteKeyTest();

    // todo can't call waitForThingsToLevelOut because it looks for jettys of all shards
    // and the new sub-shards don't have any.
    waitForRecoveriesToFinish(true);
    //waitForThingsToLevelOut(15);

  }

  private void incompleteOrOverlappingCustomRangeTest() throws Exception  {
    ClusterState clusterState = cloudClient.getZkStateReader().getClusterState();
    final DocRouter router = clusterState.getCollection(AbstractDistribZkTestBase.DEFAULT_COLLECTION).getRouter();
    Slice shard1 = clusterState.getSlice(AbstractDistribZkTestBase.DEFAULT_COLLECTION, SHARD1);
    DocRouter.Range shard1Range = shard1.getRange() != null ? shard1.getRange() : router.fullRange();

    List<DocRouter.Range> subRanges = new ArrayList<>();
    List<DocRouter.Range> ranges = router.partitionRange(4, shard1Range);

    // test with only one range
    subRanges.add(ranges.get(0));
    try {
      splitShard(AbstractDistribZkTestBase.DEFAULT_COLLECTION, SHARD1, subRanges, null);
      fail("Shard splitting with just one custom hash range should not succeed");
    } catch (HttpSolrClient.RemoteSolrException e) {
      log.info("Expected exception:", e);
    }
    subRanges.clear();

    // test with ranges with a hole in between them
    subRanges.add(ranges.get(3)); // order shouldn't matter
    subRanges.add(ranges.get(0));
    try {
      splitShard(AbstractDistribZkTestBase.DEFAULT_COLLECTION, SHARD1, subRanges, null);
      fail("Shard splitting with missing hashes in between given ranges should not succeed");
    } catch (HttpSolrClient.RemoteSolrException e) {
      log.info("Expected exception:", e);
    }
    subRanges.clear();

    // test with overlapping ranges
    subRanges.add(ranges.get(0));
    subRanges.add(ranges.get(1));
    subRanges.add(ranges.get(2));
    subRanges.add(new DocRouter.Range(ranges.get(3).min - 15, ranges.get(3).max));
    try {
      splitShard(AbstractDistribZkTestBase.DEFAULT_COLLECTION, SHARD1, subRanges, null);
      fail("Shard splitting with overlapping ranges should not succeed");
    } catch (HttpSolrClient.RemoteSolrException e) {
      log.info("Expected exception:", e);
    }
    subRanges.clear();
  }

  private void splitByUniqueKeyTest() throws Exception {
    ClusterState clusterState = cloudClient.getZkStateReader().getClusterState();
    final DocRouter router = clusterState.getCollection(AbstractDistribZkTestBase.DEFAULT_COLLECTION).getRouter();
    Slice shard1 = clusterState.getSlice(AbstractDistribZkTestBase.DEFAULT_COLLECTION, SHARD1);
    DocRouter.Range shard1Range = shard1.getRange() != null ? shard1.getRange() : router.fullRange();
    List<DocRouter.Range> subRanges = new ArrayList<>();
    if (usually())  {
      List<DocRouter.Range> ranges = router.partitionRange(4, shard1Range);
      // 75% of range goes to shard1_0 and the rest to shard1_1
      subRanges.add(new DocRouter.Range(ranges.get(0).min, ranges.get(2).max));
      subRanges.add(ranges.get(3));
    } else  {
      subRanges = router.partitionRange(2, shard1Range);
    }
    final List<DocRouter.Range> ranges = subRanges;
    final int[] docCounts = new int[ranges.size()];
    int numReplicas = shard1.getReplicas().size();

    del("*:*");
    for (int id = 0; id <= 100; id++) {
      String shardKey = "" + (char)('a' + (id % 26)); // See comment in ShardRoutingTest for hash distribution
      indexAndUpdateCount(router, ranges, docCounts, shardKey + "!" + String.valueOf(id), id);
    }
    commit();

    Thread indexThread = new Thread() {
      @Override
      public void run() {
        Random random = random();
        int max = atLeast(random, 401);
        int sleep = atLeast(random, 25);
        log.info("SHARDSPLITTEST: Going to add " + max + " number of docs at 1 doc per " + sleep + "ms");
        Set<String> deleted = new HashSet<>();
        for (int id = 101; id < max; id++) {
          try {
            indexAndUpdateCount(router, ranges, docCounts, String.valueOf(id), id);
            Thread.sleep(sleep);
            if (usually(random))  {
              String delId = String.valueOf(random.nextInt(id - 101 + 1) + 101);
              if (deleted.contains(delId))  continue;
              try {
                deleteAndUpdateCount(router, ranges, docCounts, delId);
                deleted.add(delId);
              } catch (Exception e) {
                log.error("Exception while deleting docs", e);
              }
            }
          } catch (Exception e) {
            log.error("Exception while adding doc id = " + id, e);
            // do not select this id for deletion ever
            deleted.add(String.valueOf(id));
          }
        }
      }
    };
    indexThread.start();

    try {
      for (int i = 0; i < 3; i++) {
        try {
          splitShard(AbstractDistribZkTestBase.DEFAULT_COLLECTION, SHARD1, subRanges, null);
          log.info("Layout after split: \n");
          printLayout();
          break;
        } catch (HttpSolrClient.RemoteSolrException e) {
          if (e.code() != 500)  {
            throw e;
          }
          log.error("SPLITSHARD failed. " + (i < 2 ? " Retring split" : ""), e);
          if (i == 2) {
            fail("SPLITSHARD was not successful even after three tries");
          }
        }
      }
    } finally {
      try {
        indexThread.join();
      } catch (InterruptedException e) {
        log.error("Indexing thread interrupted", e);
      }
    }

    waitForRecoveriesToFinish(true);
    checkDocCountsAndShardStates(docCounts, numReplicas);
  }


  public void splitByRouteFieldTest() throws Exception  {
    log.info("Starting testSplitWithRouteField");
    String collectionName = "routeFieldColl";
    int numShards = 4;
    int replicationFactor = 2;
    int maxShardsPerNode = (((numShards * replicationFactor) / getCommonCloudSolrClient()
        .getZkStateReader().getClusterState().getLiveNodes().size())) + 1;

    HashMap<String, List<Integer>> collectionInfos = new HashMap<>();
    String shard_fld = "shard_s";
    try (CloudSolrClient client = createCloudClient(null)) {
      Map<String, Object> props = ZkNodeProps.makeMap(
          REPLICATION_FACTOR, replicationFactor,
          MAX_SHARDS_PER_NODE, maxShardsPerNode,
          NUM_SLICES, numShards,
          "router.field", shard_fld);

      createCollection(collectionInfos, collectionName,props,client);
    }

    List<Integer> list = collectionInfos.get(collectionName);
    checkForCollection(collectionName, list, null);

    waitForRecoveriesToFinish(false);

    String url = CustomCollectionTest.getUrlFromZk(getCommonCloudSolrClient().getZkStateReader().getClusterState(), collectionName);

    try (HttpSolrClient collectionClient = new HttpSolrClient(url)) {

      ClusterState clusterState = cloudClient.getZkStateReader().getClusterState();
      final DocRouter router = clusterState.getCollection(collectionName).getRouter();
      Slice shard1 = clusterState.getSlice(collectionName, SHARD1);
      DocRouter.Range shard1Range = shard1.getRange() != null ? shard1.getRange() : router.fullRange();
      final List<DocRouter.Range> ranges = router.partitionRange(2, shard1Range);
      final int[] docCounts = new int[ranges.size()];

      for (int i = 100; i <= 200; i++) {
        String shardKey = "" + (char) ('a' + (i % 26)); // See comment in ShardRoutingTest for hash distribution

        collectionClient.add(getDoc(id, i, "n_ti", i, shard_fld, shardKey));
        int idx = getHashRangeIdx(router, ranges, shardKey);
        if (idx != -1) {
          docCounts[idx]++;
        }
      }

      for (int i = 0; i < docCounts.length; i++) {
        int docCount = docCounts[i];
        log.info("Shard {} docCount = {}", "shard1_" + i, docCount);
      }

      collectionClient.commit();

      for (int i = 0; i < 3; i++) {
        try {
          splitShard(collectionName, SHARD1, null, null);
          break;
        } catch (HttpSolrClient.RemoteSolrException e) {
          if (e.code() != 500) {
            throw e;
          }
          log.error("SPLITSHARD failed. " + (i < 2 ? " Retring split" : ""), e);
          if (i == 2) {
            fail("SPLITSHARD was not successful even after three tries");
          }
        }
      }

      waitForRecoveriesToFinish(collectionName, false);

      assertEquals(docCounts[0], collectionClient.query(new SolrQuery("*:*").setParam("shards", "shard1_0")).getResults().getNumFound());
      assertEquals(docCounts[1], collectionClient.query(new SolrQuery("*:*").setParam("shards", "shard1_1")).getResults().getNumFound());
    }
  }

  private void splitByRouteKeyTest() throws Exception {
    log.info("Starting splitByRouteKeyTest");
    String collectionName = "splitByRouteKeyTest";
    int numShards = 4;
    int replicationFactor = 2;
    int maxShardsPerNode = (((numShards * replicationFactor) / getCommonCloudSolrClient()
        .getZkStateReader().getClusterState().getLiveNodes().size())) + 1;

    HashMap<String, List<Integer>> collectionInfos = new HashMap<>();

    try (CloudSolrClient client = createCloudClient(null)) {
      Map<String, Object> props = ZkNodeProps.makeMap(
          REPLICATION_FACTOR, replicationFactor,
          MAX_SHARDS_PER_NODE, maxShardsPerNode,
          NUM_SLICES, numShards);

      createCollection(collectionInfos, collectionName,props,client);
    }

    List<Integer> list = collectionInfos.get(collectionName);
    checkForCollection(collectionName, list, null);

    waitForRecoveriesToFinish(false);

    String url = CustomCollectionTest.getUrlFromZk(getCommonCloudSolrClient().getZkStateReader().getClusterState(), collectionName);

    try (HttpSolrClient collectionClient = new HttpSolrClient(url)) {

      String splitKey = "b!";

      ClusterState clusterState = cloudClient.getZkStateReader().getClusterState();
      final DocRouter router = clusterState.getCollection(collectionName).getRouter();
      Slice shard1 = clusterState.getSlice(collectionName, SHARD1);
      DocRouter.Range shard1Range = shard1.getRange() != null ? shard1.getRange() : router.fullRange();
      final List<DocRouter.Range> ranges = ((CompositeIdRouter) router).partitionRangeByKey(splitKey, shard1Range);
      final int[] docCounts = new int[ranges.size()];

      int uniqIdentifier = (1 << 12);
      int splitKeyDocCount = 0;
      for (int i = 100; i <= 200; i++) {
        String shardKey = "" + (char) ('a' + (i % 26)); // See comment in ShardRoutingTest for hash distribution

        String idStr = shardKey + "!" + i;
        collectionClient.add(getDoc(id, idStr, "n_ti", (shardKey + "!").equals(splitKey) ? uniqIdentifier : i));
        int idx = getHashRangeIdx(router, ranges, idStr);
        if (idx != -1) {
          docCounts[idx]++;
        }
        if (splitKey.equals(shardKey + "!"))
          splitKeyDocCount++;
      }

      for (int i = 0; i < docCounts.length; i++) {
        int docCount = docCounts[i];
        log.info("Shard {} docCount = {}", "shard1_" + i, docCount);
      }
      log.info("Route key doc count = {}", splitKeyDocCount);

      collectionClient.commit();

      for (int i = 0; i < 3; i++) {
        try {
          splitShard(collectionName, null, null, splitKey);
          break;
        } catch (HttpSolrClient.RemoteSolrException e) {
          if (e.code() != 500) {
            throw e;
          }
          log.error("SPLITSHARD failed. " + (i < 2 ? " Retring split" : ""), e);
          if (i == 2) {
            fail("SPLITSHARD was not successful even after three tries");
          }
        }
      }

      waitForRecoveriesToFinish(collectionName, false);
      SolrQuery solrQuery = new SolrQuery("*:*");
      assertEquals("DocCount on shard1_0 does not match", docCounts[0], collectionClient.query(solrQuery.setParam("shards", "shard1_0")).getResults().getNumFound());
      assertEquals("DocCount on shard1_1 does not match", docCounts[1], collectionClient.query(solrQuery.setParam("shards", "shard1_1")).getResults().getNumFound());
      assertEquals("DocCount on shard1_2 does not match", docCounts[2], collectionClient.query(solrQuery.setParam("shards", "shard1_2")).getResults().getNumFound());

      solrQuery = new SolrQuery("n_ti:" + uniqIdentifier);
      assertEquals("shard1_0 must have 0 docs for route key: " + splitKey, 0, collectionClient.query(solrQuery.setParam("shards", "shard1_0")).getResults().getNumFound());
      assertEquals("Wrong number of docs on shard1_1 for route key: " + splitKey, splitKeyDocCount, collectionClient.query(solrQuery.setParam("shards", "shard1_1")).getResults().getNumFound());
      assertEquals("shard1_2 must have 0 docs for route key: " + splitKey, 0, collectionClient.query(solrQuery.setParam("shards", "shard1_2")).getResults().getNumFound());
    }
  }

  protected void checkDocCountsAndShardStates(int[] docCounts, int numReplicas) throws Exception {
    ClusterState clusterState = null;
    Slice slice1_0 = null, slice1_1 = null;
    int i = 0;
    for (i = 0; i < 10; i++) {
      ZkStateReader zkStateReader = cloudClient.getZkStateReader();
      zkStateReader.updateClusterState(true);
      clusterState = zkStateReader.getClusterState();
      slice1_0 = clusterState.getSlice(AbstractDistribZkTestBase.DEFAULT_COLLECTION, "shard1_0");
      slice1_1 = clusterState.getSlice(AbstractDistribZkTestBase.DEFAULT_COLLECTION, "shard1_1");
      if (Slice.ACTIVE.equals(slice1_0.getState()) && Slice.ACTIVE.equals(slice1_1.getState()))
        break;
      Thread.sleep(500);
    }

    log.info("ShardSplitTest waited for {} ms for shard state to be set to active", i * 500);

    assertNotNull("Cluster state does not contain shard1_0", slice1_0);
    assertNotNull("Cluster state does not contain shard1_0", slice1_1);
    assertEquals("shard1_0 is not active", Slice.ACTIVE, slice1_0.getState());
    assertEquals("shard1_1 is not active", Slice.ACTIVE, slice1_1.getState());
    assertEquals("Wrong number of replicas created for shard1_0", numReplicas, slice1_0.getReplicas().size());
    assertEquals("Wrong number of replicas created for shard1_1", numReplicas, slice1_1.getReplicas().size());

    commit();

    // can't use checkShardConsistency because it insists on jettys and clients for each shard
    checkSubShardConsistency(SHARD1_0);
    checkSubShardConsistency(SHARD1_1);

    SolrQuery query = new SolrQuery("*:*").setRows(1000).setFields("id", "_version_");
    query.set("distrib", false);

    ZkCoreNodeProps shard1_0 = getLeaderUrlFromZk(AbstractDistribZkTestBase.DEFAULT_COLLECTION, SHARD1_0);
    QueryResponse response;
    try (HttpSolrClient shard1_0Client = new HttpSolrClient(shard1_0.getCoreUrl())) {
      response = shard1_0Client.query(query);
    }
    long shard10Count = response.getResults().getNumFound();

    ZkCoreNodeProps shard1_1 = getLeaderUrlFromZk(
        AbstractDistribZkTestBase.DEFAULT_COLLECTION, SHARD1_1);
    QueryResponse response2;
    try (HttpSolrClient shard1_1Client = new HttpSolrClient(shard1_1.getCoreUrl())) {
      response2 = shard1_1Client.query(query);
    }
    long shard11Count = response2.getResults().getNumFound();

    logDebugHelp(docCounts, response, shard10Count, response2, shard11Count);

    assertEquals("Wrong doc count on shard1_0. See SOLR-5309", docCounts[0], shard10Count);
    assertEquals("Wrong doc count on shard1_1. See SOLR-5309", docCounts[1], shard11Count);
  }

  protected void checkSubShardConsistency(String shard) throws SolrServerException, IOException {
    SolrQuery query = new SolrQuery("*:*").setRows(1000).setFields("id", "_version_");
    query.set("distrib", false);

    ClusterState clusterState = cloudClient.getZkStateReader().getClusterState();
    Slice slice = clusterState.getSlice(AbstractDistribZkTestBase.DEFAULT_COLLECTION, shard);
    long[] numFound = new long[slice.getReplicasMap().size()];
    int c = 0;
    for (Replica replica : slice.getReplicas()) {
      String coreUrl = new ZkCoreNodeProps(replica).getCoreUrl();
      QueryResponse response;
      try (HttpSolrClient client = new HttpSolrClient(coreUrl)) {
        response = client.query(query);
      }
      numFound[c++] = response.getResults().getNumFound();
      log.info("Shard: " + shard + " Replica: {} has {} docs", coreUrl, String.valueOf(response.getResults().getNumFound()));
      assertTrue("Shard: " + shard + " Replica: " + coreUrl + " has 0 docs", response.getResults().getNumFound() > 0);
    }
    for (int i = 0; i < slice.getReplicasMap().size(); i++) {
      assertEquals(shard + " is not consistent", numFound[0], numFound[i]);
    }
  }

  protected void splitShard(String collection, String shardId, List<DocRouter.Range> subRanges, String splitKey) throws SolrServerException, IOException {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("action", CollectionParams.CollectionAction.SPLITSHARD.toString());
    params.set("collection", collection);
    if (shardId != null)  {
      params.set("shard", shardId);
    }
    if (subRanges != null)  {
      StringBuilder ranges = new StringBuilder();
      for (int i = 0; i < subRanges.size(); i++) {
        DocRouter.Range subRange = subRanges.get(i);
        ranges.append(subRange.toString());
        if (i < subRanges.size() - 1)
          ranges.append(",");
      }
      params.set("ranges", ranges.toString());
    }
    if (splitKey != null) {
      params.set("split.key", splitKey);
    }
    SolrRequest request = new QueryRequest(params);
    request.setPath("/admin/collections");

    String baseUrl = ((HttpSolrClient) shardToJetty.get(SHARD1).get(0).client.solrClient)
        .getBaseURL();
    baseUrl = baseUrl.substring(0, baseUrl.length() - "collection1".length());

    try (HttpSolrClient baseServer = new HttpSolrClient(baseUrl)) {
      baseServer.setConnectionTimeout(30000);
      baseServer.setSoTimeout(60000 * 5);
      baseServer.request(request);
    }
  }

  protected void indexAndUpdateCount(DocRouter router, List<DocRouter.Range> ranges, int[] docCounts, String id, int n) throws Exception {
    index("id", id, "n_ti", n);

    int idx = getHashRangeIdx(router, ranges, id);
    if (idx != -1)  {
      docCounts[idx]++;
    }
  }

  protected void deleteAndUpdateCount(DocRouter router, List<DocRouter.Range> ranges, int[] docCounts, String id) throws Exception {
    controlClient.deleteById(id);
    cloudClient.deleteById(id);

    int idx = getHashRangeIdx(router, ranges, id);
    if (idx != -1)  {
      docCounts[idx]--;
    }
  }

  public static int getHashRangeIdx(DocRouter router, List<DocRouter.Range> ranges, String id) {
    int hash = 0;
    if (router instanceof HashBasedRouter) {
      HashBasedRouter hashBasedRouter = (HashBasedRouter) router;
      hash = hashBasedRouter.sliceHash(id, null, null,null);
    }
    for (int i = 0; i < ranges.size(); i++) {
      DocRouter.Range range = ranges.get(i);
      if (range.includes(hash))
        return i;
    }
    return -1;
  }

  protected void logDebugHelp(int[] docCounts, QueryResponse response, long shard10Count, QueryResponse response2, long shard11Count) {
    for (int i = 0; i < docCounts.length; i++) {
      int docCount = docCounts[i];
      log.info("Expected docCount for shard1_{} = {}", i, docCount);
    }

    log.info("Actual docCount for shard1_0 = {}", shard10Count);
    log.info("Actual docCount for shard1_1 = {}", shard11Count);
    Map<String, String> idVsVersion = new HashMap<>();
    Map<String, SolrDocument> shard10Docs = new HashMap<>();
    Map<String, SolrDocument> shard11Docs = new HashMap<>();
    for (int i = 0; i < response.getResults().size(); i++) {
      SolrDocument document = response.getResults().get(i);
      idVsVersion.put(document.getFieldValue("id").toString(), document.getFieldValue("_version_").toString());
      SolrDocument old = shard10Docs.put(document.getFieldValue("id").toString(), document);
      if (old != null) {
        log.error("EXTRA: ID: " + document.getFieldValue("id") + " on shard1_0. Old version: " + old.getFieldValue("_version_") + " new version: " + document.getFieldValue("_version_"));
      }
    }
    for (int i = 0; i < response2.getResults().size(); i++) {
      SolrDocument document = response2.getResults().get(i);
      String value = document.getFieldValue("id").toString();
      String version = idVsVersion.get(value);
      if (version != null) {
        log.error("DUPLICATE: ID: " + value + " , shard1_0Version: " + version + " shard1_1Version:" + document.getFieldValue("_version_"));
      }
      SolrDocument old = shard11Docs.put(document.getFieldValue("id").toString(), document);
      if (old != null) {
        log.error("EXTRA: ID: " + document.getFieldValue("id") + " on shard1_1. Old version: " + old.getFieldValue("_version_") + " new version: " + document.getFieldValue("_version_"));
      }
    }
  }

  @Override
  protected SolrClient createNewSolrClient(String collection, String baseUrl) {
    HttpSolrClient client = (HttpSolrClient) super.createNewSolrClient(collection, baseUrl);
    client.setSoTimeout(5 * 60 * 1000);
    return client;
  }

  @Override
  protected SolrClient createNewSolrClient(int port) {
    HttpSolrClient client = (HttpSolrClient) super.createNewSolrClient(port);
    client.setSoTimeout(5 * 60 * 1000);
    return client;
  }

  @Override
  protected CloudSolrClient createCloudClient(String defaultCollection) {
    CloudSolrClient client = super.createCloudClient(defaultCollection);
    client.getLbClient().getHttpClient().getParams().setParameter(CoreConnectionPNames.SO_TIMEOUT, 5 * 60 * 1000);
    return client;
  }
}

