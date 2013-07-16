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

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.http.params.CoreConnectionPNames;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.cloud.HashBasedRouter;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.handler.admin.CollectionsHandler;
import org.apache.solr.update.DirectUpdateHandler2;
import org.junit.After;
import org.junit.Before;

public class ShardSplitTest extends BasicDistributedZkTest {

  public static final String SHARD1_0 = SHARD1 + "_0";
  public static final String SHARD1_1 = SHARD1 + "_1";

  public ShardSplitTest() {
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

    ClusterState clusterState = cloudClient.getZkStateReader().getClusterState();
    final DocRouter router = clusterState.getCollection(AbstractDistribZkTestBase.DEFAULT_COLLECTION).getRouter();
    Slice shard1 = clusterState.getSlice(AbstractDistribZkTestBase.DEFAULT_COLLECTION, SHARD1);
    DocRouter.Range shard1Range = shard1.getRange() != null ? shard1.getRange() : router.fullRange();
    final List<DocRouter.Range> ranges = router.partitionRange(2, shard1Range);
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
        Set<String> deleted = new HashSet<String>();
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
            log.error("Exception while adding docs", e);
          }
        }
      }
    };
    indexThread.start();

    try {
      for (int i = 0; i < 3; i++) {
        try {
          splitShard(SHARD1);
          log.info("Layout after split: \n");
          printLayout();
          break;
        } catch (HttpSolrServer.RemoteSolrException e) {
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

    checkDocCountsAndShardStates(docCounts, numReplicas);

    // todo can't call waitForThingsToLevelOut because it looks for jettys of all shards
    // and the new sub-shards don't have any.
    waitForRecoveriesToFinish(true);
    //waitForThingsToLevelOut(15);
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
    HttpSolrServer shard1_0Server = new HttpSolrServer(shard1_0.getCoreUrl());
    QueryResponse response = shard1_0Server.query(query);
    long shard10Count = response.getResults().getNumFound();

    ZkCoreNodeProps shard1_1 = getLeaderUrlFromZk(AbstractDistribZkTestBase.DEFAULT_COLLECTION, SHARD1_1);
    HttpSolrServer shard1_1Server = new HttpSolrServer(shard1_1.getCoreUrl());
    QueryResponse response2 = shard1_1Server.query(query);
    long shard11Count = response2.getResults().getNumFound();

    logDebugHelp(docCounts, response, shard10Count, response2, shard11Count);

    assertEquals("Wrong doc count on shard1_0", docCounts[0], shard10Count);
    assertEquals("Wrong doc count on shard1_1", docCounts[1], shard11Count);
  }

  protected void checkSubShardConsistency(String shard) throws SolrServerException {
    SolrQuery query = new SolrQuery("*:*").setRows(1000).setFields("id", "_version_");
    query.set("distrib", false);

    ClusterState clusterState = cloudClient.getZkStateReader().getClusterState();
    Slice slice = clusterState.getSlice(AbstractDistribZkTestBase.DEFAULT_COLLECTION, shard);
    long[] numFound = new long[slice.getReplicasMap().size()];
    int c = 0;
    for (Replica replica : slice.getReplicas()) {
      String coreUrl = new ZkCoreNodeProps(replica).getCoreUrl();
      HttpSolrServer server = new HttpSolrServer(coreUrl);
      QueryResponse response = server.query(query);
      numFound[c++] = response.getResults().getNumFound();
      log.info("Shard: " + shard + " Replica: {} has {} docs", coreUrl, String.valueOf(response.getResults().getNumFound()));
      assertTrue("Shard: " + shard + " Replica: " + coreUrl + " has 0 docs", response.getResults().getNumFound() > 0);
    }
    for (int i = 0; i < slice.getReplicasMap().size(); i++) {
      assertEquals(shard + " is not consistent", numFound[0], numFound[i]);
    }
  }

  protected void splitShard(String shardId) throws SolrServerException, IOException {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("action", CollectionParams.CollectionAction.SPLITSHARD.toString());
    params.set("collection", "collection1");
    params.set("shard", shardId);
    SolrRequest request = new QueryRequest(params);
    request.setPath("/admin/collections");

    String baseUrl = ((HttpSolrServer) shardToJetty.get(SHARD1).get(0).client.solrClient)
        .getBaseURL();
    baseUrl = baseUrl.substring(0, baseUrl.length() - "collection1".length());

    HttpSolrServer baseServer = new HttpSolrServer(baseUrl);
    baseServer.setConnectionTimeout(15000);
    baseServer.setSoTimeout((int) (CollectionsHandler.DEFAULT_ZK_TIMEOUT * 5));
    baseServer.request(request);
  }

  protected void indexAndUpdateCount(DocRouter router, List<DocRouter.Range> ranges, int[] docCounts, String id, int n) throws Exception {
    index("id", id, "n_ti", n);

    int idx = getHashRangeIdx(router, ranges, docCounts, id);
    if (idx != -1)  {
      docCounts[idx]++;
    }
  }

  protected void deleteAndUpdateCount(DocRouter router, List<DocRouter.Range> ranges, int[] docCounts, String id) throws Exception {
    controlClient.deleteById(id);
    cloudClient.deleteById(id);

    int idx = getHashRangeIdx(router, ranges, docCounts, id);
    if (idx != -1)  {
      docCounts[idx]--;
    }
  }

  private int getHashRangeIdx(DocRouter router, List<DocRouter.Range> ranges, int[] docCounts, String id) {
    int hash = 0;
    if (router instanceof HashBasedRouter) {
      HashBasedRouter hashBasedRouter = (HashBasedRouter) router;
      hash = hashBasedRouter.sliceHash(id, null, null);
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
    Map<String, String> idVsVersion = new HashMap<String, String>();
    Map<String, SolrDocument> shard10Docs = new HashMap<String, SolrDocument>();
    Map<String, SolrDocument> shard11Docs = new HashMap<String, SolrDocument>();
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
  protected SolrServer createNewSolrServer(String collection, String baseUrl) {
    HttpSolrServer server = (HttpSolrServer) super.createNewSolrServer(collection, baseUrl);
    server.setSoTimeout(5 * 60 * 1000);
    return server;
  }

  @Override
  protected SolrServer createNewSolrServer(int port) {
    HttpSolrServer server = (HttpSolrServer) super.createNewSolrServer(port);
    server.setSoTimeout(5 * 60 * 1000);
    return server;
  }

  @Override
  protected CloudSolrServer createCloudClient(String defaultCollection) throws MalformedURLException {
    CloudSolrServer client = super.createCloudClient(defaultCollection);
    client.getLbServer().getHttpClient().getParams().setParameter(CoreConnectionPNames.SO_TIMEOUT, 5 * 60 * 1000);
    return client;
  }
}

