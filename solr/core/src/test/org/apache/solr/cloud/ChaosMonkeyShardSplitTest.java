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
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.cloud.PlainIdRouter;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.Hash;
import org.apache.solr.handler.admin.CollectionsHandler;
import org.apache.solr.handler.component.HttpShardHandlerFactory;
import org.apache.solr.update.DirectUpdateHandler2;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

/**
 * Test split phase that occurs when a Collection API split call is made.
 */
@Slow
public class ChaosMonkeyShardSplitTest extends AbstractFullDistribZkTestBase {
  
  static final int TIMEOUT = 10000;
  private AtomicInteger killCounter = new AtomicInteger();
  
  @BeforeClass
  public static void beforeSuperClass() throws Exception {}
  
  @AfterClass
  public static void afterSuperClass() {
    
  }
  
  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    useFactory(null);
    System.setProperty("numShards", Integer.toString(sliceCount));
  }
  
  @Override
  @After
  public void tearDown() throws Exception {
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
    
    resetExceptionIgnores();
    System.clearProperty("zkHost");
    System.clearProperty("numShards");
    System.clearProperty("solr.xml.persist");
    
    // insurance
    DirectUpdateHandler2.commitOnClose = true;
  }
  
  public ChaosMonkeyShardSplitTest() {
    super();
//    fixShardCount = true;
//    sliceCount = 1;
//    shardCount = TEST_NIGHTLY ? 7 : 4;
  }
  
  @Override
  public void doTest() throws Exception {
    handle.clear();
    handle.put("QTime", SKIPVAL);
    handle.put("timestamp", SKIPVAL);
    
    waitForThingsToLevelOut(15);
    printLayout();

    ClusterState clusterState = cloudClient.getZkStateReader().getClusterState();
    DocRouter router = clusterState.getCollection(AbstractDistribZkTestBase.DEFAULT_COLLECTION).getRouter();
    Slice shard1 = clusterState.getSlice(AbstractDistribZkTestBase.DEFAULT_COLLECTION, SHARD1);
    DocRouter.Range shard1Range = shard1.getRange() != null ? shard1.getRange() : router.fullRange();
    final List<DocRouter.Range> ranges = router.partitionRange(2, shard1Range);
    final int[] docCounts = new int[ranges.size()];
    int numReplicas = shard1.getReplicas().size();
    Thread indexThread = null;
    OverseerRestarter killer = null;
    Thread killerThread = null;
    final SolrServer solrServer = clients.get(0);
    
    try {
      solrServer.deleteByQuery("*:*");
      for (int i = 0; i < 100; i++) {
        indexr("id", i);
        
        // todo - hook in custom hashing
        byte[] bytes = String.valueOf(i).getBytes("UTF-8");
        int hash = Hash.murmurhash3_x86_32(bytes, 0, bytes.length, 0);
        for (int i2 = 0; i2 < ranges.size(); i2++) {
          DocRouter.Range range = ranges.get(i2);
          if (range.includes(hash)) docCounts[i2]++;
        }
      }
      solrServer.commit();
      
      waitForRecoveriesToFinish(false);
      
      indexThread = new Thread() {
        @Override
        public void run() {
          for (int i = 101; i < 201; i++) {
            try {
              indexr("id", i);
              
              // todo - hook in custom hashing
              byte[] bytes = String.valueOf(i).getBytes("UTF-8");
              int hash = Hash.murmurhash3_x86_32(bytes, 0, bytes.length, 0);
              for (int i2 = 0; i2 < ranges.size(); i2++) {
                DocRouter.Range range = ranges.get(i2);
                if (range.includes(hash)) docCounts[i2]++;
              }
              Thread.sleep(100);
            } catch (Exception e) {
              log.error("Exception while adding doc", e);
            }
          }
        }
      };
      indexThread.start();
      
      // kill the leader
      CloudJettyRunner leaderJetty = shardToLeaderJetty.get("shard1");
      log.info("Cluster State: "
          + cloudClient.getZkStateReader().getClusterState());
      
      chaosMonkey.killJetty(leaderJetty);
      
      Thread.sleep(2000);
      
      waitForThingsToLevelOut(90);
      
      Thread.sleep(1000);
      checkShardConsistency(false, true);
      
      CloudJettyRunner deadJetty = leaderJetty;
      
      // TODO: Check total docs ?
      // long cloudClientDocs = cloudClient.query(new
      // SolrQuery("*:*")).getResults().getNumFound();
      
      // Wait until new leader is elected
      while (deadJetty == leaderJetty) {
        updateMappingsFromZk(this.jettys, this.clients);
        leaderJetty = shardToLeaderJetty.get("shard1");
      }
      
      // bring back dead node
      ChaosMonkey.start(deadJetty.jetty); // he is not the leader anymore
      
      waitTillRecovered();
      
      // Kill the overseer
      // TODO: Actually kill the Overseer instance
      killer = new OverseerRestarter(zkServer.getZkAddress());
      killerThread = new Thread(killer);
      killerThread.start();
      killCounter.incrementAndGet();
      
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set("action",
          CollectionParams.CollectionAction.SPLITSHARD.toString());
      params.set("collection", "collection1");
      params.set("shard", "shard1");
      SolrRequest request = new QueryRequest(params);
      request.setPath("/admin/collections");
      
      String baseUrl = ((HttpSolrServer) shardToJetty.get("shard1").get(0).client.solrClient)
          .getBaseURL();
      baseUrl = baseUrl.substring(0, baseUrl.length() - "collection1".length());
      
      HttpSolrServer baseServer = new HttpSolrServer(baseUrl);
      baseServer.setConnectionTimeout(15000);
      baseServer.setSoTimeout((int) (CollectionsHandler.DEFAULT_ZK_TIMEOUT * 5));
      baseServer.request(request);
      
      System.out.println("Layout after split: \n");
      printLayout();
      
       // distributed commit on all shards
    } finally {
      if(indexThread != null)
        indexThread.join();
      if (solrServer != null)
        solrServer.commit();
      if (killer != null) {
        killer.run = false;
        if (killerThread != null) {
          killerThread.join();
        }
      }
    }
    
    SolrQuery query = new SolrQuery("*:*").setRows(0).setFields("id");
    query.set("distrib", false);
    
    String shard1_0_url = cloudClient.getZkStateReader().getLeaderUrl(
        AbstractFullDistribZkTestBase.DEFAULT_COLLECTION, "shard1_0",
        DEFAULT_CONNECTION_TIMEOUT);
    HttpSolrServer shard1_0Server = new HttpSolrServer(shard1_0_url);
    QueryResponse response = shard1_0Server.query(query);
    long shard10Count = response.getResults().getNumFound();
    System.out.println("Resp: shard: shard1_0 url: " + shard1_0_url + "\n"
        + response.getResponse());
    
    String shard1_1_url = cloudClient.getZkStateReader().getLeaderUrl(
        AbstractFullDistribZkTestBase.DEFAULT_COLLECTION, "shard1_1",
        DEFAULT_CONNECTION_TIMEOUT);
    HttpSolrServer shard1_1Server = new HttpSolrServer(shard1_1_url);
    response = shard1_1Server.query(query);
    long shard11Count = response.getResults().getNumFound();
    System.out.println("Resp: shard: shard1_1 url: " + shard1_1_url + "\n"
        + response.getResponse());
    
    for (int i = 0; i < docCounts.length; i++) {
      int docCount = docCounts[i];
      System.out
          .println("Expected docCount for shard1_" + i + " = " + docCount);
    }
    
    assertEquals("Wrong doc count on shard1_0", docCounts[0], shard10Count);
    assertEquals("Wrong doc count on shard1_1", docCounts[1], shard11Count);

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
    
    // todo - can't call waitForThingsToLevelOut because it looks for
    // jettys of all shards
    // and the new sub-shards don't have any.
    waitForRecoveriesToFinish(true);
    // waitForThingsToLevelOut(15);
    
    // todo - more and better tests
    
  }
  
  private class OverseerRestarter implements Runnable {
    SolrZkClient overseerClient = null;
    public volatile boolean run = true;
    private final String zkAddress;
    
    public OverseerRestarter(String zkAddress) {
      this.zkAddress = zkAddress;
    }
    
    @Override
    public void run() {
      try {
        overseerClient = electNewOverseer(zkAddress);
        while (run) {
          if (killCounter.get() > 0) {
            try {
              killCounter.decrementAndGet();
              log.info("Killing overseer after 800ms");
              Thread.sleep(800);
              overseerClient.close();
              overseerClient = electNewOverseer(zkAddress);
            } catch (Throwable e) {
              // e.printStackTrace();
            }
          }
          try {
            Thread.sleep(100);
          } catch (Throwable e) {
            // e.printStackTrace();
          }
        }
      } catch (Throwable t) {
        // ignore
      } finally {
        if (overseerClient != null) {
          try {
            overseerClient.close();
          } catch (Throwable t) {
            // ignore
          }
        }
      }
    }
  }
  
  private void waitTillRecovered() throws Exception {
    for (int i = 0; i < 30; i++) {
      Thread.sleep(3000);
      ZkStateReader zkStateReader = cloudClient.getZkStateReader();
      zkStateReader.updateClusterState(true);
      ClusterState clusterState = zkStateReader.getClusterState();
      DocCollection collection1 = clusterState.getCollection("collection1");
      Slice slice = collection1.getSlice("shard1");
      Collection<Replica> replicas = slice.getReplicas();
      boolean allActive = true;
      for (Replica replica : replicas) {
        if (!clusterState.liveNodesContain(replica.getNodeName())
            || !replica.get(ZkStateReader.STATE_PROP).equals(
                ZkStateReader.ACTIVE)) {
          allActive = false;
          break;
        }
      }
      if (allActive) {
        return;
      }
    }
    printLayout();
    fail("timeout waiting to see recovered node");
  }
  
  protected void indexDoc(List<CloudJettyRunner> skipServers, Object... fields)
      throws IOException, SolrServerException {
    SolrInputDocument doc = new SolrInputDocument();
    
    addFields(doc, fields);
    addFields(doc, "rnd_b", true);
    
    controlClient.add(doc);
    
    UpdateRequest ureq = new UpdateRequest();
    ureq.add(doc);
    ModifiableSolrParams params = new ModifiableSolrParams();
    for (CloudJettyRunner skip : skipServers) {
      params.add("test.distrib.skip.servers", skip.url + "/");
    }
    ureq.setParams(params);
    ureq.process(cloudClient);
  }
  
  // skip the randoms - they can deadlock...
  @Override
  protected void indexr(Object... fields) throws Exception {
    SolrInputDocument doc = new SolrInputDocument();
    addFields(doc, fields);
    addFields(doc, "rnd_b", true);
    indexDoc(doc);
  }
  
  /**
   * Elects a new overseer
   * 
   * @return SolrZkClient
   */
  private SolrZkClient electNewOverseer(String address) throws KeeperException,
      InterruptedException, IOException {
    SolrZkClient zkClient = new SolrZkClient(address, TIMEOUT);
    ZkStateReader reader = new ZkStateReader(zkClient);
    LeaderElector overseerElector = new LeaderElector(zkClient);
    
    // TODO: close Overseer
    Overseer overseer = new Overseer(
        new HttpShardHandlerFactory().getShardHandler(), "/admin/cores", reader);
    overseer.close();
    ElectionContext ec = new OverseerElectionContext(zkClient, overseer,
        address.replaceAll("/", "_"));
    overseerElector.setup(ec);
    overseerElector.joinElection(ec, false);
    return zkClient;
  }
  
}
