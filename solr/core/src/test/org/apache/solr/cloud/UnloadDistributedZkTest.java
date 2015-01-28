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

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CoreAdminRequest.Create;
import org.apache.solr.client.solrj.request.CoreAdminRequest.Unload;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.update.DirectUpdateHandler2;
import org.apache.solr.util.DefaultSolrThreadFactory;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * This test simply does a bunch of basic things in solrcloud mode and asserts things
 * work as expected.
 */
@Slow
@SuppressSSL(bugUrl = "https://issues.apache.org/jira/browse/SOLR-5776")
public class UnloadDistributedZkTest extends BasicDistributedZkTest {

  protected String getSolrXml() {
    return "solr-no-core.xml";
  }
  
  public UnloadDistributedZkTest() {
    super();
    checkCreatedVsState = false;
  }

  @Test
  public void test() throws Exception {
    
    testCoreUnloadAndLeaders(); // long
    testUnloadLotsOfCores(); // long
    
    testUnloadShardAndCollection();
    
    if (DEBUG) {
      super.printLayout();
    }
  }

  private void testUnloadShardAndCollection() throws Exception{
    // create one leader and one replica
    Create createCmd = new Create();
    createCmd.setCoreName("test_unload_shard_and_collection_1");
    String collection = "test_unload_shard_and_collection";
    createCmd.setCollection(collection);
    String coreDataDir = createTempDir().toFile().getAbsolutePath();
    createCmd.setDataDir(getDataDir(coreDataDir));
    createCmd.setNumShards(2);
    
    SolrClient client = clients.get(0);
    String url1 = getBaseUrl(client);

    try (HttpSolrClient adminClient = new HttpSolrClient(url1)) {
      adminClient.setConnectionTimeout(15000);
      adminClient.setSoTimeout(60000);
      adminClient.request(createCmd);

      createCmd = new Create();
      createCmd.setCoreName("test_unload_shard_and_collection_2");
      collection = "test_unload_shard_and_collection";
      createCmd.setCollection(collection);
      coreDataDir = createTempDir().toFile().getAbsolutePath();
      createCmd.setDataDir(getDataDir(coreDataDir));

      adminClient.request(createCmd);

      // does not mean they are active and up yet :*
      waitForRecoveriesToFinish(collection, false);

      // now unload one of the two
      Unload unloadCmd = new Unload(false);
      unloadCmd.setCoreName("test_unload_shard_and_collection_2");
      adminClient.request(unloadCmd);

      // there should be only one shard
      int slices = getCommonCloudSolrClient().getZkStateReader().getClusterState().getSlices(collection).size();
      long timeoutAt = System.currentTimeMillis() + 45000;
      while (slices != 1) {
        if (System.currentTimeMillis() > timeoutAt) {
          printLayout();
          fail("Expected to find only one slice in " + collection);
        }

        Thread.sleep(1000);
        slices = getCommonCloudSolrClient().getZkStateReader().getClusterState().getSlices(collection).size();
      }

      // now unload one of the other
      unloadCmd = new Unload(false);
      unloadCmd.setCoreName("test_unload_shard_and_collection_1");
      adminClient.request(unloadCmd);
    }

    //printLayout();
    // the collection should be gone
    long timeoutAt = System.currentTimeMillis() + 30000;
    while (getCommonCloudSolrClient().getZkStateReader().getClusterState().hasCollection(collection)) {
      if (System.currentTimeMillis() > timeoutAt) {
        printLayout();
        fail("Still found collection");
      }
      
      Thread.sleep(50);
    }
    
  }

  /**
   * @throws Exception on any problem
   */
  private void testCoreUnloadAndLeaders() throws Exception {
    File tmpDir = createTempDir().toFile();

    String core1DataDir = tmpDir.getAbsolutePath() + File.separator + System.currentTimeMillis() + "unloadcollection1" + "_1n";

    // create a new collection collection
    SolrClient client = clients.get(0);
    String url1 = getBaseUrl(client);
    try (HttpSolrClient adminClient = new HttpSolrClient(url1)) {
      adminClient.setConnectionTimeout(15000);
      adminClient.setSoTimeout(60000);

      Create createCmd = new Create();
      createCmd.setCoreName("unloadcollection1");
      createCmd.setCollection("unloadcollection");
      createCmd.setNumShards(1);
      createCmd.setDataDir(getDataDir(core1DataDir));
      adminClient.request(createCmd);
    }
    ZkStateReader zkStateReader = getCommonCloudSolrClient().getZkStateReader();
    
    zkStateReader.updateClusterState(true);

    int slices = zkStateReader.getClusterState().getCollection("unloadcollection").getSlices().size();
    assertEquals(1, slices);
    
    client = clients.get(1);
    String url2 = getBaseUrl(client);
    try (HttpSolrClient adminClient = new HttpSolrClient(url2)) {

      Create createCmd = new Create();
      createCmd.setCoreName("unloadcollection2");
      createCmd.setCollection("unloadcollection");
      String core2dataDir = tmpDir.getAbsolutePath() + File.separator + System.currentTimeMillis() + "unloadcollection1" + "_2n";
      createCmd.setDataDir(getDataDir(core2dataDir));
      adminClient.request(createCmd);
    }
    zkStateReader.updateClusterState(true);
    slices = zkStateReader.getClusterState().getCollection("unloadcollection").getSlices().size();
    assertEquals(1, slices);
    
    waitForRecoveriesToFinish("unloadcollection", zkStateReader, false);
    
    ZkCoreNodeProps leaderProps = getLeaderUrlFromZk("unloadcollection", "shard1");
    
    Random random = random();
    if (random.nextBoolean()) {
      try (HttpSolrClient collectionClient = new HttpSolrClient(leaderProps.getCoreUrl())) {
        // lets try and use the solrj client to index and retrieve a couple
        // documents
        SolrInputDocument doc1 = getDoc(id, 6, i1, -600, tlong, 600, t1,
            "humpty dumpy sat on a wall");
        SolrInputDocument doc2 = getDoc(id, 7, i1, -600, tlong, 600, t1,
            "humpty dumpy3 sat on a walls");
        SolrInputDocument doc3 = getDoc(id, 8, i1, -600, tlong, 600, t1,
            "humpty dumpy2 sat on a walled");
        collectionClient.add(doc1);
        collectionClient.add(doc2);
        collectionClient.add(doc3);
        collectionClient.commit();
      }
    }

    // create another replica for our collection
    client = clients.get(2);
    String url3 = getBaseUrl(client);
    try (HttpSolrClient adminClient = new HttpSolrClient(url3)) {
      Create createCmd = new Create();
      createCmd.setCoreName("unloadcollection3");
      createCmd.setCollection("unloadcollection");
      String core3dataDir = tmpDir.getAbsolutePath() + File.separator + System.currentTimeMillis() + "unloadcollection" + "_3n";
      createCmd.setDataDir(getDataDir(core3dataDir));
      adminClient.request(createCmd);
    }
    
    waitForRecoveriesToFinish("unloadcollection", zkStateReader, false);
    
    // so that we start with some versions when we reload...
    DirectUpdateHandler2.commitOnClose = false;
    
    try (HttpSolrClient addClient = new HttpSolrClient(url3 + "/unloadcollection3")) {
      addClient.setConnectionTimeout(30000);

      // add a few docs
      for (int x = 20; x < 100; x++) {
        SolrInputDocument doc1 = getDoc(id, x, i1, -600, tlong, 600, t1,
            "humpty dumpy sat on a wall");
        addClient.add(doc1);
      }
    }
    // don't commit so they remain in the tran log
    //collectionClient.commit();
    
    // unload the leader
    try (HttpSolrClient collectionClient = new HttpSolrClient(leaderProps.getBaseUrl())) {
      collectionClient.setConnectionTimeout(15000);
      collectionClient.setSoTimeout(30000);

      Unload unloadCmd = new Unload(false);
      unloadCmd.setCoreName(leaderProps.getCoreName());
      ModifiableSolrParams p = (ModifiableSolrParams) unloadCmd.getParams();

      collectionClient.request(unloadCmd);
    }
//    Thread.currentThread().sleep(500);
//    printLayout();
    
    int tries = 50;
    while (leaderProps.getCoreUrl().equals(zkStateReader.getLeaderUrl("unloadcollection", "shard1", 15000))) {
      Thread.sleep(100);
      if (tries-- == 0) {
        fail("Leader never changed");
      }
    }
    
    // ensure there is a leader
    zkStateReader.getLeaderRetry("unloadcollection", "shard1", 15000);
    
    try (HttpSolrClient addClient = new HttpSolrClient(url2 + "/unloadcollection2")) {
      addClient.setConnectionTimeout(30000);
      addClient.setSoTimeout(90000);

      // add a few docs while the leader is down
      for (int x = 101; x < 200; x++) {
        SolrInputDocument doc1 = getDoc(id, x, i1, -600, tlong, 600, t1,
            "humpty dumpy sat on a wall");
        addClient.add(doc1);
      }
    }
    
    // create another replica for our collection
    client = clients.get(3);
    String url4 = getBaseUrl(client);
    try (HttpSolrClient adminClient = new HttpSolrClient(url4)) {
      adminClient.setConnectionTimeout(15000);
      adminClient.setSoTimeout(30000);

      Create createCmd = new Create();
      createCmd.setCoreName("unloadcollection4");
      createCmd.setCollection("unloadcollection");
      String core4dataDir = tmpDir.getAbsolutePath() + File.separator + System.currentTimeMillis() + "unloadcollection" + "_4n";
      createCmd.setDataDir(getDataDir(core4dataDir));
      adminClient.request(createCmd);
    }
    waitForRecoveriesToFinish("unloadcollection", zkStateReader, false);
    
    // unload the leader again
    leaderProps = getLeaderUrlFromZk("unloadcollection", "shard1");
    try (HttpSolrClient collectionClient = new HttpSolrClient(leaderProps.getBaseUrl())) {
      collectionClient.setConnectionTimeout(15000);
      collectionClient.setSoTimeout(30000);

      Unload unloadCmd = new Unload(false);
      unloadCmd.setCoreName(leaderProps.getCoreName());
      SolrParams p = (ModifiableSolrParams) unloadCmd.getParams();
      collectionClient.request(unloadCmd);
    }
    tries = 50;
    while (leaderProps.getCoreUrl().equals(zkStateReader.getLeaderUrl("unloadcollection", "shard1", 15000))) {
      Thread.sleep(100);
      if (tries-- == 0) {
        fail("Leader never changed");
      }
    }
    
    zkStateReader.getLeaderRetry("unloadcollection", "shard1", 15000);
    
    
    // set this back
    DirectUpdateHandler2.commitOnClose = true;
    
    // bring the downed leader back as replica
    try (HttpSolrClient adminClient = new HttpSolrClient(leaderProps.getBaseUrl())) {
      adminClient.setConnectionTimeout(15000);
      adminClient.setSoTimeout(30000);

      Create createCmd = new Create();
      createCmd.setCoreName(leaderProps.getCoreName());
      createCmd.setCollection("unloadcollection");
      createCmd.setDataDir(getDataDir(core1DataDir));
      adminClient.request(createCmd);
    }
    waitForRecoveriesToFinish("unloadcollection", zkStateReader, false);

    long found1, found3;
    
    try (HttpSolrClient adminClient = new HttpSolrClient(url2 + "/unloadcollection")) {
      adminClient.setConnectionTimeout(15000);
      adminClient.setSoTimeout(30000);
      adminClient.commit();
      SolrQuery q = new SolrQuery("*:*");
      q.set("distrib", false);
      found1 = adminClient.query(q).getResults().getNumFound();
    }
    try (HttpSolrClient adminClient = new HttpSolrClient(url3 + "/unloadcollection")) {
      adminClient.setConnectionTimeout(15000);
      adminClient.setSoTimeout(30000);
      adminClient.commit();
      SolrQuery q = new SolrQuery("*:*");
      q.set("distrib", false);
      found3 = adminClient.query(q).getResults().getNumFound();
    }

    try (HttpSolrClient adminClient = new HttpSolrClient(url4 + "/unloadcollection")) {
      adminClient.setConnectionTimeout(15000);
      adminClient.setSoTimeout(30000);
      adminClient.commit();
      SolrQuery q = new SolrQuery("*:*");
      q.set("distrib", false);
      long found4 = adminClient.query(q).getResults().getNumFound();

      // all 3 shards should now have the same number of docs
      assertEquals(found1, found3);
      assertEquals(found3, found4);
    }
    
  }
  
  private void testUnloadLotsOfCores() throws Exception {
    SolrClient client = clients.get(2);
    String url3 = getBaseUrl(client);
    try (final HttpSolrClient adminClient = new HttpSolrClient(url3)) {
      adminClient.setConnectionTimeout(15000);
      adminClient.setSoTimeout(60000);
      ThreadPoolExecutor executor = new ThreadPoolExecutor(0, Integer.MAX_VALUE,
          5, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(),
          new DefaultSolrThreadFactory("testExecutor"));
      int cnt = atLeast(3);

      // create the cores
      createCores(adminClient, executor, "multiunload", 2, cnt);

      executor.shutdown();
      executor.awaitTermination(120, TimeUnit.SECONDS);
      executor = new ThreadPoolExecutor(0, Integer.MAX_VALUE, 5,
          TimeUnit.SECONDS, new SynchronousQueue<Runnable>(),
          new DefaultSolrThreadFactory("testExecutor"));
      for (int j = 0; j < cnt; j++) {
        final int freezeJ = j;
        executor.execute(new Runnable() {
          @Override
          public void run() {
            Unload unloadCmd = new Unload(true);
            unloadCmd.setCoreName("multiunload" + freezeJ);
            try {
              adminClient.request(unloadCmd);
            } catch (SolrServerException | IOException e) {
              throw new RuntimeException(e);
            }
          }
        });
        Thread.sleep(random().nextInt(50));
      }
      executor.shutdown();
      executor.awaitTermination(120, TimeUnit.SECONDS);
    }
  }

}
