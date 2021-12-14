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
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest.Unload;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.core.SolrCore;
import org.apache.solr.util.TestInjection;
import org.apache.solr.util.TimeOut;
import org.junit.Test;

/**
 * This test simply does a bunch of basic things in solrcloud mode and asserts things
 * work as expected.
 */
@Slow
@SuppressSSL(bugUrl = "https://issues.apache.org/jira/browse/SOLR-5776")
@LuceneTestCase.BadApple(bugUrl = "https://issues.apache.org/jira/browse/SOLR-15848")
public class UnloadDistributedZkTest extends BasicDistributedZkTest {
  public UnloadDistributedZkTest() {
    super();
  }

  protected String getSolrXml() {
    return "solr.xml";
  }

  @Test
  public void test() throws Exception {
    jettys.forEach(j -> j.getCoreContainer().getAllowPaths().add(Paths.get("_ALL_"))); // Allow non-standard core instance path
    testCoreUnloadAndLeaders(); // long
    testUnloadLotsOfCores(); // long

    testUnloadShardAndCollection();
  }

  private void checkCoreNamePresenceAndSliceCount(String collectionName, String coreName,
      boolean shouldBePresent, int expectedSliceCount) throws Exception {
    final TimeOut timeout = new TimeOut(45, TimeUnit.SECONDS, TimeSource.NANO_TIME);
    Boolean isPresent = null; // null meaning "don't know"
    while (null == isPresent || shouldBePresent != isPresent) {
      final DocCollection docCollection = getCommonCloudSolrClient().getZkStateReader().getClusterState().getCollectionOrNull(collectionName);
      final Collection<Slice> slices = (docCollection != null) ? docCollection.getSlices() : Collections.emptyList();
      if (timeout.hasTimedOut()) {
        printLayout();
        fail("checkCoreNamePresenceAndSliceCount failed:"
            +" collection="+collectionName+" CoreName="+coreName
            +" shouldBePresent="+shouldBePresent+" isPresent="+isPresent
            +" expectedSliceCount="+expectedSliceCount+" actualSliceCount="+slices.size());
      }
      if (expectedSliceCount == slices.size()) {
        isPresent = false;
        for (Slice slice : slices) {
          for (Replica replica : slice.getReplicas()) {
            if (coreName.equals(replica.get("core"))) {
              isPresent = true;
            }
          }
        }
      }
      Thread.sleep(1000);
    }
  }

  private void testUnloadShardAndCollection() throws Exception{
    final int numShards = 2;

    final String collection = "test_unload_shard_and_collection";

    final String coreName1 = collection+"_1";
    final String coreName2 = collection+"_2";

    assertEquals(0, CollectionAdminRequest.createCollection(collection, "conf1", numShards, 1)
        .setCreateNodeSet("")
        .process(cloudClient).getStatus());
    assertTrue(CollectionAdminRequest.addReplicaToShard(collection, "shard1")
        .setCoreName(coreName1)
        .setNode(jettys.get(0).getNodeName())
        .process(cloudClient).isSuccess());

    assertTrue(CollectionAdminRequest.addReplicaToShard(collection, "shard2")
        .setCoreName(coreName2)
        .setNode(jettys.get(0).getNodeName())
        .process(cloudClient).isSuccess());


    // does not mean they are active and up yet :*
    waitForRecoveriesToFinish(collection, false);

    final boolean unloadInOrder = random().nextBoolean();
    final String unloadCmdCoreName1 = (unloadInOrder ? coreName1 : coreName2);
    final String unloadCmdCoreName2 = (unloadInOrder ? coreName2 : coreName1);

    try (HttpSolrClient adminClient = getHttpSolrClient(buildUrl(jettys.get(0).getLocalPort()))) {
      // now unload one of the two
      Unload unloadCmd = new Unload(false);
      unloadCmd.setCoreName(unloadCmdCoreName1);
      adminClient.request(unloadCmd);

      // there should still be two shards (as of SOLR-5209)
      checkCoreNamePresenceAndSliceCount(collection, unloadCmdCoreName1, false /* shouldBePresent */, numShards /* expectedSliceCount */);

      // now unload one of the other
      unloadCmd = new Unload(false);
      unloadCmd.setCoreName(unloadCmdCoreName2);
      adminClient.request(unloadCmd);
      checkCoreNamePresenceAndSliceCount(collection, unloadCmdCoreName2, false /* shouldBePresent */, numShards /* expectedSliceCount */);
    }

    //printLayout();
    // the collection should still be present (as of SOLR-5209 replica removal does not cascade to remove the slice and collection)
    assertTrue("No longer found collection "+collection, getCommonCloudSolrClient().getZkStateReader().getClusterState().hasCollection(collection));
  }

  protected SolrCore getFirstCore(String collection, JettySolrRunner jetty) {
    SolrCore solrCore = null;
    for (SolrCore core : jetty.getCoreContainer().getCores()) {
      if (core.getName().startsWith(collection)) {
        solrCore = core;
      }
    }
    return solrCore;
  }

  /**
   * @throws Exception on any problem
   */
  private void testCoreUnloadAndLeaders() throws Exception {
    JettySolrRunner jetty1 = jettys.get(0);

    assertEquals(0, CollectionAdminRequest
        .createCollection("unloadcollection", "conf1", 1,1)
        .setCreateNodeSet(jetty1.getNodeName())
        .process(cloudClient).getStatus());
    ZkStateReader zkStateReader = getCommonCloudSolrClient().getZkStateReader();
    
    zkStateReader.forceUpdateCollection("unloadcollection");

    int slices = zkStateReader.getClusterState().getCollection("unloadcollection").getSlices().size();
    assertEquals(1, slices);
    SolrCore solrCore = getFirstCore("unloadcollection", jetty1);
    String core1DataDir = solrCore.getDataDir();

    assertTrue(CollectionAdminRequest
        .addReplicaToShard("unloadcollection", "shard1")
        .setCoreName("unloadcollection_shard1_replica2")
        .setNode(jettys.get(1).getNodeName())
        .process(cloudClient).isSuccess());
    zkStateReader.forceUpdateCollection("unloadcollection");
    slices = zkStateReader.getClusterState().getCollection("unloadcollection").getSlices().size();
    assertEquals(1, slices);
    
    waitForRecoveriesToFinish("unloadcollection", zkStateReader, false);
    
    ZkCoreNodeProps leaderProps = getLeaderUrlFromZk("unloadcollection", "shard1");
    
    Random random = random();
    if (random.nextBoolean()) {
      try (HttpSolrClient collectionClient = getHttpSolrClient(leaderProps.getCoreUrl())) {
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

    assertTrue(CollectionAdminRequest
        .addReplicaToShard("unloadcollection", "shard1")
        .setCoreName("unloadcollection_shard1_replica3")
        .setNode(jettys.get(2).getNodeName())
        .process(cloudClient).isSuccess());

    waitForRecoveriesToFinish("unloadcollection", zkStateReader, false);

    // so that we start with some versions when we reload...
    TestInjection.skipIndexWriterCommitOnClose = true;

    try (HttpSolrClient addClient = getHttpSolrClient(jettys.get(2).getBaseUrl() + "/unloadcollection_shard1_replica3", 30000)) {

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
    try (HttpSolrClient collectionClient = getHttpSolrClient(leaderProps.getBaseUrl(), 15000, 30000)) {

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

    try (HttpSolrClient addClient = getHttpSolrClient(jettys.get(1).getBaseUrl() + "/unloadcollection_shard1_replica2", 30000, 90000)) {

      // add a few docs while the leader is down
      for (int x = 101; x < 200; x++) {
        SolrInputDocument doc1 = getDoc(id, x, i1, -600, tlong, 600, t1,
            "humpty dumpy sat on a wall");
        addClient.add(doc1);
      }
    }

    assertTrue(CollectionAdminRequest
        .addReplicaToShard("unloadcollection", "shard1")
        .setCoreName("unloadcollection_shard1_replica4")
        .setNode(jettys.get(3).getNodeName())
        .process(cloudClient).isSuccess());

    waitForRecoveriesToFinish("unloadcollection", zkStateReader, false);

    // unload the leader again
    leaderProps = getLeaderUrlFromZk("unloadcollection", "shard1");
    try (HttpSolrClient collectionClient = getHttpSolrClient(leaderProps.getBaseUrl(), 15000, 30000)) {

      Unload unloadCmd = new Unload(false);
      unloadCmd.setCoreName(leaderProps.getCoreName());
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

    TestInjection.skipIndexWriterCommitOnClose = false; // set this back
    assertTrue(CollectionAdminRequest
        .addReplicaToShard("unloadcollection", "shard1")
        .setCoreName(leaderProps.getCoreName())
        .setDataDir(core1DataDir)
        .setNode(leaderProps.getNodeName())
        .process(cloudClient).isSuccess());

    waitForRecoveriesToFinish("unloadcollection", zkStateReader, false);

    long found1, found3;

    try (HttpSolrClient adminClient = getHttpSolrClient(jettys.get(1).getBaseUrl() + "/unloadcollection_shard1_replica2", 15000, 30000)) {
      adminClient.commit();
      SolrQuery q = new SolrQuery("*:*");
      q.set("distrib", false);
      found1 = adminClient.query(q).getResults().getNumFound();
    }

    try (HttpSolrClient adminClient = getHttpSolrClient(jettys.get(2).getBaseUrl() + "/unloadcollection_shard1_replica3", 15000, 30000)) {
      adminClient.commit();
      SolrQuery q = new SolrQuery("*:*");
      q.set("distrib", false);
      found3 = adminClient.query(q).getResults().getNumFound();
    }

    try (HttpSolrClient adminClient = getHttpSolrClient(jettys.get(3).getBaseUrl() + "/unloadcollection_shard1_replica4", 15000, 30000)) {
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
    JettySolrRunner jetty = jettys.get(0);
    try (final HttpSolrClient adminClient = (HttpSolrClient) jetty.newClient(15000, 60000)) {
      int numReplicas = atLeast(3);
      ThreadPoolExecutor executor = new ExecutorUtil.MDCAwareThreadPoolExecutor(0, Integer.MAX_VALUE,
          5, TimeUnit.SECONDS, new SynchronousQueue<>(),
          new SolrNamedThreadFactory("testExecutor"));
      try {
        // create the cores
        createCollectionInOneInstance(adminClient, jetty.getNodeName(), executor, "multiunload", 2, numReplicas);
      } finally {
        ExecutorUtil.shutdownAndAwaitTermination(executor);
      }

      executor = new ExecutorUtil.MDCAwareThreadPoolExecutor(0, Integer.MAX_VALUE, 5,
          TimeUnit.SECONDS, new SynchronousQueue<>(),
          new SolrNamedThreadFactory("testExecutor"));
      try {
        for (int j = 0; j < numReplicas; j++) {
          final int freezeJ = j;
          executor.execute(() -> {
            Unload unloadCmd = new Unload(true);
            unloadCmd.setCoreName("multiunload" + freezeJ);
            try {
              adminClient.request(unloadCmd);
            } catch (SolrServerException | IOException e) {
              throw new RuntimeException(e);
            }
          });
          Thread.sleep(random().nextInt(50));
        }
      } finally {
        ExecutorUtil.shutdownAndAwaitTermination(executor);
      }
    }
  }
}
