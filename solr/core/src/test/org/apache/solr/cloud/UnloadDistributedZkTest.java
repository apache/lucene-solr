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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.apache.solr.SolrTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
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
import org.apache.solr.common.util.TimeOut;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.core.SolrCore;
import org.apache.solr.util.TestInjection;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

/**
 * This test simply does a bunch of basic things in solrcloud mode and asserts things
 * work as expected.
 */
@SolrTestCase.SuppressSSL(bugUrl = "https://issues.apache.org/jira/browse/SOLR-5776")
public class UnloadDistributedZkTest extends SolrCloudBridgeTestCase {


  public UnloadDistributedZkTest() throws Exception {
    numJettys = 4;
    sliceCount = 2;
  }

  @BeforeClass
  public static void beforeUnloadDistributedZkTest() throws Exception {
    System.setProperty("managed.schema.mutable", "true");
    System.setProperty("solr.skipCommitOnClose", "false");
    useFactory(null);
  }

  protected String getSolrXml() {
    return "solr.xml";
  }

  public void checkCoreNamePresenceAndSliceCount(String collectionName, String coreName,
                                                 boolean shouldBePresent, int expectedSliceCount) throws Exception {
    final TimeOut timeout = new TimeOut(45, TimeUnit.SECONDS, TimeSource.NANO_TIME);
    Boolean isPresent = null; // null meaning "don't know"
    while (null == isPresent || shouldBePresent != isPresent) {
      final DocCollection docCollection = cloudClient.getZkStateReader().getClusterState().getCollectionOrNull(collectionName);
      final Collection<Slice> slices = (docCollection != null) ? docCollection.getSlices() : Collections.emptyList();
      if (timeout.hasTimedOut()) {
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

  @Test
  @Nightly
  public void testUnloadShardAndCollection() throws Exception{
    final int numShards = 2;

    final String collection = "test_unload_shard_and_collection";

    final String coreName1 = collection+"_1";
    final String coreName2 = collection+"_2";

    assertEquals(0, CollectionAdminRequest.createCollection(collection, "_default", numShards, 1)
            .setCreateNodeSet("")
            .process(cloudClient).getStatus());
    CollectionAdminRequest.addReplicaToShard(collection, "shard1")
            .setCoreName(coreName1)
            .setNode(cluster.getJettySolrRunner(0).getNodeName())
            .process(cloudClient);

    CollectionAdminRequest.addReplicaToShard(collection, "shard2")
            .setCoreName(coreName2)
            .setNode(cluster.getJettySolrRunner(0).getNodeName())
            .process(cloudClient);

    final boolean unloadInOrder = random().nextBoolean();
    final String unloadCmdCoreName1 = (unloadInOrder ? coreName1 : coreName2);
    final String unloadCmdCoreName2 = (unloadInOrder ? coreName2 : coreName1);

    try (Http2SolrClient adminClient = SolrTestCaseJ4.getHttpSolrClient(cluster.getJettySolrRunner(0).getBaseUrl().toString())) {
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
    assertTrue("No longer found collection "+collection, cloudClient.getZkStateReader().getClusterState().hasCollection(collection));
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
  @Test
  @Ignore // unload is not correct here, we should delete the replica
  public void testCoreUnloadAndLeaders() throws Exception {
    JettySolrRunner jetty1 = cluster.getJettySolrRunner(0);

    assertEquals(0, CollectionAdminRequest
            .createCollection("unloadcollection", "_default", 1,1)
            .setCreateNodeSet(jetty1.getNodeName())
            .process(cloudClient).getStatus());
    ZkStateReader zkStateReader = cloudClient.getZkStateReader();

    int slices = zkStateReader.getClusterState().getCollection("unloadcollection").getSlices().size();
    assertEquals(1, slices);
    SolrCore solrCore = getFirstCore("unloadcollection", jetty1);
    String core1DataDir = solrCore.getDataDir();

    assertTrue(CollectionAdminRequest
            .addReplicaToShard("unloadcollection", "shard1")
            .setCoreName("unloadcollection_shard1_replica2")
            .setNode(cluster.getJettySolrRunner(1).getNodeName())
            .process(cloudClient).isSuccess());
    slices = zkStateReader.getClusterState().getCollection("unloadcollection").getSlices().size();
    assertEquals(1, slices);

    waitForRecoveriesToFinish("unloadcollection");

    Replica leaderProps = getLeaderUrlFromZk("unloadcollection", "shard1");

    Random random = random();
    if (random.nextBoolean()) {
      try (Http2SolrClient collectionClient = SolrTestCaseJ4.getHttpSolrClient(leaderProps.getCoreUrl())) {
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
            .setNode(cluster.getJettySolrRunner(2).getNodeName())
            .process(cloudClient).isSuccess());

    cluster.waitForActiveCollection("unloadcollection", 1, 3);

    // so that we start with some versions when we reload...
    TestInjection.skipIndexWriterCommitOnClose = true;

    try (Http2SolrClient addClient = SolrTestCaseJ4.getHttpSolrClient(cluster.getJettySolrRunner(2).getBaseUrl() + "/unloadcollection_shard1_replica3", 30000)) {

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
    leaderProps = getLeaderUrlFromZk("unloadcollection", "shard1");
    try (Http2SolrClient collectionClient = SolrTestCaseJ4.getHttpSolrClient(leaderProps.getBaseUrl(), 15000, 30000)) {

      Unload unloadCmd = new Unload(false);
      unloadCmd.setCoreName(leaderProps.getName());
      ModifiableSolrParams p = (ModifiableSolrParams) unloadCmd.getParams();

      collectionClient.request(unloadCmd);
    }
//    Thread.currentThread().sleep(500);
//    printLayout();

    cluster.waitForActiveCollection("unloadcollection", 1, 2);

    try (Http2SolrClient addClient = SolrTestCaseJ4.getHttpSolrClient(cluster.getJettySolrRunner(1).getBaseUrl() + "/unloadcollection_shard1_replica2", 30000, 90000)) {

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
            .setNode(cluster.getJettySolrRunner(3).getNodeName())
            .process(cloudClient).isSuccess());

    cluster.waitForActiveCollection("unloadcollection", 1, 3);

    // unload the leader again
    leaderProps = getLeaderUrlFromZk("unloadcollection", "shard1");
    try (Http2SolrClient collectionClient = SolrTestCaseJ4.getHttpSolrClient(leaderProps.getBaseUrl(), 15000, 30000)) {

      Unload unloadCmd = new Unload(false);
      unloadCmd.setCoreName(leaderProps.getName());
      collectionClient.request(unloadCmd);
    }

    cluster.waitForActiveCollection("unloadcollection", 1, 2);

    // set this back
    TestInjection.skipIndexWriterCommitOnClose = false; // set this back
    assertTrue(CollectionAdminRequest
            .addReplicaToShard("unloadcollection", "shard1")
            .setCoreName(leaderProps.getName())
            .setDataDir(core1DataDir)
            .setNode(leaderProps.getNodeName())
            .process(cloudClient).isSuccess());

    cluster.waitForActiveCollection("unloadcollection", 1, 3);

    long found1, found3;

    try (Http2SolrClient adminClient = SolrTestCaseJ4.getHttpSolrClient(cluster.getJettySolrRunner(1).getBaseUrl() + "/unloadcollection_shard1_replica2", 15000, 30000)) {
      adminClient.commit();
      SolrQuery q = new SolrQuery("*:*");
      q.set("distrib", false);
      found1 = adminClient.query(q).getResults().getNumFound();
    }

    try (Http2SolrClient adminClient = SolrTestCaseJ4.getHttpSolrClient(cluster.getJettySolrRunner(2).getBaseUrl() + "/unloadcollection_shard1_replica3", 15000, 30000)) {
      adminClient.commit();
      SolrQuery q = new SolrQuery("*:*");
      q.set("distrib", false);
      found3 = adminClient.query(q).getResults().getNumFound();
    }

    try (Http2SolrClient adminClient = SolrTestCaseJ4.getHttpSolrClient(cluster.getJettySolrRunner(3).getBaseUrl() + "/unloadcollection_shard1_replica4", 15000, 30000)) {
      adminClient.commit();
      SolrQuery q = new SolrQuery("*:*");
      q.set("distrib", false);
      long found4 = adminClient.query(q).getResults().getNumFound();

      // all 3 shards should now have the same number of docs
      assertEquals(found1, found3);
      assertEquals(found3, found4);
    }
  }

  @Test
  @Ignore // nocommit - needs to be hardened
  public void testUnloadLotsOfCores() throws Exception {
    JettySolrRunner jetty = cluster.getJettySolrRunner(0);
    try (final Http2SolrClient adminClient = (Http2SolrClient) jetty.newClient(15000, 60000)) {
      int numReplicas = atLeast(3);


      // create the cores
      createCollectionInOneInstance(adminClient, jetty.getNodeName(), testExecutor, "multiunload", 2, numReplicas);
      List<Callable<Object>> calls = new ArrayList<>();

      for (int j = 0; j < numReplicas; j++) {
        final int freezeJ = j;
        calls.add(() -> {
          Unload unloadCmd = new Unload(true);
          unloadCmd.setCoreName("multiunload" + freezeJ);
          try {
            adminClient.request(unloadCmd);
          } catch (SolrServerException | IOException e) {
            throw new RuntimeException(e);
          }
          return null;
        });
        Thread.sleep(random().nextInt(50));
      }
      testExecutor.invokeAll(calls);
    }
  }
}
