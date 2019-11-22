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
package org.apache.solr.store.shared;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Replica.State;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.store.blob.client.BlobCoreMetadata;
import org.apache.solr.store.blob.client.CoreStorageClient;
import org.apache.solr.store.blob.process.BlobProcessUtil;
import org.apache.solr.store.blob.process.CorePullTask;
import org.apache.solr.store.blob.process.CorePullTask.PullCoreCallback;
import org.apache.solr.store.blob.process.CorePullerFeeder;
import org.apache.solr.store.blob.process.CoreSyncStatus;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests for missing shared core. Missing core refers to a case in which shard index exists on the source-of-truth for
 * shared collections, the shared store, but is missing locally on the solr node. The metadata for the index shard and 
 * replica exist on ZK which indicates Solr Cloud is aware of the shard and replica. 
 */
public class SharedStoreMissingCoreTest extends SolrCloudSharedStoreTestCase {

  private static Path sharedStoreRootPath;

  @BeforeClass
  public static void setupClass() throws Exception {
    sharedStoreRootPath = createTempDir("tempDir");
  }

  @After
  public void teardownTest() throws Exception {
    shutdownCluster();
    // clean up the shared store after each test. The temp dir should clean up itself after the
    // test class finishes
    FileUtils.cleanDirectory(sharedStoreRootPath.toFile());
  }

  /**
   * Tests that after a missing core is discovered and created, a query is able to successfully pull into it from shared store.
   * Also makes sure an indexing on top of pulled contents is also successful.
   */
  @Test
  public void testMissingCorePullAndIndexingSucceeds() throws Exception {
    setupCluster(1);
    CloudSolrClient cloudClient = cluster.getSolrClient();

    // setup the test harness
    CoreStorageClient storageClient = setupLocalBlobStoreClient(sharedStoreRootPath, DEFAULT_BLOB_DIR_NAME);
    setupTestSharedClientForNode(getBlobStorageProviderTestInstance(storageClient), cluster.getJettySolrRunner(0));

    String collectionName = "sharedCollection";
    int maxShardsPerNode = 1;
    int numReplicas = 1;
    // specify a comma-delimited string of shard names for multiple shards when using
    // an implicit router
    String shardNames = "shard1";
    setupSharedCollectionWithShardNames(collectionName, maxShardsPerNode, numReplicas, shardNames);

    // send an update to the collection
    UpdateRequest updateReq = new UpdateRequest();
    updateReq.add("id", "1");
    updateReq.commit(cloudClient, collectionName);

    Map<String, CountDownLatch> asyncPullLatches = stopSolrRemoveCoreRestartSolr(cloudClient, storageClient, collectionName);
    queryAndWaitForPullToFinish(cloudClient, collectionName, asyncPullLatches);
    // verify the documents are present
    assertQueryReturnsAllDocs(cloudClient, collectionName, Lists.newArrayList("1"));

    // send another update to the collection
    updateReq = new UpdateRequest();
    updateReq.add("id", "2");
    updateReq.commit(cloudClient, collectionName);

    // verify the documents are present
    assertQueryReturnsAllDocs(cloudClient, collectionName, Lists.newArrayList("1", "2"));

    // verify that they made it to shared store by a clean pull
    asyncPullLatches = stopSolrRemoveCoreRestartSolr(cloudClient, storageClient, collectionName);
    queryAndWaitForPullToFinish(cloudClient, collectionName, asyncPullLatches);
    assertQueryReturnsAllDocs(cloudClient, collectionName, Lists.newArrayList("1", "2"));
  }


  /**
   * Tests that after a missing core is discovered and created, an indexing is able to successfully pull into
   * it from shared store and is itself successful.
   */
  @Test
  public void testMissingCoreIndexingSucceeds() throws Exception {
    setupCluster(1);
    CloudSolrClient cloudClient = cluster.getSolrClient();

    // setup the test harness
    CoreStorageClient storageClient = setupLocalBlobStoreClient(sharedStoreRootPath, DEFAULT_BLOB_DIR_NAME);
    setupTestSharedClientForNode(getBlobStorageProviderTestInstance(storageClient), cluster.getJettySolrRunner(0));

    String collectionName = "sharedCollection";
    int maxShardsPerNode = 1;
    int numReplicas = 1;
    // specify a comma-delimited string of shard names for multiple shards when using
    // an implicit router
    String shardNames = "shard1";
    setupSharedCollectionWithShardNames(collectionName, maxShardsPerNode, numReplicas, shardNames);

    // send an update to the collection
    UpdateRequest updateReq = new UpdateRequest();
    updateReq.add("id", "1");
    updateReq.commit(cloudClient, collectionName);

    stopSolrRemoveCoreRestartSolr(cloudClient, storageClient, collectionName);

    // send another update to the collection
    updateReq = new UpdateRequest();
    updateReq.add("id", "2");
    updateReq.commit(cloudClient, collectionName);

    // verify that previous and new document both are present
    assertQueryReturnsAllDocs(cloudClient, collectionName, Lists.newArrayList("1", "2"));

    // verify that new state made it to shared store by doing a clean pull
    Map<String, CountDownLatch> asyncPullLatches = stopSolrRemoveCoreRestartSolr(cloudClient, storageClient, collectionName);
    queryAndWaitForPullToFinish(cloudClient, collectionName, asyncPullLatches);
    assertQueryReturnsAllDocs(cloudClient, collectionName, Lists.newArrayList("1", "2"));
  }

  private void queryAndWaitForPullToFinish(CloudSolrClient cloudClient, String collectionName, Map<String, CountDownLatch> asyncPullLatches) throws SolrServerException, IOException, InterruptedException {
    // do a query to trigger core pull
    String leaderCoreName = cloudClient.getZkStateReader().getClusterState().getCollection(collectionName).getLeader("shard1").getCoreName();
    CountDownLatch latch = new CountDownLatch(1);
    asyncPullLatches.put(leaderCoreName, latch);

    cloudClient.query(collectionName, new ModifiableSolrParams().set("q", "*:*"));

    // wait until pull is finished
    assertTrue("Timed-out waiting for pull to finish", latch.await(120, TimeUnit.SECONDS));
  }

  private Map<String, CountDownLatch> stopSolrRemoveCoreRestartSolr(CloudSolrClient cloudClient, CoreStorageClient storageClient, String collectionName) throws Exception {
    // get the replica
    DocCollection collection = cloudClient.getZkStateReader().getClusterState().getCollection(collectionName);
    Replica shardLeaderReplica = collection.getLeader("shard1");

    CoreContainer cc = getCoreContainer(shardLeaderReplica.getNodeName());

    SolrCore core = cc.getCore(shardLeaderReplica.getCoreName());
    try {
      assertNotNull("core not found", core);
      assertTrue("core is empty", core.getDeletionPolicy().getLatestCommit().getGeneration() > 1L);
    } finally {
      core.close();
    }

    // get the core directory
    File coreIndexDir = new File(cc.getCoreRootDirectory() + "/" + shardLeaderReplica.getCoreName());

    // stop the cluster's node
    JettySolrRunner runner = cluster.stopJettySolrRunner(0);
    cluster.waitForJettyToStop(runner);

    // remove the core locally
    FileUtils.deleteDirectory(coreIndexDir);

    // start up the node again
    runner = cluster.startJettySolrRunner(runner, true);
    cluster.waitForNode(runner, /* seconds */ 30);
    setupTestSharedClientForNode(getBlobStorageProviderTestInstance(storageClient), cluster.getJettySolrRunner(0));
    Map<String, CountDownLatch> asyncPullLatches = configureTestBlobProcessForNode(cluster.getJettySolrRunner(0));

    collection = cloudClient.getZkStateReader().getClusterState().getCollection(collectionName);
    shardLeaderReplica = collection.getLeader("shard1");

    // at core container load time we will discover that we have a replica for this node on ZK but no corresponding core
    // thus, we will create one at load time
    cc = getCoreContainer(shardLeaderReplica.getNodeName());
    core = cc.getCore(shardLeaderReplica.getCoreName());
    try {
      assertNotNull("core not found", core);
      assertEquals("core is not empty", 1L, core.getDeletionPolicy().getLatestCommit().getGeneration());
    } finally {
      core.close();
    }

    // after restart it takes some time for downed replica to be marked active
    final Replica r = shardLeaderReplica;
    waitForState("Timeout occurred while waiting for replica to become active ", collectionName,
        (liveNodes, collectionState) -> collectionState.getReplica(r.getName()).getState() == State.ACTIVE);

    return asyncPullLatches;
  }

  private void assertQueryReturnsAllDocs(CloudSolrClient cloudClient, String collectionName, List<String> expectedDocs) throws Exception {
    ModifiableSolrParams params = new ModifiableSolrParams().set("q", "*:*");
    QueryResponse resp = cloudClient.query(collectionName, params);
    assertEquals("wrong number of docs", expectedDocs.size(), resp.getResults().getNumFound());
    List<String> actualDocs = resp.getResults().stream().map(r -> (String) r.getFieldValue("id")).collect(Collectors.toList());
    Collections.sort(expectedDocs);
    Collections.sort(actualDocs);
    assertEquals("wrong docs", expectedDocs.toString(), actualDocs.toString());
  }
}
