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
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
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
 * A simple end-to-end test for collections using a shared store. Missing cores
 * refers to case in which shard index exists on the source-of-truth for shared
 * collections, the shared store, but is missing locally on the solr node. The
 * metadata for the index shard exists on ZK which indicates Solr Cloud is aware
 * of the shard and requests trigger it being pulled 
 */
public class SimpleSharedStoreMissingCorePullTest extends SolrCloudSharedStoreTestCase {
  
  private static Path sharedStoreRootPath;
  
  @BeforeClass
  public static void setupClass() throws Exception {
    sharedStoreRootPath = createTempDir("tempDir");    
  }
  
  @After
  public void teardownTest() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
    // clean up the shared store after each test. The temp dir should clean up itself after the
    // test class finishes
    FileUtils.cleanDirectory(sharedStoreRootPath.toFile());
  }
  
  /**
   * Tests that if a core is missing locally on a solr node, the replica is known to 
   * Solr Cloud (present in ZK), and the index data is present on the shared store,
   * then we pull the core from the shared store.
   */
  @Test
  public void testMissingCorePullSucceeds() throws Exception {
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
    
    // send an update to the cluster
    UpdateRequest updateReq = new UpdateRequest();
    updateReq.add("id", "1");
    updateReq.commit(cloudClient, collectionName);
    
    // get the replica
    DocCollection collection = cloudClient.getZkStateReader().getClusterState().getCollection(collectionName);
    Replica shardLeaderReplica = collection.getLeader("shard1");

    CoreContainer cc = getCoreContainer(shardLeaderReplica.getNodeName());
    SolrCore core = null;
    try {
      // verify the document is present
      ModifiableSolrParams params = new ModifiableSolrParams().set("q", "*:*");
      QueryResponse resp = cloudClient.query(collectionName, params);
      assertEquals(1, resp.getResults().getNumFound());
      assertEquals("1", (String) resp.getResults().get(0).getFieldValue("id"));
      
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
      
      // verify we don't find the core on the node
      cc = getCoreContainer(shardLeaderReplica.getNodeName());
      core = cc.getCore(shardLeaderReplica.getCoreName());
      
      assertNull(core);

      // do a query to trigger missing core pulls - this client request will wait for 5 seconds
      // before the request fails with pull in progress. This is okay as the syncer is meant
      // to support this.
      // We want to wait until the pull completes so set up a count down latch for the follower's
      // core that we'll wait until pull finishes before verifying
      CountDownLatch latch = new CountDownLatch(1);
      asyncPullLatches.put(shardLeaderReplica.getCoreName(), latch);

      try {
        resp = cloudClient.query(collectionName, params);
        assertEquals(1, resp.getResults().getNumFound());
        assertEquals("1", (String) resp.getResults().get(0).getFieldValue("id"));
      } catch (Exception ex) {
        // the query may fail if it waits longer than PULL_WAITING_MS which is 5 seconds 
        // so we can accept that in this test
      }
      
      // wait until pull is finished
      assertTrue(latch.await(120, TimeUnit.SECONDS));

      core = cc.getCore(shardLeaderReplica.getCoreName());
      // verify we pulled
      assertTrue(core.getDeletionPolicy().getLatestCommit().getFileNames().size() > 1);
      
      // verify the document is present now
      resp = cloudClient.query(collectionName, params);
      assertEquals(1, resp.getResults().getNumFound());
      assertEquals("1", (String) resp.getResults().get(0).getFieldValue("id"));
    } finally {
      if (core != null) {
        core.close();
      }
    }
  }

  private Map<String, CountDownLatch> configureTestBlobProcessForNode(JettySolrRunner runner) {
    Map<String, CountDownLatch> asyncPullTracker = new HashMap<>();

    CorePullerFeeder cpf = new CorePullerFeeder(runner.getCoreContainer()) {
      @Override
      protected CorePullTask.PullCoreCallback getCorePullTaskCallback() {
        return new PullCoreCallback() {
          @Override
          public void finishedPull(CorePullTask pullTask, BlobCoreMetadata blobMetadata, CoreSyncStatus status,
              String message) throws InterruptedException {
            CountDownLatch latch = asyncPullTracker.get(pullTask.getPullCoreInfo().getCoreName());
            latch.countDown();
          }
        };
      }
    };

    BlobProcessUtil testUtil = new BlobProcessUtil(runner.getCoreContainer(), cpf);
    setupTestBlobProcessUtilForNode(testUtil, runner);
    return asyncPullTracker;
  }
}
