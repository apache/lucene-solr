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
package org.apache.solr.store.blob.process;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.api.collections.Assign;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.store.blob.client.CoreStorageClient;
import org.apache.solr.store.blob.metadata.DeleteBlobStrategyTest;
import org.apache.solr.store.blob.process.BlobDeleterTask.BlobDeleterTaskResult;
import org.apache.solr.store.shared.SolrCloudSharedStoreTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests around the deletion of files from shared storage via background deletion
 * processes as triggered by normal indexing and collection api calls
 */
public class SharedStoreDeletionProcessTest extends SolrCloudSharedStoreTestCase {
  
  private static String DEFAULT_PROCESSOR_NAME = "DeleterForTest";
  private static Path sharedStoreRootPath;
  
  private List<CompletableFuture<BlobDeleterTaskResult>> taskFutures;
  private List<CompletableFuture<BlobDeleterTaskResult>> overseerTaskFutures;
  
  @BeforeClass
  public static void setupClass() throws Exception {
    sharedStoreRootPath = createTempDir("tempDir");    
  }
  
  @Before
  public void setupTest() {
    taskFutures = new LinkedList<>();
    overseerTaskFutures = new LinkedList<>();
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
   * Test that verifies that files marked for deletion during the indexing process get
   * enqueued for deletion and deleted. This test case differs from {@link DeleteBlobStrategyTest}
   * because it tests the end-to-end indexing flow with a {@link MiniSolrCloudCluster}
   */
  @Test
  public void testIndexingTriggersDeletes() throws Exception {
    setupCluster(1);
    setupSolrNodes();
    JettySolrRunner node = cluster.getJettySolrRunner(0);
    int numThreads = 5;
    int targetMaxAttempts = 5;
    // files don't need to age before marked for deletion, deleted as indexed in this test
    int delay = 0;
    initiateDeleteManagerForTest(node, targetMaxAttempts, numThreads, delay,
        taskFutures, overseerTaskFutures);
    
    // set up the collection
    String collectionName = "SharedCollection";
    int maxShardsPerNode = 1;
    int numReplicas = 1;
    String shardNames = "shard1";
    setupSharedCollectionWithShardNames(collectionName, maxShardsPerNode, numReplicas, shardNames);
    
    // index and track deletions, commits are implicit in shared storage so we expect pushes to occur 
    // per batch and new segment index files to be deleted per batch with the exception of the very first
    sendIndexingBatch(collectionName, /*numDocs */ 1, /* docIdStart */ 0);
    assertEquals(0, taskFutures.size());
    
    // next indexing batch should cause files to be added for deletion
    sendIndexingBatch(collectionName, /*numDocs */ 1, /* docIdStart */ 1);
    assertEquals(1, taskFutures.size());
    
    CompletableFuture<BlobDeleterTaskResult> cf = taskFutures.get(0);
    BlobDeleterTaskResult result = cf.get(5000, TimeUnit.MILLISECONDS);
    
    // verify the files were deleted
    CoreStorageClient storageClient = node.getCoreContainer().getSharedStoreManager().getBlobStorageProvider().getClient();
    assertTrue(result.isSuccess());
    assertFilesDeleted(storageClient, result);
  }
  
  /**
   * Test that verifies that collection deletion command deletes all files for the given collection on the
   * happy path
   */
  @Test
  public void testDeleteCollectionCommand() throws Exception {
    setupCluster(1);
    setupSolrNodes();
    CloudSolrClient cloudClient = cluster.getSolrClient();
    JettySolrRunner node = cluster.getJettySolrRunner(0);
    int numThreads = 5;
    int targetMaxAttempts = 5;
    // files don't need to age before marked for deletion, deleted as indexed in this test
    int delay = 0;
    initiateDeleteManagerForTest(node, targetMaxAttempts, numThreads, delay,
        taskFutures, overseerTaskFutures);
    
    // set up the collection
    String collectionName = "SharedCollection";
    int maxShardsPerNode = 10;
    int numReplicas = 1;
    String shardNames = "shard1,shard2";
    setupSharedCollectionWithShardNames(collectionName, maxShardsPerNode, numReplicas, shardNames);
    
    // index a bunch of docs
    for (int i = 0; i < 10; i++) {
      int numDocs = 100;
      sendIndexingBatch(collectionName, numDocs, i*numDocs);
    }
    assertTrue(taskFutures.size() > 0);
    assertEquals(0, overseerTaskFutures.size());
    
    // do collection deletion
    CollectionAdminRequest.Delete delete = CollectionAdminRequest.deleteCollection(collectionName);
    delete.process(cloudClient).getResponse();
    assertEquals(1, overseerTaskFutures.size());
    
    // wait for the deletion command to complete
    CompletableFuture<BlobDeleterTaskResult> cf = taskFutures.get(0);
    BlobDeleterTaskResult result = cf.get(5000, TimeUnit.MILLISECONDS);
    
    // the collection should no longer exist on zookeeper
    cloudClient.getZkStateReader().forceUpdateCollection(collectionName);
    assertNull(cloudClient.getZkStateReader().getClusterState().getCollectionOrNull(collectionName));
    
    // verify the files in the deletion tasks were all deleted
    CoreStorageClient storageClient = node.getCoreContainer().getSharedStoreManager().getBlobStorageProvider().getClient();
    assertTrue(result.isSuccess());
    assertFilesDeleted(storageClient, result);
    
    // verify no files belonging to this collection exists on shared storage
    List<String> names = storageClient.listCoreBlobFiles(collectionName);
    assertTrue(names.isEmpty());
  }
  
  /**
   * Test that verifies that shard deletion command deletes all files for the given shard on the
   * happy path
   */
  @Test
  public void testDeleteShardCommand() throws Exception {
    setupCluster(1);
    setupSolrNodes();
    CloudSolrClient cloudClient = cluster.getSolrClient();
    JettySolrRunner node = cluster.getJettySolrRunner(0);
    int numThreads = 5;
    int targetMaxAttempts = 5;
    // files don't need to age before marked for deletion, deleted as indexed in this test
    int delay = 0;
    initiateDeleteManagerForTest(node, targetMaxAttempts, numThreads, delay,
        taskFutures, overseerTaskFutures);
    
    // set up the collection
    String collectionName = "SharedCollection";
    int maxShardsPerNode = 10;
    int numReplicas = 1;
    String shardNames = "shard1";
    setupSharedCollectionWithShardNames(collectionName, maxShardsPerNode, numReplicas, shardNames);
    
    // index a bunch of docs
    for (int i = 0; i < 10; i++) {
      int numDocs = 100;
      sendIndexingBatch(collectionName, numDocs, i*numDocs);
    }
    assertTrue(taskFutures.size() > 0);
    assertEquals(0, overseerTaskFutures.size());
    
    // split the shard so the parents are set inactive and can be deleted 
    CollectionAdminRequest.SplitShard splitShard = CollectionAdminRequest.splitShard(collectionName)
        .setShardName("shard1");
    splitShard.process(cloudClient);
    waitForState("Timed out waiting for sub shards to be active.",
        collectionName, activeClusterShape(2, 3));
    
    // do shard deletion on the parent
    CollectionAdminRequest.DeleteShard delete = CollectionAdminRequest.deleteShard(collectionName, "shard1");
    delete.process(cloudClient).getResponse();
    assertEquals(1, overseerTaskFutures.size());
    
    // wait for the deletion command to complete
    CompletableFuture<BlobDeleterTaskResult> cf = taskFutures.get(0);
    BlobDeleterTaskResult result = cf.get(5000, TimeUnit.MILLISECONDS);
    
    // the collection shard should no longer exist on zookeeper
    cloudClient.getZkStateReader().forceUpdateCollection(collectionName);
    DocCollection coll = cloudClient.getZkStateReader().getClusterState().getCollectionOrNull(collectionName); 
    assertNotNull(coll);
    assertNull(coll.getSlice("shard1"));
    
    // verify the files in the deletion tasks were all deleted
    CoreStorageClient storageClient = node.getCoreContainer().getSharedStoreManager().getBlobStorageProvider().getClient();
    assertTrue(result.isSuccess());
    assertFilesDeleted(storageClient, result);
    
    // verify no files belonging to shard1 in the collection exists on shared storage
    List<String> names = storageClient.listCoreBlobFiles(Assign.buildSharedShardName(collectionName, "shard1/"));
    assertTrue(names.isEmpty());
    
    // verify files belonging to shard1_0 and shard1_1 exist still
    names = storageClient.listCoreBlobFiles(Assign.buildSharedShardName(collectionName, "shard1_0"));
    assertFalse(names.isEmpty());
    names = storageClient.listCoreBlobFiles(Assign.buildSharedShardName(collectionName, "shard1_1"));
    assertFalse(names.isEmpty());
  }
  
  private void assertFilesDeleted(CoreStorageClient storageClient, BlobDeleterTaskResult result) throws IOException {
    Collection<String> filesDeleted = result.getFilesDeleted();
    for (String filename : filesDeleted) {
      InputStream s = null;
      try  {
        s = storageClient.pullStream(filename);
        fail(filename + " should have been deleted from shared store");
      } catch (Exception ex) {
        if (!(ex.getCause() instanceof FileNotFoundException)) {
          fail("Unexpected exception thrown = " + ex.getMessage());
        }
      } finally {
        if (s != null) {
          s.close();
        }
      }
    }
  }
  
  private void sendIndexingBatch(String collectionName, int numberOfDocs, int docIdStart) throws SolrServerException, IOException {
    UpdateRequest updateReq = new UpdateRequest();
    for (int k = docIdStart; k < docIdStart + numberOfDocs; k++) {
      updateReq.add("id", Integer.toString(k));
    }
    updateReq.process(cluster.getSolrClient(), collectionName);
  }
  
  private BlobDeleteManager initiateDeleteManagerForTest(JettySolrRunner solrRunner, 
      int almostMaxQueueSize, int numDeleterThreads, int deleteDelayMs, List<CompletableFuture<BlobDeleterTaskResult>> taskFutures,
      List<CompletableFuture<BlobDeleterTaskResult>> overseerTaskFutures) {
    CoreStorageClient client = solrRunner.getCoreContainer().getSharedStoreManager().getBlobStorageProvider().getClient();
    int maxQueueSize = 200;
    int numThreads = 5;
    int defaultMaxAttempts = 5;
    int retryDelay = 500; 
    
    // setup processors with the same defaults but enhanced to capture future results
    BlobDeleteProcessor processor = buildBlobDeleteProcessorForTest(taskFutures, client,
        maxQueueSize, numThreads, defaultMaxAttempts, retryDelay);
    BlobDeleteProcessor overseerProcessor = buildBlobDeleteProcessorForTest(overseerTaskFutures, client,
        maxQueueSize, numThreads, defaultMaxAttempts, retryDelay);
    
    BlobDeleteManager deleteManager = new BlobDeleteManager(client, deleteDelayMs, processor, overseerProcessor);
    setupBlobDeleteManagerForNode(deleteManager, solrRunner);
    return deleteManager;
  }
  
  private void setupSolrNodes() throws Exception {
    for (JettySolrRunner process : cluster.getJettySolrRunners()) {
      CoreStorageClient storageClient = setupLocalBlobStoreClient(sharedStoreRootPath, DEFAULT_BLOB_DIR_NAME);
      setupTestSharedClientForNode(getBlobStorageProviderTestInstance(storageClient), process);
    }
  }
  
  // enables capturing all enqueues to the executor pool, including retries
  private BlobDeleteProcessor buildBlobDeleteProcessorForTest(List<CompletableFuture<BlobDeleterTaskResult>> taskFutures, 
      CoreStorageClient client, int almostMaxQueueSize, int numDeleterThreads, int defaultMaxDeleteAttempts, 
      long fixedRetryDelay) {
    return new BlobDeleteProcessorForTest(DEFAULT_PROCESSOR_NAME, client, almostMaxQueueSize, numDeleterThreads,
        defaultMaxDeleteAttempts, fixedRetryDelay, taskFutures);
  }
  
  /**
   * Test class extending BlobDeleteProcessor to allow capturing task futures
   */
  class BlobDeleteProcessorForTest extends BlobDeleteProcessor {
    List<CompletableFuture<BlobDeleterTaskResult>> futures;
    
    public BlobDeleteProcessorForTest(String name, CoreStorageClient client, int almostMaxQueueSize,
        int numDeleterThreads, int defaultMaxDeleteAttempts, long fixedRetryDelay, 
        List<CompletableFuture<BlobDeleterTaskResult>> taskFutures) {
      super(name, client, almostMaxQueueSize, numDeleterThreads, defaultMaxDeleteAttempts, fixedRetryDelay);
      this.futures = taskFutures;
    }
    
    @Override
    protected CompletableFuture<BlobDeleterTaskResult> enqueue(BlobDeleterTask task, boolean isRetry) {
      CompletableFuture<BlobDeleterTaskResult> futureRes = super.enqueue(task, isRetry);
      futures.add(futureRes);
      return futureRes;
    }
  }
}
