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

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.io.FileUtils;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.core.SharedStoreConfig.SharedSystemProperty;
import org.apache.solr.store.blob.client.BlobCoreMetadata;
import org.apache.solr.store.blob.client.CoreStorageClient;
import org.apache.solr.store.blob.client.LocalStorageClient;
import org.apache.solr.store.blob.process.BlobDeleteManager;
import org.apache.solr.store.blob.process.BlobProcessUtil;
import org.apache.solr.store.blob.process.CorePullTask;
import org.apache.solr.store.blob.process.CorePullTask.PullCoreCallback;
import org.apache.solr.store.blob.process.CorePullerFeeder;
import org.apache.solr.store.blob.process.CoreSyncStatus;
import org.apache.solr.store.blob.provider.BlobStorageProvider;

import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * Base class for SolrCloud tests with a few additional utilities for testing with a shared store
 *
 * Derived tests should call {@link #configureCluster(int)} in a {@code BeforeClass}
 * static method or {@code Before} setUp method.  This configures and starts a {@link MiniSolrCloudCluster}, available
 * via the {@code cluster} variable.  Cluster shutdown is handled automatically if using {@code BeforeClass}.
 *
 * <pre>
 *   <code>
 *   {@literal @}BeforeClass
 *   public static void setupCluster() {
 *     configureCluster(NUM_NODES)
 *        .addConfig("configname", pathToConfig)
 *        .configure();
 *   }
 *   </code>
 * </pre>
 */
public class SolrCloudSharedStoreTestCase extends SolrCloudTestCase {
  
  public static String DEFAULT_BLOB_DIR_NAME = "LocalBlobStore/";
  
  public static Path blobDir;
  
  @BeforeClass
  public static void setupBlobDirectory() throws Exception {
    blobDir = createTempDir("tempDir");
  }
  
  @AfterClass
  public static void cleanupBlobDirectory() throws Exception {
    if (blobDir != null) {
      FileUtils.cleanDirectory(blobDir.toFile());
    }
  }
  
  @AfterClass
  public static void afterSharedStore() {
    // clean up any properties used by shared storage tests
    System.clearProperty(LocalStorageClient.BLOB_STORE_LOCAL_FS_ROOT_DIR_PROPERTY);
    System.clearProperty(SharedSystemProperty.SharedStoreEnabled.getPropertyName());
  }
  
  protected static void setupSharedCollectionWithShardNames(String collectionName, 
      int maxShardsPerNode, int numReplicas, String shardNames) throws Exception {
    CollectionAdminRequest.Create create = CollectionAdminRequest
      .createCollectionWithImplicitRouter(collectionName, "conf", shardNames, 0)
      .setMaxShardsPerNode(maxShardsPerNode)
      .setSharedIndex(true)
      .setSharedReplicas(numReplicas);
    create.process(cluster.getSolrClient());
    int numShards = shardNames.split(",").length;

    // Verify that collection was created
    waitForState("Timed-out wait for collection to be created", collectionName, clusterShape(numShards, numShards*numReplicas));
  }

  /**
   * Spin up a {@link MiniSolrCloudCluster} with shared storage enabled and 
   * the local FS as the shared storage provider
   */
  protected static void setupCluster(int nodes) throws Exception {
    System.setProperty(LocalStorageClient.BLOB_STORE_LOCAL_FS_ROOT_DIR_PROPERTY, 
        blobDir.resolve(DEFAULT_BLOB_DIR_NAME).toString());
    System.setProperty(SharedSystemProperty.SharedStoreEnabled.getPropertyName(), "true");
    configureCluster(nodes)
      .addConfig("conf", configset("cloud-minimal"))
      .configure();
  }
  
  /**
   * Spin up a {@link MiniSolrCloudCluster} with shared storage disabled
   */
  protected static void setupClusterSharedDisable(int nodes) throws Exception {
    // make sure the feature is explicitly disabled in case programmer error results in the property remaining enabled
    System.setProperty(SharedSystemProperty.SharedStoreEnabled.getPropertyName(), "false");
    configureCluster(nodes)
      .addConfig("conf", configset("cloud-minimal"))
      .configure();
  }
  
  /**
   * Configures the Solr process with the given BlobStorageProvider
   */
  protected static void setupTestSharedClientForNode(BlobStorageProvider testBlobStorageProvider, JettySolrRunner solrRunner) {
    SharedStoreManager manager = solrRunner.getCoreContainer().getSharedStoreManager();
    manager.initBlobStorageProvider(testBlobStorageProvider);
  }
  
  /**
   * Configures the Solr process with the given BlobProcessUtil
   */
  protected static void setupTestBlobProcessUtilForNode(BlobProcessUtil testBlobProcessUtil, JettySolrRunner solrRunner) {
    SharedStoreManager manager = solrRunner.getCoreContainer().getSharedStoreManager();
    manager.initBlobProcessUtil(testBlobProcessUtil);
  }

  /**
   * Configures the Solr process with the given {@link SharedCoreConcurrencyController}
   */
  protected static void setupTestSharedConcurrencyControllerForNode(SharedCoreConcurrencyController concurrencyController, JettySolrRunner solrRunner) {
    SharedStoreManager manager = solrRunner.getCoreContainer().getSharedStoreManager();
    manager.initConcurrencyController(concurrencyController);
  }
  
  /**
   * Configures the Solr process with the given {@link BlobDeleteManager}
   */
  protected static void setupBlobDeleteManagerForNode(BlobDeleteManager deleteManager, JettySolrRunner solrRunner) {
    SharedStoreManager manager = solrRunner.getCoreContainer().getSharedStoreManager();
    manager.initBlobDeleteManager(deleteManager);
  }

  /**
   * Return a new CoreStorageClient that writes to the specified sharedStoreRootPath and blobDirectoryName
   * The sharedStoreRootPath should already exist when passed to this method
   */
  protected static CoreStorageClient setupLocalBlobStoreClient(Path sharedStoreRootPath, String blobDirectoryName) throws Exception {
    System.setProperty(LocalStorageClient.BLOB_STORE_LOCAL_FS_ROOT_DIR_PROPERTY, sharedStoreRootPath.resolve(blobDirectoryName).toString());
    return new LocalStorageClient();
  }
  
  protected static BlobStorageProvider getBlobStorageProviderTestInstance(CoreStorageClient client) {
    BlobStorageProvider testBlobStorageProvider = new BlobStorageProvider() {
      @Override
      public CoreStorageClient getClient() {
        return client;
      }
    };
    return testBlobStorageProvider;
  }
  
  /**
   * Initiates a new test BlobProcessUtil to be injected into a Solr node within MiniSolrCloudCluster.
   * BlobProcessUtil defines a CorePullerFeeder to allow for async pull testing. 
   */
  protected static Map<String, CountDownLatch> configureTestBlobProcessForNode(JettySolrRunner runner) {
    final Map<String, CountDownLatch> asyncPullTracker = new HashMap<>();

    final PullCoreCallback callback = new PullCoreCallback() {
      @Override
      public void finishedPull(CorePullTask pullTask, BlobCoreMetadata blobMetadata, CoreSyncStatus status,
          String message) throws InterruptedException {
        CountDownLatch latch = asyncPullTracker.get(pullTask.getPullCoreInfo().getDedupeKey());
        if (latch != null) {
          latch.countDown();
        }
      }
    };
    
    CorePullerFeeder cpf = new CorePullerFeeder(runner.getCoreContainer().getSharedStoreManager()) {  
      @Override
      protected CorePullTask.PullCoreCallback getCorePullTaskCallback() {
        return callback;
      }
    };

    BlobProcessUtil testUtil = new BlobProcessUtil();
    testUtil.load(cpf);
    setupTestBlobProcessUtilForNode(testUtil, runner);
    return asyncPullTracker;
  }

}
