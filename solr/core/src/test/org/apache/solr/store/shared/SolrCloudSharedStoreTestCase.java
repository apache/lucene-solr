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

import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.store.blob.client.CoreStorageClient;
import org.apache.solr.store.blob.client.LocalStorageClient;
import org.apache.solr.store.blob.process.BlobProcessUtil;
import org.apache.solr.store.blob.provider.BlobStorageProvider;

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

  protected static void setupCluster(int nodes) throws Exception {
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

}
