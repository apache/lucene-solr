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

package org.apache.solr.store.blob.util;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.nio.file.Path;
import java.util.UUID;

import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.store.blob.client.CoreStorageClient;
import org.apache.solr.store.blob.client.LocalStorageClient;
import org.apache.solr.store.blob.provider.BlobStorageProvider;
import org.apache.solr.store.shared.SharedStoreManager;
import org.apache.solr.store.shared.metadata.SharedShardMetadataController;
import org.hamcrest.core.StringContains;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link BlobStoreUtils}
 */
public class BlobStoreUtilsTest extends SolrCloudTestCase { 
  private String collectionName;
  private String shardName;
  private String sharedStoreName;
  private Replica newReplica;
  private CoreContainer cc;
  private ZkController zk;
  
  private static CoreStorageClient storageClient;
  private static BlobStorageProvider providerTestHarness = new BlobStorageProvider() {
    @Override
    public CoreStorageClient getDefaultClient() {
      return storageClient;
    }
    
    @Override
    public CoreStorageClient getClient(String localBlobDir, String blobBucketName, String blobStoreEndpoint,
        String blobStoreAccessKey, String blobStoreSecretKey, String blobStorageProvider) {
      return storageClient;
    }
  };  
  
  @BeforeClass
  public static void setupCluster() throws Exception {    
    configureCluster(1)
      .addConfig("conf", configset("cloud-minimal"))
      .configure();
    
    // setup a local blob store client to be used by the Mini Solr Cluster for testing
    setupLocalBlobStoreClient("LocalBlobStore/");
    // provision the harness on each node
    setupBlobProviderTestHarness();
  }
  
  @Before
  public void doBefore() throws Exception{
    collectionName = "sharedCol" + UUID.randomUUID();
    shardName = "shard" + UUID.randomUUID();
    CloudSolrClient cloudClient = cluster.getSolrClient();
    CollectionAdminRequest.Create create = CollectionAdminRequest
        .createCollectionWithImplicitRouter(collectionName, "conf", shardName, 0)
          .setSharedIndex(true)
          .setSharedReplicas(1);
    create.process(cloudClient);
    waitForState("Timed-out wait for collection to be created", collectionName, clusterShape(1, 1));
    assertTrue(cloudClient.getZkStateReader().getZkClient().exists(ZkStateReader.COLLECTIONS_ZKNODE + "/" + collectionName, false));
    DocCollection collection = cloudClient.getZkStateReader().getClusterState().getCollection(collectionName);
    newReplica = collection.getReplicas().get(0);
    cc = getCoreContainer(newReplica.getNodeName());
    zk = cc.getZkController();
    Slice shard = collection.getSlicesMap().get(shardName);
    if (null != shard) {
      sharedStoreName = (String)shard.get(ZkStateReader.SHARED_SHARD_NAME);
    } 
  }
  
  @After
  public void doAfter() throws Exception {
    cluster.deleteAllCollections();
    if (null!= sharedStoreName) {
      cc.getSharedStoreManager().getBlobStorageProvider().getDefaultClient().deleteCore(sharedStoreName);
    }
  }
  
  /**
   * testSyncLocalCoreWithSharedStore_NoSuchElementZK checks that syncLocalCoreWithSharedStore 
   * will throw exception if metadataSuffix can't be found in the ZK.
   */
  @Test
  public void testSyncLocalCoreWithSharedStore_NoSuchElementZK() throws Exception {
    try {
      BlobStoreUtils.syncLocalCoreWithSharedStore(collectionName, newReplica.getCoreName(), shardName, cc);
      fail("syncLocalCoreWithSharedStore should throw exception if zookeeper doesn't have metadataSuffix.");
    } catch (Exception ex){
      String expectedException = "Error reading data from path: " +
                                  ZkStateReader.COLLECTIONS_ZKNODE + "/" +
                                  collectionName + "/" + 
                                  ZkStateReader.SHARD_LEADERS_ZKNODE + "/" + 
                                  shardName;
      assertThat(ex.getMessage(), StringContains.containsString(expectedException)); 
    } 
  }
  
  /**
   * testSyncLocalCoreWithSharedStore_syncEquivalent checks that syncLocalCoreWithSharedStore 
   * will skip sync if metadataSuffix is set to default in the ZK.
   */
  @Test
  public void testSyncLocalCoreWithSharedStore_syncEquivalent() throws Exception {
    CoreStorageClient blobClientSpy = Mockito.spy(storageClient);    
    SharedShardMetadataController sharedMetadataController = cc.getSharedStoreManager().getSharedShardMetadataController();
    sharedMetadataController.ensureMetadataNodeExists(collectionName, shardName);
    try {
      BlobStoreUtils.syncLocalCoreWithSharedStore(collectionName, newReplica.getCoreName(), shardName, cc);
      verify(blobClientSpy, never()).pullCoreMetadata(anyString(), anyString());
    } catch (Exception ex){
      fail("syncLocalCoreWithSharedStore failed with exception: " + ex.getMessage());
    } 
  }
  
  /**
   * testSyncLocalCoreWithSharedStore_missingBlob checks that syncLocalCoreWithSharedStore 
   * will throw exception if core.metadata file is missing from the sharedStore.
   */
  @Test
  public void testSyncLocalCoreWithSharedStore_missingBlob() throws Exception {
    SharedShardMetadataController sharedMetadataController = cc.getSharedStoreManager().getSharedShardMetadataController();
    sharedMetadataController.ensureMetadataNodeExists(collectionName, shardName);
    sharedMetadataController.updateMetadataValueWithVersion(collectionName, shardName, UUID.randomUUID().toString(), -1);
    try {
      BlobStoreUtils.syncLocalCoreWithSharedStore(collectionName, newReplica.getCoreName(),shardName, cc);
      fail("syncLocalCoreWithSharedStore should throw exception if shared store doesn't have the core.metadata file.");
    } catch (Exception ex){
      String expectedException = "cannot get core.metadata file from shared store";
      assertThat(ex.getMessage(), StringContains.containsString(expectedException)); 
    } 
  }
  
  /**
   * testSyncLocalCoreWithSharedStore_syncSuccessful checks that syncLocalCoreWithSharedStore 
   * doesn't throw an exception if shared store and local files, already are in sync.
   */
  @Test
  public void testSyncLocalCoreWithSharedStore_syncSuccessful() throws Exception {
    CoreStorageClient blobClientSpy = Mockito.spy(storageClient);
    // Add a document.
    SolrInputDocument doc = new SolrInputDocument();
    doc.setField("id", "1");
    doc.setField("cat", "cat123");
    UpdateRequest req = new UpdateRequest();
    req.add(doc);
    req.commit(cluster.getSolrClient(), collectionName);
    try {
      // we push and already have the latest updates so we should not pull here
      BlobStoreUtils.syncLocalCoreWithSharedStore(collectionName, newReplica.getCoreName(), shardName, cc);
      verify(blobClientSpy, never()).pullCoreMetadata(anyString(), anyString());
    } catch (Exception ex) { 
      fail("syncLocalCoreWithSharedStore failed with exception: " + ex.getMessage());
    }
  }
  
  // adds the test harness to each solr process in the MiniSolrCluster
  private static void setupBlobProviderTestHarness() {
    for (JettySolrRunner solrRunner : cluster.getJettySolrRunners()) {
      SharedStoreManager manager = solrRunner.getCoreContainer().getSharedStoreManager();
      manager.initBlobStorageProvider(providerTestHarness);
    }
  }
  
  // setup a blob local storage client that writes to the blobDirName within a test temp dir
  // that automatically gets cleaned up
  private static Path setupLocalBlobStoreClient(String blobDirName) throws Exception {
    // set up the temp directory for a local blob store
    Path localBlobDir = createTempDir("tempDir");
    storageClient = new LocalStorageClient(localBlobDir.resolve(blobDirName).toString());
    return localBlobDir;
  }
}
