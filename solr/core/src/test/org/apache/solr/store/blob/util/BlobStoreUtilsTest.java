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

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
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
import org.apache.solr.store.blob.provider.BlobStorageProvider;
import org.apache.solr.store.shared.metadata.SharedShardMetadataController;
import org.hamcrest.core.StringContains;
import java.util.UUID;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.anyString;


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
  @BeforeClass
  public static void setupCluster() throws Exception {    
    configureCluster(1)
      .addConfig("conf", configset("cloud-minimal"))
      .configure();
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
      zk.getBlobStorageProvider().getDefaultClient().deleteCore(sharedStoreName);
    }
  }
  
  /**
   *  testSyncLocalCoreWithSharedStore_NoSuchElementZK checks that syncLocalCoreWithSharedStore 
   *  will throw exception if metadataSuffix can't be found in the ZK.
   */
  @Test
  public void testSyncLocalCoreWithSharedStore_NoSuchElementZK() throws Exception {
    CoreContainer ccSpy = Mockito.spy(cc);
    ZkController zkSpy = Mockito.spy(zk);
    
    Mockito.when(ccSpy.getZkController()).thenReturn(zkSpy);
    try {
      BlobStoreUtils.syncLocalCoreWithSharedStore(collectionName, newReplica.getCoreName(),shardName, ccSpy);
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
   *  testSyncLocalCoreWithSharedStore_syncEquivalent checks that syncLocalCoreWithSharedStore 
   *  will skip sync if metadataSuffix is set to default in the ZK.
   */
  @Test
  public void testSyncLocalCoreWithSharedStore_syncEquivalent() throws Exception {
    CoreContainer ccSpy = Mockito.spy(cc);
    ZkController zkSpy = Mockito.spy(zk);
    BlobStorageProvider blobProvideMock = Mockito.mock(BlobStorageProvider.class);
    CoreStorageClient blobClientMock = Mockito.mock(CoreStorageClient.class);
    
    Mockito.when(ccSpy.getZkController()).thenReturn(zkSpy);
    Mockito.when(zkSpy.getBlobStorageProvider()).thenReturn(blobProvideMock);
    Mockito.when(blobProvideMock.getDefaultClient()).thenReturn(blobClientMock);
    
    SharedShardMetadataController sharedMetadataController = zk.getSharedShardMetadataController();
    sharedMetadataController.ensureMetadataNodeExists(collectionName, shardName);
    try {
      BlobStoreUtils.syncLocalCoreWithSharedStore(collectionName, newReplica.getCoreName(),shardName, ccSpy);
      verify(blobClientMock,never()).pullCoreMetadata(anyString(),anyString());
    } catch (Exception ex){
      fail("syncLocalCoreWithSharedStore failed with exception: " + ex.getMessage() );
    } 
  }
  
  /**
   *  testSyncLocalCoreWithSharedStore_missingBlob checks that syncLocalCoreWithSharedStore 
   *  will throw exception if core.metadata file is missing from the sharedStore.
   */
  @Test
  public void testSyncLocalCoreWithSharedStore_missingBlob() throws Exception {
    CoreContainer ccSpy = Mockito.spy(cc);
    ZkController zkSpy = Mockito.spy(zk);
    Mockito.when(ccSpy.getZkController()).thenReturn(zkSpy);
    SharedShardMetadataController sharedMetadataController = zk.getSharedShardMetadataController();
    sharedMetadataController.ensureMetadataNodeExists(collectionName, shardName);
    sharedMetadataController.updateMetadataValueWithVersion(collectionName, shardName, UUID.randomUUID().toString(), -1);
    try {
      BlobStoreUtils.syncLocalCoreWithSharedStore(collectionName, newReplica.getCoreName(),shardName, ccSpy);
      fail("syncLocalCoreWithSharedStore should throw exception if shared store doesn't have the core.metadata file.");
    } catch (Exception ex){
      String expectedException = "cannot get core.metadata file from shared store";
      assertThat(ex.getMessage(), StringContains.containsString(expectedException)); 
    } 
  }
  
  /**
   *  testSyncLocalCoreWithSharedStore_syncSuccessful checks that syncLocalCoreWithSharedStore 
   *  doesn't throw an exception if shared store and local files, already are in sync.
   */
  @Test
  public void testSyncLocalCoreWithSharedStore_syncSuccessful() throws Exception {
    SolrInputDocument doc;
    CloudSolrClient cloudClient = cluster.getSolrClient();
    // Add a document.
    doc = new SolrInputDocument();
    doc.setField("id", "1");
    doc.setField("cat", "cat123");
    UpdateRequest req = new UpdateRequest();
    req.add(doc);
    req.process(cloudClient, collectionName);
    req.commit(cloudClient, collectionName);
    try {
      BlobStoreUtils.syncLocalCoreWithSharedStore(collectionName, newReplica.getCoreName(),shardName, cc);
    } catch (Exception ex){ 
      fail("syncLocalCoreWithSharedStore failed with exception: " + ex.getMessage());
    }
  }
  
}
