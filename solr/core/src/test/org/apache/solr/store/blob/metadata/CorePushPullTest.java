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
package org.apache.solr.store.blob.metadata;

import java.util.Arrays;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.store.Directory;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.store.blob.client.BlobCoreMetadata;
import org.apache.solr.store.blob.client.BlobCoreMetadataBuilder;
import org.apache.solr.store.blob.client.BlobstoreProviderType;
import org.apache.solr.store.blob.client.CoreStorageClient;
import org.apache.solr.store.blob.metadata.SharedStoreResolutionUtil.SharedMetadataResolutionResult;
import org.apache.solr.store.blob.util.BlobStoreUtils;
import org.junit.Before;
import org.junit.BeforeClass;
import org.mockito.Mockito;

/**
 * Unit tests for {@link CorePushPull}
 */
public class CorePushPullTest extends SolrTestCaseJ4 {
  
  private static CoreStorageClient client = Mockito.mock(CoreStorageClient.class);
  
  private String sharedBlobName = "collectionTest_shardTest";
  private String collectionName = "collectionTest";
  private String shardName = "shardTest";
  private String metadataSuffix = "metadataSuffix";
  
  @BeforeClass
  public static void setup() throws Exception {
    initCore("solrconfig.xml", "schema-minimal.xml");
    
    Mockito.doNothing().when(client).pushCoreMetadata(Mockito.any(), Mockito.any(), Mockito.any());
    Mockito.when(client.getStorageProvider()).thenReturn(BlobstoreProviderType.LOCAL_FILE_SYSTEM);
    Mockito.when(client.getBucketRegion()).thenReturn("bucketRegion");
    Mockito.when(client.getBucketName()).thenReturn("bucketName");
  }
  
  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    clearIndex();
    assertU(commit());
  }
  
  /*
   * Test that if a core is not found in the core container when pushing, an exception is thrown
   */
  public void testPushFailsOnMissingCore() throws Exception {
    ServerSideMetadata solrServerMetadata = Mockito.mock(ServerSideMetadata.class);
    BlobCoreMetadata blobMetadata = Mockito.mock(BlobCoreMetadata.class);
    
    CoreContainer mockCC = Mockito.mock(CoreContainer.class);
    Mockito.when(solrServerMetadata.getCoreContainer()).thenReturn(mockCC);
    Mockito.when(mockCC.getCore(Mockito.any())).thenReturn(null);
    
    // We can pass null values here because we don't expect those arguments to be interacted with.
    // If they are, a bug is introduced and should be addressed
    CorePushPull pushPull = new CorePushPull(client, null, null, solrServerMetadata, blobMetadata);
    
    // verify an exception is thrown
    try {
      pushPull.pushToBlobStore();
      fail("pushToBlobStore should have thrown an exception");
    } catch (Exception ex) {
      // core missing from core container should throw exception
    }
  }
  
  /*
   * Test that pushing to blob is successful when blob core metadata on the blob store is empty
   * (or equivalently, didn't exist).
   */
  public void testPushSucceedsOnEmptyMetadata() throws Exception {
    SolrCore core = h.getCore();
    
    // add a doc
    String docId = "docID";
    assertU(adoc("id", docId));
    assertU(commit());
    
    // build the require metadata
    ServerSideMetadata solrServerMetadata = new ServerSideMetadata(core.getName(), h.getCoreContainer());
    
    // empty bcm means we should push everything we have locally 
    BlobCoreMetadata bcm = BlobCoreMetadataBuilder.buildEmptyCoreMetadata(sharedBlobName);
    
    String randomSuffix = BlobStoreUtils.generateMetadataSuffix();
    PushPullData ppd = new PushPullData.Builder()
        .setCollectionName(collectionName)
        .setShardName(shardName)
        .setCoreName(core.getName())
        .setSharedStoreName(sharedBlobName)
        .setLastReadMetadataSuffix(metadataSuffix)
        .setNewMetadataSuffix(randomSuffix)
        .setZkVersion(1)
        .build();
    SharedMetadataResolutionResult resResult = SharedStoreResolutionUtil.resolveMetadata(solrServerMetadata, bcm);
    
    // the returned BCM is what is pushed to blob store so we should verify the push
    // was made with the correct data
    CorePushPull pushPull = new CorePushPull(client, ppd, resResult, solrServerMetadata, bcm) {
      @Override
      protected String pushFileToBlobStore(CoreStorageClient blob, Directory dir, 
          String fileName, long fileSize) throws Exception {
       return UUID.randomUUID().toString();
      }
    };
    BlobCoreMetadata returnedBcm = pushPull.pushToBlobStore();
    
    // verify we called the storage client with the right args
    Mockito.verify(client).pushCoreMetadata(
        Mockito.eq(sharedBlobName), 
        Mockito.eq(BlobStoreUtils.buildBlobStoreMetadataName(randomSuffix)),
        Mockito.any());

    // verify the return BCM is correct
    IndexCommit indexCommit = core.getDeletionPolicy().getLatestCommit();
    
    assertEquals(sharedBlobName, returnedBcm.getSharedBlobName());
    // the bcm on blob was empty so the file count should be equal to the count of the core's latest commit point
    assertEquals(indexCommit.getFileNames().size(), returnedBcm.getBlobFiles().length);
    
    // this is a little ugly but the readability is better than the alternatives
    Set<String> blobFileNames =  
        Arrays.asList(returnedBcm.getBlobFiles())
          .stream()
          .map(s -> s.getSolrFileName())
          .collect(Collectors.toSet());
    assertEquals(indexCommit.getFileNames().stream().collect(Collectors.toSet()), blobFileNames);
  }
}
