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

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.lucene.index.IndexCommit;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.store.blob.client.BlobCoreMetadata;
import org.apache.solr.store.blob.client.BlobCoreMetadataBuilder;
import org.apache.solr.store.blob.client.CoreStorageClient;
import org.apache.solr.store.blob.client.LocalStorageClient;
import org.apache.solr.store.blob.metadata.SharedStoreResolutionUtil.SharedMetadataResolutionResult;
import org.apache.solr.store.blob.process.BlobDeleteManager;
import org.apache.solr.store.blob.util.BlobStoreUtils;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link CorePushPull}
 */
public class CorePushPullTest extends SolrTestCaseJ4 {
  
  private static BlobDeleteManager deleteManager = Mockito.mock(BlobDeleteManager.class);
  private static CoreStorageClient storageClient;
  private static Path localBlobDir;
  
  private String sharedBlobName = "collectionTest_shardTest";
  private String collectionName = "collectionTest";
  private String shardName = "shardTest";
  private String metadataSuffix = "metadataSuffix";
  
  @BeforeClass
  public static void setupTest() throws Exception {
    // set up the temp directory for a local blob store
    localBlobDir = createTempDir("tempDir");
    storageClient = new LocalStorageClient(localBlobDir.resolve("LocalBlobStore/").toString());
  }
  
  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    deleteCore();
    initCore("solrconfig.xml", "schema-minimal.xml");
    
    FileUtils.cleanDirectory(localBlobDir.toFile());
  }
  
  /*
   * Test that if a core is not found in the core container when pushing, an exception is thrown
   */
  @Test
  public void testPushFailsOnMissingCore() throws Exception {
    ServerSideMetadata solrServerMetadata = Mockito.mock(ServerSideMetadata.class);
    BlobCoreMetadata blobMetadata = Mockito.mock(BlobCoreMetadata.class);
    
    CoreContainer mockCC = Mockito.mock(CoreContainer.class);
    Mockito.when(solrServerMetadata.getCoreContainer()).thenReturn(mockCC);
    Mockito.when(mockCC.getCore(Mockito.any())).thenReturn(null);
    
    // We can pass null values here because we don't expect those arguments to be interacted with.
    // If they are, a bug is introduced and should be addressed
    CorePushPull pushPull = new CorePushPull(storageClient, deleteManager, null, null, solrServerMetadata, blobMetadata) {
      @Override
      void enqueueForHardDelete(BlobCoreMetadataBuilder bcmBuilder) throws Exception {
        return;
      }
    };
    
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
  @Test
  public void testPushSucceedsOnEmptyMetadata() throws Exception {
    SolrCore core = h.getCore();
    
    // add a doc
    String docId = "docID";
    assertU(adoc("id", docId));
    assertU(commit());
    
    // the doc should be present
    assertQ(req("*:*"), "//*[@numFound='1']");

    // do a push via CorePushPull
    BlobCoreMetadata returnedBcm = doPush(core);

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
  
  /*
   * Test that pull to blob is successful when the core locally needs to be refreshed
   */
  @Test
  public void testPullSucceedsAfterUpdate() throws Exception {
    SolrCore core = h.getCore();
    
    // add a doc
    String docId = "docID";
    assertU(adoc("id", docId));
    assertU(commit());
    
    // the doc should be present
    assertQ(req("*:*"), "//*[@numFound='1']");
    
    // do a push via CorePushPull, the returned BlobCoreMetadata is what we'd expect to find
    // on the blob store
    BlobCoreMetadata returnedBcm = doPush(core);
    // Delete the core to clear the index data and then re-create it 
    deleteCore();
    initCore("solrconfig.xml", "schema-minimal.xml");
    core = h.getCore();
    
    // the doc should not be present
    assertQ(req("*:*"), "//*[@numFound='0']");
    
    // now perform a pull
    doPull(core, returnedBcm);
    
    // the doc should be present, we should be able to index and query again
    assertQ(req("*:*"), "//*[@numFound='1']");
    assertU(adoc("id", docId + "1"));
    assertU(commit());
    assertQ(req("*:*"), "//*[@numFound='2']");
  }
  
  private BlobCoreMetadata doPush(SolrCore core) throws Exception {
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
    CorePushPull pushPull = new CorePushPull(storageClient, deleteManager, ppd, resResult, solrServerMetadata, bcm) {
      @Override
      void enqueueForHardDelete(BlobCoreMetadataBuilder bcmBuilder) throws Exception {
        return;
      }
    };
    return pushPull.pushToBlobStore();
  }
  
  private void doPull(SolrCore core, BlobCoreMetadata bcm) throws Exception {
    // build the require metadata
    ServerSideMetadata solrServerMetadata = new ServerSideMetadata(core.getName(), h.getCoreContainer());
    
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
    
    CorePushPull pushPull = new CorePushPull(storageClient, deleteManager, ppd, resResult, solrServerMetadata, bcm);
    pushPull.pullUpdateFromBlob(true);
  }  
}
