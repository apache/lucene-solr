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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.solr.common.SolrException;
import org.apache.solr.store.blob.client.BlobCoreMetadata;
import org.apache.solr.store.blob.client.BlobCoreMetadata.BlobFile;
import org.apache.solr.store.blob.client.BlobCoreMetadataBuilder;
import org.apache.solr.store.blob.metadata.ServerSideMetadata.CoreFileData;
import org.apache.solr.store.blob.metadata.SharedStoreResolutionUtil.SharedMetadataResolutionResult;
import org.apache.solr.store.shared.SolrCloudSharedStoreTestCase;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;

/**
 * Unit tests for {@link SharedStoreResolutionUtil}.
 */
public class SharedStoreResolutionUtilTest extends SolrCloudSharedStoreTestCase {
  
  @BeforeClass
  public static void beforeClass() {
    assumeWorkingMockito();
  }
  
  /**
   * Test that passing both local and blob as null to {@link SharedStoreResolutionUtil} throws exception.
   */
  @Test
  public void testResolveMetadata() throws Exception {
    try {
      SharedStoreResolutionUtil.resolveMetadata(null, null);
      fail("SharedStoreResolutionUtil did not throw IllegalStateException");
    } catch (SolrException ex) {
  
    }
  }
  
  /**
   * Tests that resolve returns a result with no files to push or pull when {@link BlobCoreMetadata}
   * is present but empty
   */
  @Test
  public void testNoOpWhenBCMEmpty() throws Exception {
    String sharedShardName = "sharedShardName";
    final BlobCoreMetadata blobMetadata = new BlobCoreMetadataBuilder(sharedShardName)
        .build();
      
    // expected resolution
    SharedMetadataResolutionResult expectedResult = new 
        SharedMetadataResolutionResult(Collections.emptySet(), Collections.emptySet());
    
    // do resolution
    SharedMetadataResolutionResult actual = SharedStoreResolutionUtil.resolveMetadata(null, blobMetadata);
    assertSharedStoreResolutionResult(expectedResult, actual);
  }
  
  /**
   * Tests that resolve returns a result with files to push when a null {@link BlobCoreMetadata} is 
   * passed in but {@link ServerSideMetadata} is present
   */
  @Test
  public void testPushNeededWhenBlobNull() throws Exception {
    String coreName = "localReplica";
    final long localFileSize = 10;
    // local core metadata
    final ServerSideMetadata serverMetadata = new ServerSideCoreMetadataBuilder(coreName)
      .addFile(new CoreFileData(getSolrSegmentFileName(1), localFileSize))
      .build();
      
    // expected resolution
    SharedMetadataResolutionResult expectedResult = new 
        SharedMetadataResolutionResult(serverMetadata.getFiles(), Collections.emptySet());
    
    // do resolution
    SharedMetadataResolutionResult actual = SharedStoreResolutionUtil.resolveMetadata(serverMetadata, null);
    assertSharedStoreResolutionResult(expectedResult, actual);
  }
  
  /**
   * Tests that returns a result with files to pull when a null {@link ServerSideMetadata} is
   * passed in but {@link BlobCoreMetadata} is present
   */
  @Test
  public void testPullNeededWhenLocalNull() throws Exception {
    String sharedShardName = "sharedShardName";
    final long blobFileSize = 10;
    // blob core metadata
    final BlobCoreMetadata blobMetadata = new BlobCoreMetadataBuilder(sharedShardName)
        .addFile(new BlobFile(getSolrSegmentFileName(1), getBlobFileName(1), blobFileSize))
        .build();

    // expected resolution
    SharedMetadataResolutionResult expectedResult = new 
        SharedMetadataResolutionResult(Collections.emptySet(), Arrays.asList(blobMetadata.getBlobFiles()));
    
    // do resolution
    SharedMetadataResolutionResult actual = SharedStoreResolutionUtil.resolveMetadata(null, blobMetadata);
    assertSharedStoreResolutionResult(expectedResult, actual);
  }
  
  /**
   * Tests that resolve throws an exception under missing or duplicate segments_N situations 
   */
  @Test
  public void testSegmentNCorruptionThrowsError() throws Exception {
    String sharedShardName = "sharedShardName";
    final long blobFileSize = 10;
    // blob core metadata with duplicate segment_n files
    BlobCoreMetadata blobMetadata = new BlobCoreMetadataBuilder(sharedShardName)
        .addFile(new BlobFile(getSolrSegmentFileName(1), getBlobFileName(1), blobFileSize))
        .addFile(new BlobFile(getSolrSegmentFileName(1), getBlobFileName(2), blobFileSize))
        .build();
    
    // do resolution
    try {
      SharedStoreResolutionUtil.resolveMetadata(null, blobMetadata);
      fail("Expected resolve to fail due to duplicate segment N files present in the blob files listing");
    } catch (SolrException ex) {
      
    }
    
    // blob core metadata with missing segment_n file while we have other segment files
    blobMetadata = new BlobCoreMetadataBuilder(sharedShardName)
        .addFile(new BlobFile(getSolrFileName(1), getBlobFileName(1), blobFileSize))
        .build();
    // do resolution
    try {
      SharedStoreResolutionUtil.resolveMetadata(null, blobMetadata);
      fail("Expected resolve to fail due to missing segment N files present in the blob files listing");
    } catch (SolrException ex) {
      
    }
  }
  
  /**
   * Test resolve of both {@link BlobCoreMetadata} and {@link ServerSideMetadata} contains files to push only
   */
  public void testFilesToPushResolution() throws Exception {
    String coreName = "coreName";
    String sharedShardName = "sharedShardName";
    final long fileSize = 10;
    
    CoreFileData expectedFileToPush = new CoreFileData(getSolrFileName(1), fileSize);
    
    final ServerSideMetadata serverMetadata = new ServerSideCoreMetadataBuilder(coreName)
        .addFile(new CoreFileData(getSolrSegmentFileName(1), fileSize))
        .addFile(expectedFileToPush)
        .build();
    
    final BlobCoreMetadata blobMetadata = new BlobCoreMetadataBuilder(sharedShardName)
        .addFile(new BlobFile(getSolrSegmentFileName(1), getBlobFileName(1), fileSize))
        .build();
    
    // expected resolution
    SharedMetadataResolutionResult expectedResult = new 
        SharedMetadataResolutionResult(Arrays.asList(expectedFileToPush), Collections.emptySet());
    
    // do resolution
    SharedMetadataResolutionResult actual = SharedStoreResolutionUtil.resolveMetadata(serverMetadata, blobMetadata);
    assertSharedStoreResolutionResult(expectedResult, actual);
  }
  
  /**
   * Test resolve of both {@link BlobCoreMetadata} and {@link ServerSideMetadata} contains files to pull only
   */
  public void testFilesToPullResolution() throws Exception {
    String coreName = "coreName";
    String sharedShardName = "sharedShardName";
    final long fileSize = 10;
    
    BlobFile expectedFileToPull = new BlobFile(getSolrFileName(1),  getBlobFileName(1), fileSize);
    
    final ServerSideMetadata serverMetadata = new ServerSideCoreMetadataBuilder(coreName)
        .addFile(new CoreFileData(getSolrSegmentFileName(1), fileSize))
        .build();
    
    final BlobCoreMetadata blobMetadata = new BlobCoreMetadataBuilder(sharedShardName)
        .addFile(new BlobFile(getSolrSegmentFileName(1), getBlobFileName(1), fileSize))
        .addFile(expectedFileToPull)
        .build();
    
    // expected resolution
    SharedMetadataResolutionResult expectedResult = new 
        SharedMetadataResolutionResult(Collections.emptySet(), Arrays.asList(expectedFileToPull));
    
    // do resolution
    SharedMetadataResolutionResult actual = SharedStoreResolutionUtil.resolveMetadata(serverMetadata, blobMetadata);
    assertSharedStoreResolutionResult(expectedResult, actual);
  }
  
  /**
   * Test resolve of both {@link BlobCoreMetadata} and {@link ServerSideMetadata} contains both files to 
   * push and pull
   */
  public void testFilesToPushPullResolution() throws Exception {
    String coreName = "coreName";
    String sharedShardName = "sharedShardName";
    final long fileSize = 10;
    
    CoreFileData expectedFileToPush = new CoreFileData(getSolrFileName(1), fileSize);
    BlobFile expectedFileToPull = new BlobFile(getSolrFileName(1),  getBlobFileName(1), fileSize);
    
    final ServerSideMetadata serverMetadata = new ServerSideCoreMetadataBuilder(coreName)
        .addFile(new CoreFileData(getSolrSegmentFileName(1), fileSize))
        .addFile(expectedFileToPush)
        .build();
    
    final BlobCoreMetadata blobMetadata = new BlobCoreMetadataBuilder(sharedShardName)
        .addFile(new BlobFile(getSolrSegmentFileName(1), getBlobFileName(1), fileSize))
        .addFile(expectedFileToPull)
        .build();
    
    // expected resolution
    SharedMetadataResolutionResult expectedResult = new 
        SharedMetadataResolutionResult(Arrays.asList(expectedFileToPush), Arrays.asList(expectedFileToPull));
    
    // do resolution
    SharedMetadataResolutionResult actual = SharedStoreResolutionUtil.resolveMetadata(serverMetadata, blobMetadata);
    assertSharedStoreResolutionResult(expectedResult, actual);
  }
  
  private static void assertSharedStoreResolutionResult(SharedMetadataResolutionResult expected, SharedMetadataResolutionResult actual) {
    assertCollections("filesToPull", expected.getFilesToPull(), actual.getFilesToPull());
    assertCollections("filesToPush", expected.getFilesToPush(), actual.getFilesToPush());
  }
  
  /**
   * Creates a segment file name.
   */
  private String getSolrSegmentFileName(long suffix){
    return "segments_" + suffix;
  }
  
  /**
   * Creates a solr file name.
   */
  private String getSolrFileName(long genNumber){
    return "_" + genNumber + ".cfs";
  }
  
  /**
   * Creates a dummy blob file name.
   */
  private String getBlobFileName(long suffix){
    return "blobFile_" + suffix;
  }
  
  /**
   * Asserts that two collection are same according to {@link Set#equals(Object)} semantics
   * i.e. same size and same elements irrespective of order. 
   */
  private static <T> void assertCollections(String elementType, Collection<T> expected, Collection<T> actual){
      assertEquals("Wrong number of " + elementType, expected.size(), actual.size());
      assertEquals(elementType + " are not same", expected.stream().collect(Collectors.toSet()), actual.stream().collect(Collectors.toSet()));
  }
  
  /**
   * A builder for {@link ServerSideMetadata} that uses mocking
   */
  private static class ServerSideCoreMetadataBuilder {
    final private String coreName;
    final private ImmutableSet.Builder<CoreFileData> files;

    ServerSideCoreMetadataBuilder(String coreName) {
      this.coreName = coreName;
      files = new ImmutableSet.Builder();
    }

    ServerSideCoreMetadataBuilder addFile(CoreFileData file) {
      this.files.add(file);
      return this;
    }

    public ServerSideMetadata build() {
      ServerSideMetadata serverMetadata = mock(ServerSideMetadata.class);
      when(serverMetadata.getCoreName()).thenReturn(coreName);
      when(serverMetadata.getFiles()).thenReturn(files.build());
      return serverMetadata;
    }
  }
}
