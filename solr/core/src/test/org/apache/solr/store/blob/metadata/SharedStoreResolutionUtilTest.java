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
  public void testBothLocalAndBlobNullThrows() throws Exception {
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
    final BlobCoreMetadata blobMetadata = BlobCoreMetadataBuilder.buildEmptyCoreMetadata(sharedShardName);
      
    // expected resolution
    SharedMetadataResolutionResult expectedResult = new 
        SharedMetadataResolutionResult(Collections.emptySet(), Collections.emptySet(), false);
    
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
    final long localFileChecksum = 100;
    // local core metadata
    final ServerSideMetadata serverMetadata = new ServerSideCoreMetadataBuilder(coreName, 1L)
      .addLatestCommitFile(new CoreFileData(getSolrSegmentFileName(1), localFileSize, localFileChecksum))
      .build();
      
    // expected resolution
    SharedMetadataResolutionResult expectedResult = new 
        SharedMetadataResolutionResult(serverMetadata.getLatestCommitFiles(), Collections.emptySet(), false);
    
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
    final long blobFileChecksum = 100;
    // blob core metadata
    final BlobCoreMetadata blobMetadata = new BlobCoreMetadataBuilder(sharedShardName, 1L)
        .addFile(new BlobFile(getSolrSegmentFileName(1), getBlobFileName(1), blobFileSize, blobFileChecksum))
        .build();

    // expected resolution
    SharedMetadataResolutionResult expectedResult = new 
        SharedMetadataResolutionResult(Collections.emptySet(), Arrays.asList(blobMetadata.getBlobFiles()), false);
    
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
    final long blobFileChecksum = 100;
    // blob core metadata with duplicate segment_n files
    BlobCoreMetadata blobMetadata = new BlobCoreMetadataBuilder(sharedShardName, 1L)
        .addFile(new BlobFile(getSolrSegmentFileName(1), getBlobFileName(1), blobFileSize, blobFileChecksum))
        .addFile(new BlobFile(getSolrSegmentFileName(1), getBlobFileName(2), blobFileSize, blobFileChecksum))
        .build();
    
    // do resolution
    try {
      SharedStoreResolutionUtil.resolveMetadata(null, blobMetadata);
      fail("Expected resolve to fail due to duplicate segment N files present in the blob files listing");
    } catch (SolrException ex) {
      
    }
    
    // blob core metadata with missing segment_n file while we have other segment files
    blobMetadata = new BlobCoreMetadataBuilder(sharedShardName, 1L)
        .addFile(new BlobFile(getSolrFileName(1), getBlobFileName(1), blobFileSize, blobFileChecksum))
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
  @Test
  public void testFilesToPushResolution() throws Exception {
    String coreName = "coreName";
    String sharedShardName = "sharedShardName";
    final long fileSize = 10;
    final long checksum = 100;

    CoreFileData expectedFileToPush = new CoreFileData(getSolrFileName(1), fileSize, checksum);
    
    final ServerSideMetadata serverMetadata = new ServerSideCoreMetadataBuilder(coreName, 1L)
        .addLatestCommitFile(new CoreFileData(getSolrSegmentFileName(1), fileSize, checksum))
        .addLatestCommitFile(expectedFileToPush)
        .build();
    
    final BlobCoreMetadata blobMetadata = new BlobCoreMetadataBuilder(sharedShardName, 1L)
        .addFile(new BlobFile(getSolrSegmentFileName(1), getBlobFileName(1), fileSize, checksum))
        .build();
    
    // expected resolution
    SharedMetadataResolutionResult expectedResult = new 
        SharedMetadataResolutionResult(Arrays.asList(expectedFileToPush), Collections.emptySet(), false);
    
    // do resolution
    SharedMetadataResolutionResult actual = SharedStoreResolutionUtil.resolveMetadata(serverMetadata, blobMetadata);
    assertSharedStoreResolutionResult(expectedResult, actual);
  }
  
  /**
   * Test resolve of both {@link BlobCoreMetadata} and {@link ServerSideMetadata} contains files to pull only
   */
  @Test
  public void testFilesToPullResolution() throws Exception {
    String coreName = "coreName";
    String sharedShardName = "sharedShardName";
    final long fileSize = 10;
    final long checksum = 100;

    BlobFile expectedFileToPull = new BlobFile(getSolrFileName(1),  getBlobFileName(1), fileSize, checksum);
    
    final ServerSideMetadata serverMetadata = new ServerSideCoreMetadataBuilder(coreName, 1L)
        .addLatestCommitFile(new CoreFileData(getSolrSegmentFileName(1), fileSize, checksum))
        .build();
    
    final BlobCoreMetadata blobMetadata = new BlobCoreMetadataBuilder(sharedShardName, 1L)
        .addFile(new BlobFile(getSolrSegmentFileName(1), getBlobFileName(1), fileSize, checksum))
        .addFile(expectedFileToPull)
        .build();
    
    // expected resolution
    SharedMetadataResolutionResult expectedResult = new 
        SharedMetadataResolutionResult(Collections.emptySet(), Arrays.asList(expectedFileToPull), false);
    
    // do resolution
    SharedMetadataResolutionResult actual = SharedStoreResolutionUtil.resolveMetadata(serverMetadata, blobMetadata);
    assertSharedStoreResolutionResult(expectedResult, actual);
  }
  
  /**
   * Test resolve of both {@link BlobCoreMetadata} and {@link ServerSideMetadata} contains both files to 
   * push and pull
   */
  @Test
  public void testFilesToPushPullResolution() throws Exception {
    String coreName = "coreName";
    String sharedShardName = "sharedShardName";
    final long fileSize = 10;
    final long checksum = 100;

    CoreFileData expectedFileToPush = new CoreFileData(getSolrFileName(3), fileSize, checksum);
    BlobFile expectedFileToPull = new BlobFile(getSolrFileName(2),  getBlobFileName(2), fileSize, checksum);
    
    final ServerSideMetadata serverMetadata = new ServerSideCoreMetadataBuilder(coreName, 1L)
        .addLatestCommitFile(new CoreFileData(getSolrSegmentFileName(1), fileSize, checksum))
        .addLatestCommitFile(expectedFileToPush)
        .build();
    
    final BlobCoreMetadata blobMetadata = new BlobCoreMetadataBuilder(sharedShardName, 1L)
        .addFile(new BlobFile(getSolrSegmentFileName(1), getBlobFileName(1), fileSize, checksum))
        .addFile(expectedFileToPull)
        .build();
    
    // expected resolution
    SharedMetadataResolutionResult expectedResult = new 
        SharedMetadataResolutionResult(Arrays.asList(expectedFileToPush), Arrays.asList(expectedFileToPull), false);
    
    // do resolution
    SharedMetadataResolutionResult actual = SharedStoreResolutionUtil.resolveMetadata(serverMetadata, blobMetadata);
    assertSharedStoreResolutionResult(expectedResult, actual);
  }

  /**
   * Tests that local generation number being higher than blob resolves into a conflict 
   */
  @Test
  public void testHigherLocalGenerationResolution() throws Exception {
    String coreName = "coreName";
    String sharedShardName = "sharedShardName";
    final long fileSize = 10;
    final long checksum = 100;

    CoreFileData expectedFileToPush = new CoreFileData(getSolrSegmentFileName(2), fileSize, checksum);

    final ServerSideMetadata serverMetadata = new ServerSideCoreMetadataBuilder(coreName, 2L)
        .addLatestCommitFile(expectedFileToPush)
        .addLatestCommitFile(new CoreFileData(getSolrFileName(1), fileSize, checksum))
        .build();

    final BlobCoreMetadata blobMetadata = new BlobCoreMetadataBuilder(sharedShardName, 1L)
        .addFile(new BlobFile(getSolrSegmentFileName(1), getBlobFileName(1), fileSize, checksum))
        .addFile(new BlobFile(getSolrFileName(1), getBlobFileName(1), fileSize, checksum))
        .build();

    // expected resolution
    SharedMetadataResolutionResult expectedResult = new
        SharedMetadataResolutionResult(Arrays.asList(expectedFileToPush), Arrays.asList(blobMetadata.getBlobFiles()), true);

    // do resolution
    SharedMetadataResolutionResult actual = SharedStoreResolutionUtil.resolveMetadata(serverMetadata, blobMetadata);
    assertSharedStoreResolutionResult(expectedResult, actual);
  }

  /**
   * Tests that file belonging to latest commit point being present both locally and in blob but with different checksum
   * resolves into a conflict 
   */
  @Test
  public void testFileFromLatestCommitConflictsResolution() throws Exception {
    String coreName = "coreName";
    String sharedShardName = "sharedShardName";
    final long fileSize = 10;

    final ServerSideMetadata serverMetadata = new ServerSideCoreMetadataBuilder(coreName, 1L)
        .addLatestCommitFile(new CoreFileData(getSolrSegmentFileName(1), fileSize, 99))
        .build();

    final BlobCoreMetadata blobMetadata = new BlobCoreMetadataBuilder(sharedShardName, 1L)
        .addFile(new BlobFile(getSolrSegmentFileName(1), getBlobFileName(1), fileSize, 100))
        .build();

    // expected resolution
    SharedMetadataResolutionResult expectedResult = new
        SharedMetadataResolutionResult(Collections.emptySet(), Arrays.asList(blobMetadata.getBlobFiles()), true);

    // do resolution
    SharedMetadataResolutionResult actual = SharedStoreResolutionUtil.resolveMetadata(serverMetadata, blobMetadata);
    assertSharedStoreResolutionResult(expectedResult, actual);
  }

  /**
   * Tests that file belonging to previous commit point being present both locally and in blob but with different checksum
   * resolves into a conflict 
   */
  @Test
  public void testFileFromPreviousCommitConflictsResolution() throws Exception {
    String coreName = "coreName";
    String sharedShardName = "sharedShardName";
    final long fileSize = 10;
    final long checksum = 100;

    CoreFileData expectedFileToPush = new CoreFileData(getSolrSegmentFileName(2), fileSize, checksum);

    final ServerSideMetadata serverMetadata = new ServerSideCoreMetadataBuilder(coreName, 2L)
        .addLatestCommitFile(expectedFileToPush)
        .addAllCommitsFile(new CoreFileData(getSolrFileName(1), fileSize, 99))
        .build();

    final BlobCoreMetadata blobMetadata = new BlobCoreMetadataBuilder(sharedShardName, 3L)
        .addFile(new BlobFile(getSolrSegmentFileName(3), getBlobFileName(3), fileSize, checksum))
        .addFile(new BlobFile(getSolrFileName(1), getBlobFileName(1), fileSize, 100))
        .build();

    // expected resolution
    SharedMetadataResolutionResult expectedResult = new
        SharedMetadataResolutionResult(Arrays.asList(expectedFileToPush), Arrays.asList(blobMetadata.getBlobFiles()), true);

    // do resolution
    SharedMetadataResolutionResult actual = SharedStoreResolutionUtil.resolveMetadata(serverMetadata, blobMetadata);
    assertSharedStoreResolutionResult(expectedResult, actual);
  }

  private static void assertSharedStoreResolutionResult(SharedMetadataResolutionResult expected, SharedMetadataResolutionResult actual) {
    assertCollections("filesToPull", expected.getFilesToPull(), actual.getFilesToPull());
    assertCollections("filesToPush", expected.getFilesToPush(), actual.getFilesToPush());
    assertEquals("localConflictingWithBlob", expected.isLocalConflictingWithBlob(), actual.isLocalConflictingWithBlob());
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
    final private long generation;
    final private ImmutableSet.Builder<CoreFileData> latestCommitFiles;
    final private ImmutableSet.Builder<CoreFileData> allCommitsFiles;

    ServerSideCoreMetadataBuilder(String coreName, Long generation) {
      this.coreName = coreName;
      this.generation = generation;
      latestCommitFiles = new ImmutableSet.Builder();
      allCommitsFiles = new ImmutableSet.Builder();
    }

    ServerSideCoreMetadataBuilder addLatestCommitFile(CoreFileData file) {
      this.latestCommitFiles.add(file);
      addAllCommitsFile(file);
      return this;
    }

    ServerSideCoreMetadataBuilder addAllCommitsFile(CoreFileData file) {
      this.allCommitsFiles.add(file);
      return this;
    }

    public ServerSideMetadata build() {
      ServerSideMetadata serverMetadata = mock(ServerSideMetadata.class);
      when(serverMetadata.getCoreName()).thenReturn(coreName);
      when(serverMetadata.getGeneration()).thenReturn(generation);
      when(serverMetadata.getLatestCommitFiles()).thenReturn(latestCommitFiles.build());
      when(serverMetadata.getAllCommitsFiles()).thenReturn(allCommitsFiles.build());
      return serverMetadata;
    }
  }
}
