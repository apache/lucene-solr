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

package org.apache.solr.store.blob.client;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Collection;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.lucene.util.IOUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.store.blob.util.BlobStoreUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

/**
 * Unit tests for {@link LocalStorageClient}
 *
 * TODO - right now this test fails. This test should evolve into one that tests
 * the {@link CoreStorageClient} abstraction vs an actual integration test with an 
 * underlying storage provider (meaning we won't actually be writing files anywhere 
 * like we're doing here). 
 * 
 * What that means is we should be testing that each implementation type adhere's to 
 * the interface's prescribed behavior and verify input argument correctly and consistently
 * across implementations transform to the correct input arguments that underlying (mocked)
 * storage libraries expect.
 */
public class CoreStorageClientTest extends SolrTestCaseJ4 {
   
  static final String TEST_CORE_NAME_1 = "s_test_core_name1";
  static final String TEST_CORE_NAME_2 = "s_test_core_name2";
  
  private static Path sharedStoreRootPath;
  private static CoreStorageClient blobClient;
  
  static final byte[] EMPTY_BYTES_ARR = {};

  @BeforeClass
  public static void setupTestClass() throws Exception {
    sharedStoreRootPath = createTempDir("tempDir");
    System.setProperty(LocalStorageClient.BLOB_STORE_LOCAL_FS_ROOT_DIR_PROPERTY, sharedStoreRootPath.resolve("LocalBlobStore/").toString());
    blobClient = new LocalStorageClient();
  }

  @After
  public void cleanUp() throws Exception {
    File blobPath = sharedStoreRootPath.toFile();
    FileUtils.cleanDirectory(blobPath);
  }
    
  /**
   * Verify that pushing a new file to blob store returns the unique blob key for that file
   */
  @Test
  public void testPushStreamReturnsPath() throws Exception {
    String blobPath = blobClient.pushStream(TEST_CORE_NAME_1, new ByteArrayInputStream(EMPTY_BYTES_ARR), EMPTY_BYTES_ARR.length, "xxx");
    Assert.assertTrue(blobPath.contains(TEST_CORE_NAME_1));
    Assert.assertTrue(blobPath.contains("/"));
    // The generated UUID for blob files is a 128 bit value which is represented with 36 chars in a hex string
    int uuid4length = 36;
    // core name length + uuid4 length + 1 for delimiter + 4 for the "xxx." prepended
    int expectedBlobKeyLength = TEST_CORE_NAME_1.length() + uuid4length + 1 + 4;
    Assert.assertEquals(blobPath.length(), expectedBlobKeyLength);
  }
    
  @Test
  public void testListBlobCommonPrefixes() throws Exception {
    blobClient.pushStream(TEST_CORE_NAME_1, new ByteArrayInputStream(EMPTY_BYTES_ARR), EMPTY_BYTES_ARR.length, "xxx");
    blobClient.pushStream(TEST_CORE_NAME_1, new ByteArrayInputStream(EMPTY_BYTES_ARR), EMPTY_BYTES_ARR.length, "yyy");
    blobClient.pushStream(TEST_CORE_NAME_2, new ByteArrayInputStream(EMPTY_BYTES_ARR), EMPTY_BYTES_ARR.length, "zzzz");
    blobClient.pushStream(TEST_CORE_NAME_2, new ByteArrayInputStream(EMPTY_BYTES_ARR), EMPTY_BYTES_ARR.length, "1234");
    // the common prefix is s_test_core_name for these blob files
    List<String> commonPrefixes = blobClient.listCommonBlobPrefix("s_test_core_name");
    Assert.assertTrue(commonPrefixes.containsAll(ImmutableList.of(TEST_CORE_NAME_1, TEST_CORE_NAME_2)));
  }

  /**
   * Simple test of pushing content to Blob store then verifying it can be retrieved from there with preserved content.
   */
  @Test
  public void testPushPullIdentity() throws Exception {
    // We might leave local file behind if creating pulled fails, taking that risk :)
    File local = File.createTempFile("myFile", ".txt");
    File pulled = File.createTempFile("myPulledFile", ".txt");
    try {
      // Write binary data
      byte bytesWritten[] = {0, -1, 5, 10, 32, 127, -15, 20, 0, -100, 40, 0, 0, 0, (byte) BlobStoreUtils.getCurrentTimeMs()};

      FileUtils.writeByteArrayToFile(local, bytesWritten);

      String blobPath;
      blobPath = blobClient.pushStream(TEST_CORE_NAME_1, new ByteArrayInputStream(bytesWritten), bytesWritten.length, local.getName());

      // Now pull the blob file into the pulled local file
      Files.copy(blobClient.pullStream(blobPath), Paths.get(pulled.getAbsolutePath()), StandardCopyOption.REPLACE_EXISTING);

      // Verify pulled and local are the same
      byte[] bytesLocal = Files.readAllBytes(local.toPath());
      byte[] bytesPulled = Files.readAllBytes(pulled.toPath());

      Assert.assertArrayEquals("Content pulled from blob should be identical to value previously pushed", bytesLocal, bytesPulled);
      Assert.assertEquals("Pulled content expected same size as content initially written", bytesPulled.length, bytesWritten.length);
    } finally {
      IOUtils.deleteFilesIgnoringExceptions(local.toPath(), pulled.toPath());
    }
  }

  /**
   * Test we can delete a blob (file)
   */
  @Test
  public void testDeleteBlob() throws Exception {
    byte bytesWritten[] = {0, 1, 2, 5};

    // Create a blob
    String blobPath = blobClient.pushStream(TEST_CORE_NAME_1, new ByteArrayInputStream(bytesWritten), bytesWritten.length, "rrtr");

    // Pull it back (actual pulled content tested in pushPullIdentity(), not testing here).
    blobClient.pullStream(blobPath).close();

    // Delete the blob
    blobClient.deleteBlobs(ImmutableList.of(blobPath));

    // Pull it again, this should fail as the blob no longer exists
    Assert.assertTrue(pullDoesFail(blobClient, TEST_CORE_NAME_1, blobPath));
  }
    
  /**
   * Test we can batch delete a blob files
   */
  @Test
  public void testBatchDeleteBlobs() throws Exception {
    byte bytesWritten[] = {0, 1, 2, 5};

    // Create some blobs
    String blobPath1 = blobClient.pushStream(TEST_CORE_NAME_1, new ByteArrayInputStream(bytesWritten), bytesWritten.length, "azerty");
    String blobPath2 = blobClient.pushStream(TEST_CORE_NAME_1, new ByteArrayInputStream(bytesWritten), bytesWritten.length, "qsdf");
    String blobPath3 = blobClient.pushStream(TEST_CORE_NAME_1, new ByteArrayInputStream(bytesWritten), bytesWritten.length, "wxcv");
    
    // deleteBlobs should still return true if the specified file does not exist
    String blobPath4 = "pathDoesNotExist";
    Assert.assertTrue(assertDeleteSucceeds(TEST_CORE_NAME_1, ImmutableSet.of(blobPath4)));
    
    // pull them back to verify they were created (an exception will occur if we failed to push for some reason)
    blobClient.pullStream(blobPath1).close();
    blobClient.pullStream(blobPath2).close();
    blobClient.pullStream(blobPath3).close();
    
    // perform a batch delete
    Assert.assertTrue(assertDeleteSucceeds(TEST_CORE_NAME_1, ImmutableSet.of(blobPath1, blobPath2, blobPath3, blobPath4)));
    
    // Pull them again, this should fail as the blobs no longer exists
    Assert.assertTrue(pullDoesFail(blobClient, TEST_CORE_NAME_1, blobPath1));
    Assert.assertTrue(pullDoesFail(blobClient, TEST_CORE_NAME_1, blobPath2));
    Assert.assertTrue(pullDoesFail(blobClient, TEST_CORE_NAME_1, blobPath3));
  }

  /**
   * Test that we can push, check existence of, and pull the blob core metadata from the blob store
   * 
   * TODO fix this test
   */
  @Test
  public void testPushPullCoreMetadata() throws Exception {
    blobClient.deleteCore(TEST_CORE_NAME_1);
    BlobCoreMetadata pushedBcm = new BlobCoreMetadataBuilder(TEST_CORE_NAME_1, 19L).build();
    Assert.assertNull(blobClient.pullCoreMetadata(TEST_CORE_NAME_1, "core.metadata"));
    
    blobClient.pushCoreMetadata(TEST_CORE_NAME_1, "core.metadata", pushedBcm);
    Assert.assertTrue(blobClient.coreMetadataExists(TEST_CORE_NAME_1, "core.metadata"));
    BlobCoreMetadata pulledBcm = blobClient.pullCoreMetadata(TEST_CORE_NAME_1, "core.metadata");
    
    Assert.assertEquals(pushedBcm, pulledBcm);
  }
    
  private boolean pullDoesFail(CoreStorageClient blob, String coreName, String blobPath) {
    try {
      blob.pullStream(blobPath);
      return false;
    } catch (BlobException e) {
      // Expected exception: all implementations of CoreStorageClient should
      // throw an exception if we try to delete a file that is no longer there.
      return true;
    }
  }
  
  private boolean assertDeleteSucceeds(String coreName, Collection<String> keys) {
    try {
      blobClient.deleteBlobs(keys);
      return true;
    } catch (Exception e) {
      return false;
    }
  }
}
