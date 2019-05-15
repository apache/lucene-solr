package org.apache.solr.store.blob.client;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.nio.file.*;
import java.util.*;

import org.apache.commons.io.FileUtils;
import org.junit.*;
//import org.junit.runner.RunWith;
//import org.junit.runners.Parameterized;
//import org.junit.runners.Parameterized.Parameter;
//import org.junit.runners.Parameterized.Parameters;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.apache.solr.store.blob.provider.BlobStorageProvider;

/**
 * Unit tests for {@link LocalStorageClient}
 *
 * @author iginzburg
 * @since 214/solr.6
 */
//@RunWith(Parameterized.class)
public class CoreStorageClientTest {
   
    static final String TEST_CORE_NAME_1 = "s_test_core_name1";
    static final String TEST_CORE_NAME_2 = "s_test_core_name2";

    File testDir;
    CoreStorageClient blobClient;
//    @Parameter
//    public BlobstoreProviderType provider;
    
    static final byte[] EMPTY_BYTES_ARR = {};
    
//    /**
//     * Defines a list of blobstore implementations that this test will run against.
//     */
//    @Parameters(name = "provider={0}")
//    public static Collection<Object[]> data() {
//        return Arrays.asList(new Object[][] {
//                { BlobstoreProviderType.LOCAL_FILE_SYSTEM },
//                { BlobstoreProviderType.S3 }
//        });
//    }

    @Before
    public void setup() throws Exception {
        BlobStorageProvider.init();
        blobClient = BlobStorageProvider.get().getBlobStorageClient();

        blobClient.deleteCore(TEST_CORE_NAME_1);
        blobClient.deleteCore(TEST_CORE_NAME_2);
    }

    @After
    public void cleanUp() throws Exception {
        String localDirHome = System.getProperty("blob.local.dir", "/tmp/BlobStoreLocal/");
        FileUtils.deleteQuietly(new File(localDirHome));
        blobClient.deleteCore(TEST_CORE_NAME_1);
    }
    
    /**
     * Verify that pushing a new file to blob store returns the unique blob key for that file
     */
    @Test
    public void testPushStreamReturnsPath() throws Exception {
        String blobPath = blobClient.pushStream(TEST_CORE_NAME_1, new ByteArrayInputStream(EMPTY_BYTES_ARR), EMPTY_BYTES_ARR.length);
        Assert.assertTrue(blobPath.contains(TEST_CORE_NAME_1));
        Assert.assertTrue(blobPath.contains("/"));
        // The generated UUID for blob files is a 128 bit value which is represented with 36 chars in a hex string
        int uuid4length = 36;
        // core name length + uuid4 length + 1 for delimiter 
        int expectedBlobKeyLength = TEST_CORE_NAME_1.length() + uuid4length + 1;
        Assert.assertEquals(blobPath.length(), expectedBlobKeyLength);
    }
    
    @Test
    public void testListBlobCommonPrefixes() throws Exception {
        blobClient.pushStream(TEST_CORE_NAME_1, new ByteArrayInputStream(EMPTY_BYTES_ARR), EMPTY_BYTES_ARR.length);
        blobClient.pushStream(TEST_CORE_NAME_1, new ByteArrayInputStream(EMPTY_BYTES_ARR), EMPTY_BYTES_ARR.length);
        blobClient.pushStream(TEST_CORE_NAME_2, new ByteArrayInputStream(EMPTY_BYTES_ARR), EMPTY_BYTES_ARR.length);
        blobClient.pushStream(TEST_CORE_NAME_2, new ByteArrayInputStream(EMPTY_BYTES_ARR), EMPTY_BYTES_ARR.length);
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
            byte bytesWritten[] = {0, -1, 5, 10, 32, 127, -15, 20, 0, -100, 40, 0, 0, 0, (byte) System.currentTimeMillis()};

            FileUtils.writeByteArrayToFile(local, bytesWritten);

            String blobPath;
            blobPath = blobClient.pushStream(TEST_CORE_NAME_1, new ByteArrayInputStream(bytesWritten), bytesWritten.length);

            // Now pull the blob file into the pulled local file
            Files.copy(blobClient.pullStream(blobPath), Paths.get(pulled.getAbsolutePath()), StandardCopyOption.REPLACE_EXISTING);

            // Verify pulled and local are the same
            byte[] bytesLocal = Files.readAllBytes(local.toPath());
            byte[] bytesPulled = Files.readAllBytes(pulled.toPath());

            Assert.assertArrayEquals("Content pulled from blob should be identical to value previously pushed", bytesLocal, bytesPulled);

            Assert.assertEquals("Pulled content expected same size as content initially written", bytesPulled.length, bytesWritten.length);
        } finally {
            local.delete();
            pulled.delete();
        }
    }

    /**
     * Test we can delete a blob (file)
     */
    @Test
    public void testDeleteBlob() throws Exception {
        byte bytesWritten[] = {0, 1, 2, 5};

        // Create a blob
        String blobPath = blobClient.pushStream(TEST_CORE_NAME_1, new ByteArrayInputStream(bytesWritten), bytesWritten.length);

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
        String blobPath1 = blobClient.pushStream(TEST_CORE_NAME_1, new ByteArrayInputStream(bytesWritten), bytesWritten.length);
        String blobPath2 = blobClient.pushStream(TEST_CORE_NAME_1, new ByteArrayInputStream(bytesWritten), bytesWritten.length);
        String blobPath3 = blobClient.pushStream(TEST_CORE_NAME_1, new ByteArrayInputStream(bytesWritten), bytesWritten.length);
        
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
     */
    @Test
    public void testPushPullCoreMetadata() throws Exception {
        blobClient.deleteCore(TEST_CORE_NAME_1);
        BlobCoreMetadata pushedBcm = new BlobCoreMetadataBuilder(TEST_CORE_NAME_1, 19L, 68L).build();
        Assert.assertNull(blobClient.pullCoreMetadata(TEST_CORE_NAME_1));
        
        blobClient.pushCoreMetadata(TEST_CORE_NAME_1, pushedBcm);
        Assert.assertTrue(blobClient.coreMetadataExists(TEST_CORE_NAME_1));
        BlobCoreMetadata pulledBcm = blobClient.pullCoreMetadata(TEST_CORE_NAME_1);
        
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
