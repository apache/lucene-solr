package org.apache.solr.store.blob.metadata;

import com.google.common.collect.*;

import org.apache.solr.store.blob.client.BlobCoreMetadata;
import org.apache.solr.store.blob.client.BlobCoreMetadata.BlobConfigFile;
import org.apache.solr.store.blob.client.BlobCoreMetadata.BlobFile;
import org.apache.solr.store.blob.client.BlobCoreMetadataBuilder;

import org.junit.Test;
import org.apache.solr.store.blob.metadata.MetadataResolver.Action;
import org.apache.solr.store.blob.metadata.ServerSideCoreMetadata.CoreConfigFileData;
import org.apache.solr.store.blob.metadata.ServerSideCoreMetadata.CoreFileData;
import org.apache.solr.store.blob.util.BlobTestUtil;

import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link MetadataResolver}.
 *
 * @author mwaheed
 * @since 218/solr.6
 */
public class MetadataResolverTest {

    private static final String CORE_NAME = BlobTestUtil.getValidSolrCoreName();
    private static final Collection EXCEPTION_EXPECTED_INSTEAD_OF_COLLECTION = null;

    /**
     * Tests that passing both local and blob as null to {@link MetadataResolver} throws exception.
     */
    @Test
    public void testBothLocalAndBlobAreNull() throws Exception {
        try {
            new MetadataResolver(null, null);
            fail("MetadataResolver did not throw IllegalStateException");
        } catch (IllegalStateException ex) {

        }
    }

    /**
     * Tests that re-resolving an instance of {@link MetadataResolver} throws exception.
     */
    @Test
    public void testReResolving() throws Exception {
        final BlobCoreMetadata blobMetadata = BlobCoreMetadataBuilder.buildEmptyCoreMetadata(CORE_NAME);
        final MetadataResolver resolver = new MetadataResolver(null, blobMetadata);
        try {
            resolver.resolve(null, blobMetadata);
            fail("Re-resolving did not throw IllegalStateException");
        } catch (IllegalStateException ex) {

        }
    }

    /**
     * Tests that a null blob core resolves into {@link MetadataResolver.Action#PUSH}.
     */
    @Test
    public void testPushNeededWhenBlobNull() throws Exception {
        final long localSequenceNumber = 1;
        final long localGenerationNumber = 1;
        final long localFileSize = 10;
        // local core metadata
        final ServerSideCoreMetadata serverMetadata = new ServerSideCoreMetadataBuilder(CORE_NAME, localSequenceNumber, localGenerationNumber)
                .addFile(new CoreFileData(getSolrSegmentFileName(localGenerationNumber), localFileSize))
                .build();
        // expected resolution
        final Action expectedAction = Action.PUSH;
        final Collection<CoreFileData> expectedFilesToPush = serverMetadata.getFiles();
        final Collection<BlobFile> expectedFilesToPull = EXCEPTION_EXPECTED_INSTEAD_OF_COLLECTION;
        final Collection<CoreConfigFileData> expectedConfigFilesToPush = Collections.emptySet();
        final Collection<BlobConfigFile> expectedConfigFilesToPull = Collections.emptySet();
        final Collection<BlobFile> expectedFilesToDelete = Collections.emptySet();
        final String expectedMessage = null;

        // resolve
        final MetadataResolver resolver = new MetadataResolver(serverMetadata, null);

        // assert
        assertMetadataResolver(expectedAction, expectedFilesToPush, expectedFilesToPull, expectedConfigFilesToPush,
                expectedConfigFilesToPull, expectedFilesToDelete, expectedMessage, resolver);
    }

    /**
     * Tests that a local core ahead of blob resolves into {@link Action#PUSH}.
     */
    @Test
    public void testPushNeededWhenLocalAheadOfBlob() throws Exception {
        final long localSequenceNumber = 2;
        final long localGenerationNumber = 2;
        final long localFileSize = 20;
        final long blobSequenceNumber = 1;
        final long blobGenerationNumber = 1;
        final long blobFileSize = 10;
        // local core metadata ahead of blob
        final ServerSideCoreMetadata serverMetadata = new ServerSideCoreMetadataBuilder(CORE_NAME, localSequenceNumber, localGenerationNumber)
                .addFile(new CoreFileData(getSolrSegmentFileName(localGenerationNumber), localFileSize))
                .build();
        // blob core metadata
        final BlobCoreMetadata blobMetadata = new BlobCoreMetadataBuilder(CORE_NAME, blobSequenceNumber, blobGenerationNumber)
                .addFile(new BlobFile(getSolrSegmentFileName(blobGenerationNumber), getBlobFileName(blobGenerationNumber), blobFileSize))
                .build();
        // expected resolution
        final Action expectedAction = Action.PUSH;
        final Collection<CoreFileData> expectedFilesToPush = serverMetadata.getFiles();
        final Collection<BlobFile> expectedFilesToPull = EXCEPTION_EXPECTED_INSTEAD_OF_COLLECTION;
        final Collection<CoreConfigFileData> expectedConfigFilesToPush = Collections.emptySet();
        final Collection<BlobConfigFile> expectedConfigFilesToPull = Collections.emptySet();
        final Collection<BlobFile> expectedFilesToDelete = Arrays.asList(blobMetadata.getBlobFiles());
        final String expectedMessage = null;

        // resolve
        final MetadataResolver resolver = new MetadataResolver(serverMetadata, blobMetadata);

        // assert
        assertMetadataResolver(expectedAction, expectedFilesToPush, expectedFilesToPull, expectedConfigFilesToPush,
                expectedConfigFilesToPull, expectedFilesToDelete, expectedMessage, resolver);
    }

    /**
     * Tests that a local core ahead of blob just by generation number resolves into {@link Action#PUSH}.
     */
    @Test
    public void testPushNeededWhenLocalAheadOfBlobJustByGenerationNumber() throws Exception {
        final long localSequenceNumber = 1;
        final long localGenerationNumber = 2;
        final long localFileSize = 20;
        final long blobSequenceNumber = 1;
        final long blobGenerationNumber = 1;
        final long blobFileSize = 10;
        // local core metadata ahead of blob just by generation number
        final ServerSideCoreMetadata serverMetadata = new ServerSideCoreMetadataBuilder(CORE_NAME, localSequenceNumber, localGenerationNumber)
                .addFile(new CoreFileData(getSolrSegmentFileName(localGenerationNumber), localFileSize))
                .build();
        // blob core metadata
        final BlobCoreMetadata blobMetadata = new BlobCoreMetadataBuilder(CORE_NAME, blobSequenceNumber, blobGenerationNumber)
                .addFile(new BlobFile(getSolrSegmentFileName(blobGenerationNumber), getBlobFileName(blobGenerationNumber), blobFileSize))
                .build();
        // expected resolution
        final Action expectedAction = Action.PUSH;
        final Collection<CoreFileData> expectedFilesToPush = serverMetadata.getFiles();
        final Collection<BlobFile> expectedFilesToPull = EXCEPTION_EXPECTED_INSTEAD_OF_COLLECTION;
        final Collection<CoreConfigFileData> expectedConfigFilesToPush = Collections.emptySet();
        final Collection<BlobConfigFile> expectedConfigFilesToPull = Collections.emptySet();
        final Collection<BlobFile> expectedFilesToDelete = Arrays.asList(blobMetadata.getBlobFiles());
        final String expectedMessage = null;

        // resolve
        final MetadataResolver resolver = new MetadataResolver(serverMetadata, blobMetadata);

        // assert
        assertMetadataResolver(expectedAction, expectedFilesToPush, expectedFilesToPull, expectedConfigFilesToPush,
                expectedConfigFilesToPull, expectedFilesToDelete, expectedMessage, resolver);
    }

    /**
     * Tests that a null local core resolves into {@link Action#PULL}.
     */
    @Test
    public void testPullNeededWhenLocalNull() throws Exception {
        final long blobSequenceNumber = 1;
        final long blobGenerationNumber = 1;
        final long bloblFileSize = 10;
        // blob core metadata
        final BlobCoreMetadata blobMetadata = new BlobCoreMetadataBuilder(CORE_NAME, blobSequenceNumber, blobGenerationNumber)
                .addFile(new BlobFile(getSolrSegmentFileName(blobSequenceNumber), getBlobFileName(blobGenerationNumber), bloblFileSize))
                .build();
        // expected resolution
        final Action expectedAction = Action.PULL;
        final Collection<CoreFileData> expectedFilesToPush = EXCEPTION_EXPECTED_INSTEAD_OF_COLLECTION;
        final Collection<BlobFile> expectedFilesToPull = Arrays.asList(blobMetadata.getBlobFiles());
        final Collection<CoreConfigFileData> expectedConfigFilesToPush = Collections.emptySet();
        final Collection<BlobConfigFile> expectedConfigFilesToPull = Collections.emptySet();
        final Collection<BlobFile> expectedFilesToDelete = EXCEPTION_EXPECTED_INSTEAD_OF_COLLECTION;
        final String expectedMessage = null;

        // resolve
        final MetadataResolver resolver = new MetadataResolver(null, blobMetadata);

        // assert
        assertMetadataResolver(expectedAction, expectedFilesToPush, expectedFilesToPull, expectedConfigFilesToPush,
                expectedConfigFilesToPull, expectedFilesToDelete, expectedMessage, resolver);

    }

    /**
     * Tests that a blob core ahead of local resolves into {@link Action#PULL}.
     */
    @Test
    public void testPullNeededWhenBlobAheadOfLocal() throws Exception {
        final long localSequenceNumber = 1;
        final long localGenerationNumber = 1;
        final long localFileSize = 10;
        final long blobSequenceNumber = 2;
        final long blobGenerationNumber = 2;
        final long blobFileSize = 20;
        // local core metadata
        final ServerSideCoreMetadata serverMetadata = new ServerSideCoreMetadataBuilder(CORE_NAME, localSequenceNumber, localGenerationNumber)
                .addFile(new CoreFileData(getSolrSegmentFileName(localGenerationNumber), localFileSize))
                .build();
        // blob core metadata ahead of local
        final BlobCoreMetadata blobMetadata = new BlobCoreMetadataBuilder(CORE_NAME, blobSequenceNumber, blobGenerationNumber)
                .addFile(new BlobFile(getSolrSegmentFileName(blobGenerationNumber), getBlobFileName(blobGenerationNumber), blobFileSize))
                .build();
        // expected resolution
        final Action expectedAction = Action.PULL;
        final Collection<CoreFileData> expectedFilesToPush = EXCEPTION_EXPECTED_INSTEAD_OF_COLLECTION;
        final Collection<BlobFile> expectedFilesToPull = Arrays.asList(blobMetadata.getBlobFiles());
        final Collection<CoreConfigFileData> expectedConfigFilesToPush = Collections.emptySet();
        final Collection<BlobConfigFile> expectedConfigFilesToPull = Collections.emptySet();
        final Collection<BlobFile> expectedFilesToDelete = EXCEPTION_EXPECTED_INSTEAD_OF_COLLECTION;
        final String expectedMessage = null;

        // resolve
        MetadataResolver resolver = new MetadataResolver(serverMetadata, blobMetadata);

        // assert
        assertMetadataResolver(expectedAction, expectedFilesToPush, expectedFilesToPull, expectedConfigFilesToPush,
                expectedConfigFilesToPull, expectedFilesToDelete, expectedMessage, resolver);
    }

    /**
     * Tests that a blob core ahead of local just by generation number resolves into {@link Action#PULL}.
     */
    @Test
    public void testPullNeededWhenBlobAheadOfLocalJustByGenerationNumber() throws Exception {
        final long localSequenceNumber = 1;
        final long localGenerationNumber = 1;
        final long localFileSize = 10;
        final long blobSequenceNumber = 1;
        final long blobGenerationNumber = 2;
        final long blobFileSize = 20;
        // local core metadata
        final ServerSideCoreMetadata serverMetadata = new ServerSideCoreMetadataBuilder(CORE_NAME, localSequenceNumber, localGenerationNumber)
                .addFile(new CoreFileData(getSolrSegmentFileName(localGenerationNumber), localFileSize))
                .build();
        // blob core metadata ahead of local just by generation number
        final BlobCoreMetadata blobMetadata = new BlobCoreMetadataBuilder(CORE_NAME, blobSequenceNumber, blobGenerationNumber)
                .addFile(new BlobFile(getSolrSegmentFileName(blobGenerationNumber), getBlobFileName(blobGenerationNumber), blobFileSize))
                .build();
        // expected resolution
        final Action expectedAction = Action.PULL;
        final Collection<CoreFileData> expectedFilesToPush = EXCEPTION_EXPECTED_INSTEAD_OF_COLLECTION;
        final Collection<BlobFile> expectedFilesToPull = Arrays.asList(blobMetadata.getBlobFiles());
        final Collection<CoreConfigFileData> expectedConfigFilesToPush = Collections.emptySet();
        final Collection<BlobConfigFile> expectedConfigFilesToPull = Collections.emptySet();
        final Collection<BlobFile> expectedFilesToDelete = EXCEPTION_EXPECTED_INSTEAD_OF_COLLECTION;
        final String expectedMessage = null;

        // resolve
        final MetadataResolver resolver = new MetadataResolver(serverMetadata, blobMetadata);

        // assert
        assertMetadataResolver(expectedAction, expectedFilesToPush, expectedFilesToPull, expectedConfigFilesToPush,
                expectedConfigFilesToPull, expectedFilesToDelete, expectedMessage, resolver);
    }

    /**
     * Tests that identical local and blob cores resolve into {@link Action#EQUIVALENT}.
     */
    @Test
    public void testIdenticalCores() throws Exception {
        final long localSequenceNumber = 1;
        final long localGenerationNumber = 1;
        final long localFileSize = 10;
        final long blobSequenceNumber = 1;
        final long blobGenerationNumber = 1;
        final long blobFileSize = 10;
        // local core metadata
        final ServerSideCoreMetadata serverMetadata = new ServerSideCoreMetadataBuilder(CORE_NAME, localSequenceNumber, localGenerationNumber)
                .addFile(new CoreFileData(getSolrSegmentFileName(localGenerationNumber), localFileSize))
                .build();
        // blob core metadata identical to local core metadata
        final BlobCoreMetadata blobMetadata = new BlobCoreMetadataBuilder(CORE_NAME, blobSequenceNumber, blobGenerationNumber)
                .addFile(new BlobFile(getSolrSegmentFileName(blobGenerationNumber), getBlobFileName(blobGenerationNumber), blobFileSize))
                .build();
        // expected resolution
        final Action expectedAction = Action.EQUIVALENT;
        final Collection<CoreFileData> expectedFilesToPush = EXCEPTION_EXPECTED_INSTEAD_OF_COLLECTION;
        final Collection<BlobFile> expectedFilesToPull = EXCEPTION_EXPECTED_INSTEAD_OF_COLLECTION;
        final Collection<CoreConfigFileData> expectedConfigFilesToPush = EXCEPTION_EXPECTED_INSTEAD_OF_COLLECTION;
        final Collection<BlobConfigFile> expectedConfigFilesToPull = EXCEPTION_EXPECTED_INSTEAD_OF_COLLECTION;
        final Collection<BlobFile> expectedFilesToDelete = EXCEPTION_EXPECTED_INSTEAD_OF_COLLECTION;
        final String expectedMessage = null;

        // resolve
        final MetadataResolver resolver = new MetadataResolver(serverMetadata, blobMetadata);

        // assert
        assertMetadataResolver(expectedAction, expectedFilesToPush, expectedFilesToPull, expectedConfigFilesToPush,
                expectedConfigFilesToPull, expectedFilesToDelete, expectedMessage, resolver);
    }

    /**
     * Tests that blob core marked as deleted resolves into {@link Action#BLOB_DELETED}.
     */
    @Test
    public void testBlobCoreMarkedAsDeleted() throws Exception {
        final long blobSequenceNumber = 1;
        final long blobGenerationNumber = 1;
        final long blobFileSize = 10;
        // blob core metadata marked as deleted
        final BlobCoreMetadata blobMetadata = new BlobCoreMetadataBuilder(CORE_NAME, blobSequenceNumber, blobGenerationNumber)
                .addFile(new BlobFile(getSolrSegmentFileName(blobGenerationNumber), getBlobFileName(blobGenerationNumber), blobFileSize))
                .build()
                .getDeletedOf();
        // expected resolution
        final Action expectedAction = Action.BLOB_DELETED;
        final Collection<CoreFileData> expectedFilesToPush = EXCEPTION_EXPECTED_INSTEAD_OF_COLLECTION;
        final Collection<BlobFile> expectedFilesToPull = EXCEPTION_EXPECTED_INSTEAD_OF_COLLECTION;
        final Collection<CoreConfigFileData> expectedConfigFilesToPush = EXCEPTION_EXPECTED_INSTEAD_OF_COLLECTION;
        final Collection<BlobConfigFile> expectedConfigFilesToPull = EXCEPTION_EXPECTED_INSTEAD_OF_COLLECTION;
        final Collection<BlobFile> expectedFilesToDelete = EXCEPTION_EXPECTED_INSTEAD_OF_COLLECTION;
        final String expectedMessage = null;

        // resolve
        final MetadataResolver resolver = new MetadataResolver(null, blobMetadata);

        // assert
        assertMetadataResolver(expectedAction, expectedFilesToPush, expectedFilesToPull, expectedConfigFilesToPush,
                expectedConfigFilesToPull, expectedFilesToDelete, expectedMessage, resolver);
    }

    /**
     * Tests that blob core marked as corrupt resolves into {@link Action#BLOB_CORRUPT}.
     */
    @Test
    public void testBlobCoreMarkedAsCorrupted() throws Exception {
        final long localSequenceNumber = 1;
        final long localGenerationNumber = 1;
        final long localFileSize = 10;
        final long blobSequenceNumber = 1;
        final long blobGenerationNumber = 1;
        final long blobFileSize = 10;
        // local core metadata
        final ServerSideCoreMetadata serverMetadata = new ServerSideCoreMetadataBuilder(CORE_NAME, localSequenceNumber, localGenerationNumber)
                .addFile(new CoreFileData(getSolrSegmentFileName(localGenerationNumber), localFileSize))
                .build();
        // blob core metadata marked as corrupt
        final BlobCoreMetadata blobMetadata = new BlobCoreMetadataBuilder(CORE_NAME, blobSequenceNumber, blobGenerationNumber)
                .addFile(new BlobFile(getSolrSegmentFileName(blobGenerationNumber), getBlobFileName(blobGenerationNumber), blobFileSize))
                .build()
                .getCorruptOf();
        // expected resolution
        final Action expectedAction = Action.BLOB_CORRUPT;
        final Collection<CoreFileData> expectedFilesToPush = serverMetadata.getFiles();
        final Collection<BlobFile> expectedFilesToPull = EXCEPTION_EXPECTED_INSTEAD_OF_COLLECTION;
        final Collection<CoreConfigFileData> expectedConfigFilesToPush = Collections.emptySet();
        final Collection<BlobConfigFile> expectedConfigFilesToPull = EXCEPTION_EXPECTED_INSTEAD_OF_COLLECTION;
        final Collection<BlobFile> expectedFilesToDelete = EXCEPTION_EXPECTED_INSTEAD_OF_COLLECTION;
        final String expectedMessage = null;

        // resolve
        final MetadataResolver resolver = new MetadataResolver(serverMetadata, blobMetadata);

        // assert
        assertMetadataResolver(expectedAction, expectedFilesToPush, expectedFilesToPull, expectedConfigFilesToPush,
                expectedConfigFilesToPull, expectedFilesToDelete, expectedMessage, resolver);
    }

    /**
     * Tests that conflicting file sizes between local and blob cores resolve into {@link Action#CONFLICT}.
     */
    @Test
    public void testConflictingFileSizes() throws Exception {
        final long localSequenceNumber = 1;
        final long localGenerationNumber = 1;
        final long localFileSize = 10;
        final long blobSequenceNumber = 1;
        final long blobGenerationNumber = 1;
        final long blobFileSize = 100;
        // local core metadata
        final ServerSideCoreMetadata serverMetadata = new ServerSideCoreMetadataBuilder(CORE_NAME, localSequenceNumber, localGenerationNumber)
                .addFile(new CoreFileData(getSolrSegmentFileName(localGenerationNumber), localFileSize))
                .build();
        // blob core metadata identical to local core metadata but different file size
        final BlobCoreMetadata blobMetadata = new BlobCoreMetadataBuilder(CORE_NAME, blobSequenceNumber, blobGenerationNumber)
                .addFile(new BlobFile(getSolrSegmentFileName(blobGenerationNumber), getBlobFileName(blobGenerationNumber), blobFileSize))
                .build();        
        // expected resolution
        final Action expectedAction = Action.CONFLICT;
        final Collection<CoreFileData> expectedFilesToPush = EXCEPTION_EXPECTED_INSTEAD_OF_COLLECTION;
        final Collection<BlobFile> expectedFilesToPull = EXCEPTION_EXPECTED_INSTEAD_OF_COLLECTION;
        final Collection<CoreConfigFileData> expectedConfigFilesToPush = EXCEPTION_EXPECTED_INSTEAD_OF_COLLECTION;
        final Collection<BlobConfigFile> expectedConfigFilesToPull = EXCEPTION_EXPECTED_INSTEAD_OF_COLLECTION;
        final Collection<BlobFile> expectedFilesToDelete = EXCEPTION_EXPECTED_INSTEAD_OF_COLLECTION;
        final String expectedMessage = String.format("Size conflict. File %s (Blob name %s) local size %s blob size %s (core %s)",
                getSolrSegmentFileName(localGenerationNumber), getBlobFileName(blobGenerationNumber), localFileSize, blobFileSize, CORE_NAME);

        // resolve
        final MetadataResolver resolver = new MetadataResolver(serverMetadata, blobMetadata);

        // assert
        assertMetadataResolver(expectedAction, expectedFilesToPush, expectedFilesToPull, expectedConfigFilesToPush,
                expectedConfigFilesToPull, expectedFilesToDelete, expectedMessage, resolver);
    }

    /**
     * Tests that blob core containing conflicting segment files resolves into {@link Action#BLOB_CORRUPT}.
     */
    @Test
    public void testBlobCoreWithConflictingSegmentFiles() throws Exception {
        final long localSequenceNumber = 1;
        final long localGenerationNumber = 1;
        final long localFileSize = 10;
        final long blobSequenceNumber = 2;
        final long blobGenerationNumber = 2;
        final long blobFileSize = 10;
        final long conflictingBlobGenerationNumber = 1;
        // local core metadata
        final ServerSideCoreMetadata serverMetadata = new ServerSideCoreMetadataBuilder(CORE_NAME, localSequenceNumber, localGenerationNumber)
                .addFile(new CoreFileData(getSolrSegmentFileName(localGenerationNumber), localFileSize))
                .build();
        // blob core metadata with conflicting segment files
        final BlobCoreMetadata blobMetadata = new BlobCoreMetadataBuilder(CORE_NAME, blobSequenceNumber, blobGenerationNumber)
                .addFile(new BlobFile(getSolrSegmentFileName(conflictingBlobGenerationNumber), getBlobFileName(conflictingBlobGenerationNumber), blobFileSize))
                .addFile(new BlobFile(getSolrSegmentFileName(blobGenerationNumber), getBlobFileName(blobGenerationNumber), blobFileSize))
                .build();
        // expected resolution
        final Action expectedAction = Action.BLOB_CORRUPT;
        final Collection<CoreFileData> expectedFilesToPush = serverMetadata.getFiles();
        final Collection<BlobFile> expectedFilesToPull = EXCEPTION_EXPECTED_INSTEAD_OF_COLLECTION;
        final Collection<CoreConfigFileData> expectedConfigFilesToPush = Collections.emptySet();
        final Collection<BlobConfigFile> expectedConfigFilesToPull = EXCEPTION_EXPECTED_INSTEAD_OF_COLLECTION;
        final Collection<BlobFile> expectedFilesToDelete = EXCEPTION_EXPECTED_INSTEAD_OF_COLLECTION;
        final String expectedMessage = String.format("Blob store for core %s has conflicting files %s and %s",
                CORE_NAME, getSolrSegmentFileName(conflictingBlobGenerationNumber), getSolrSegmentFileName(blobGenerationNumber));

        // resolve
        final MetadataResolver resolver = new MetadataResolver(serverMetadata, blobMetadata);

        // assert
        assertMetadataResolver(expectedAction, expectedFilesToPush, expectedFilesToPull, expectedConfigFilesToPush,
                expectedConfigFilesToPull, expectedFilesToDelete, expectedMessage, resolver);
    }

    /**
     * Tests that blob core missing segment file resolves into {@link Action#BLOB_CORRUPT}.
     */
    @Test
    public void testBlobCoreWithMissingSegmentFile() throws Exception {
        final long blobSequenceNumber = 1;
        final long blobGenerationNumber = 1;
        // blob core metadata with missing segment file
        final BlobCoreMetadata blobMetadata = new BlobCoreMetadataBuilder(CORE_NAME, blobSequenceNumber, blobGenerationNumber).build();
        // expected resolution
        final Action expectedAction = Action.BLOB_CORRUPT;
        final Collection<CoreFileData> expectedFilesToPush = Collections.emptySet();
        final Collection<BlobFile> expectedFilesToPull = EXCEPTION_EXPECTED_INSTEAD_OF_COLLECTION;
        final Collection<CoreConfigFileData> expectedConfigFilesToPush = Collections.emptySet();
        final Collection<BlobConfigFile> expectedConfigFilesToPull = EXCEPTION_EXPECTED_INSTEAD_OF_COLLECTION;
        final Collection<BlobFile> expectedFilesToDelete = EXCEPTION_EXPECTED_INSTEAD_OF_COLLECTION;
        final String expectedMessage = String.format("Blob store for core %s does not contain a segments_N file", CORE_NAME);

        // resolve
        final MetadataResolver resolver = new MetadataResolver(null, blobMetadata);

        // assert
        assertMetadataResolver(expectedAction, expectedFilesToPush, expectedFilesToPull, expectedConfigFilesToPush,
                expectedConfigFilesToPull, expectedFilesToDelete, expectedMessage, resolver);
    }

    /**
     * Tests cores with config only changes resolve into {@link Action#CONFIG_CHANGE}.
     */
    @Test
    public void testConfigOnlyChange() throws Exception {
        final long localSequenceNumber = 1;
        final long localGenerationNumber = 1;
        final long localFileSize = 10;
        final long blobSequenceNumber = 1;
        final long blobGenerationNumber = 1;
        final long blobFileSize = 10;
        final String configMissingOnBlobName = "configMissingOnBlob.txt";
        final String configBehindOnBlobName = "configBehindOnBlob.txt";
        final String configMissingOnLocalName = "configMissingOnLocal.txt";
        final String configBehindOnLocalName = "configBehindOnLocal.txt";
        final CoreConfigFileData localConfigMissingOnBlob = new CoreConfigFileData(configMissingOnBlobName, 10, 1);
        final CoreConfigFileData localConfigBehindOnBlob = new CoreConfigFileData(configBehindOnBlobName, 10, 2);
        final CoreConfigFileData localConfigBehindOnLocal = new CoreConfigFileData(configBehindOnLocalName, 10, 1);
        final BlobConfigFile blobConfigMissingOnLocal = new BlobConfigFile(configMissingOnLocalName, configMissingOnLocalName,10, 1);
        final BlobConfigFile blobConfigBehindOnBlob = new BlobConfigFile(configBehindOnBlobName, configBehindOnBlobName,10, 1);
        final BlobConfigFile blobConfigBehindOnLocal = new BlobConfigFile(configBehindOnLocalName, configBehindOnLocalName,10, 2);
        // local core metadata
        final ServerSideCoreMetadata serverMetadata = new ServerSideCoreMetadataBuilder(CORE_NAME, localSequenceNumber, localGenerationNumber)
                .addFile(new CoreFileData(getSolrSegmentFileName(localGenerationNumber), localFileSize))
                .addConfigFile(localConfigMissingOnBlob)
                .addConfigFile(localConfigBehindOnBlob)
                .addConfigFile(localConfigBehindOnLocal)
                .build();
        // blob core metadata identical to local core metadata
        final BlobCoreMetadata blobMetadata = new BlobCoreMetadataBuilder(CORE_NAME, blobSequenceNumber, blobGenerationNumber)
                .addFile(new BlobFile(getSolrSegmentFileName(blobGenerationNumber), getBlobFileName(blobGenerationNumber), blobFileSize))
                .addConfigFile(blobConfigMissingOnLocal)
                .addConfigFile(blobConfigBehindOnBlob)
                .addConfigFile(blobConfigBehindOnLocal)
                .build();
        // expected resolution
        final Action expectedAction = Action.CONFIG_CHANGE;
        final Collection<CoreFileData> expectedFilesToPush = EXCEPTION_EXPECTED_INSTEAD_OF_COLLECTION;
        final Collection<BlobFile> expectedFilesToPull = EXCEPTION_EXPECTED_INSTEAD_OF_COLLECTION;
        final Collection<CoreConfigFileData> expectedConfigFilesToPush = Lists.newArrayList(localConfigMissingOnBlob, localConfigBehindOnBlob);
        final Collection<BlobConfigFile> expectedConfigFilesToPull = Arrays.asList(blobConfigMissingOnLocal, blobConfigBehindOnLocal);
        final Collection<BlobFile> expectedFilesToDelete = EXCEPTION_EXPECTED_INSTEAD_OF_COLLECTION;
        final String expectedMessage = null;

        // resolve
        final MetadataResolver resolver = new MetadataResolver(serverMetadata, blobMetadata);

        // assert
        assertMetadataResolver(expectedAction, expectedFilesToPush, expectedFilesToPull, expectedConfigFilesToPush,
                expectedConfigFilesToPull, expectedFilesToDelete, expectedMessage, resolver);
    }

    /**
     * Creates a segment file name.
     */
    private String getSolrSegmentFileName(long suffix){
        return "segments_" + suffix;
    }

    /**
     * Creates a dummy blob file name.
     */
    private String getBlobFileName(long suffix){
        return "blobFile_" + suffix;
    }

    /**
     * Asserts {@code actual} resolver against expected values. 
     */
    private static void assertMetadataResolver(Action expectedAction, Collection<CoreFileData> expectedFilesToPush, 
                                               Collection<BlobFile> expectedFilesToPull,  Collection<CoreConfigFileData> expectedConfigFilesToPush,
                                               Collection<BlobConfigFile> expectedConfigFilesToPull, Collection<BlobFile> expectedFilesToDelete, 
                                               String expectedMessage,  MetadataResolver actual) throws Exception {
        assertEquals("Wrong action", expectedAction, actual.getAction());
        assertEquals("Wrong message", expectedMessage, actual.getMessage());

        // assert files to push
        if (expectedFilesToPush != null) {
            assertCollections("Push files", expectedFilesToPush, actual.getFilesToPush());
        } else {
            try {
                actual.getFilesToPush();
                fail("getFilesToPush did not throw IllegalStateException");
            } catch (IllegalStateException ex) {

            }
        }
        // assert files to pull
        if (expectedFilesToPull != null) {
            assertCollections("Pull files", expectedFilesToPull, actual.getFilesToPull().values());
        } else {
            try {
                actual.getFilesToPull();
                fail("getFilesToPull did not throw IllegalStateException");
            } catch (IllegalStateException ex) {

            }
        }
        // assert config files to push
        if (expectedConfigFilesToPush != null) {
            assertCollections("Push config files", expectedConfigFilesToPush, actual.getConfigFilesToPush());
        } else {
            try {
                actual.getConfigFilesToPush();
                fail("getConfigFilesToPush did not throw IllegalStateException");
            } catch (IllegalStateException ex) {

            }
        }
        // assert config files to pull
        if (expectedConfigFilesToPull != null) {
            assertCollections("Pull config files", expectedConfigFilesToPull, actual.getConfigFilesToPull().values());
        } else {
            try {
                actual.getConfigFilesToPull();
                fail("getConfigFilesToPull did not throw IllegalStateException");
            } catch (IllegalStateException ex) {

            }
        }
        // assert files to delete
        if (expectedFilesToDelete != null) {
            assertCollections("Delete files", expectedFilesToDelete, actual.getBlobFilesToDelete());
        } else {
            try {
                actual.getBlobFilesToDelete();
                fail("getBlobFilesToDelete did not throw IllegalStateException");
            } catch (IllegalStateException ex) {

            }
        }
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
     * A builder for {@link ServerSideCoreMetadata} that uses mock.
     */
    private static class ServerSideCoreMetadataBuilder {
        final private String coreName;
        final private long sequenceNumber;
        final private long generation;
        final private ImmutableSet.Builder<CoreFileData> files;
        final private ImmutableList.Builder<CoreConfigFileData> configFiles;

        ServerSideCoreMetadataBuilder(String coreName, long sequenceNumber, long generation){
            this.coreName = coreName;
            this.sequenceNumber = sequenceNumber;
            this.generation = generation;
            files = new ImmutableSet.Builder();
            configFiles = new ImmutableList.Builder<>();
        }

        ServerSideCoreMetadataBuilder addFile(CoreFileData file) {
            this.files.add(file);
            return this;
        }

        ServerSideCoreMetadataBuilder addConfigFile(CoreConfigFileData file) {
            this.configFiles.add(file);
            return this;
        }

        public ServerSideCoreMetadata build() {
            ServerSideCoreMetadata serverMetadata = mock(ServerSideCoreMetadata.class);
            when(serverMetadata.getCoreName()).thenReturn(coreName);
            when(serverMetadata.getGeneration()).thenReturn(sequenceNumber);
            when(serverMetadata.getSequenceNumber()).thenReturn(generation);
            when(serverMetadata.getFiles()).thenReturn(files.build());
            when(serverMetadata.getConfigFiles()).thenReturn(configFiles.build());

            return serverMetadata;
        }
    }
}
