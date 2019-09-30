package org.apache.solr.store.blob.metadata;

import org.junit.Assert;
import org.junit.Test;

import org.apache.solr.store.blob.client.BlobCoreMetadata;
import org.apache.solr.store.blob.client.BlobCoreMetadataBuilder;

/**
 * Unit tests for {@link BlobCoreMetadata} and its builder {@link BlobCoreMetadataBuilder}.
 * See some more related tests in {@link ToFromJsonTest}.
 */
public class BlobCoreMetadataTest extends Assert {

    final String SHARED_BLOB_NAME = "collectionName_shardNameTest";
    final long GENERATION_NUMBER = 456;
    final long FILE_SIZE = 123000;
    final long CHECKSUM = 100;

    @Test
    public void buildCoreMetadataNoFiles() throws Exception {
        BlobCoreMetadata bcm = new BlobCoreMetadataBuilder(SHARED_BLOB_NAME, GENERATION_NUMBER).build();

        assertEquals("Blob metadata without any files should not have any files", 0, bcm.getBlobFiles().length);
        assertEquals("Blob metadata should have specified shared blob name", SHARED_BLOB_NAME, bcm.getSharedBlobName());
        assertEquals("Blob metadata should have specified generation", GENERATION_NUMBER, bcm.getGeneration());
    }

    @Test
    public void buildCoreMetadataWithFile() throws Exception {
        BlobCoreMetadata bcm = new BlobCoreMetadataBuilder(SHARED_BLOB_NAME, GENERATION_NUMBER)
            .addFile(new BlobCoreMetadata.BlobFile("solrFilename", "blobFilename", FILE_SIZE, CHECKSUM)).build();

        assertEquals("Blob metadata should have specified shared blob name", SHARED_BLOB_NAME, bcm.getSharedBlobName());
        assertEquals("Blob metadata should have specified generation", GENERATION_NUMBER, bcm.getGeneration());
        assertEquals("Blob metadata should have the correct number of added files", 1, bcm.getBlobFiles().length);
        assertEquals("Blob metadata file should have correct solr filename", "solrFilename", bcm.getBlobFiles()[0].getSolrFileName());
        assertEquals("Blob metadata file should have correct blob store filename", "blobFilename", bcm.getBlobFiles()[0].getBlobName());
        assertEquals("Blob metadata file should have correct file size", FILE_SIZE, bcm.getBlobFiles()[0].getFileSize());
        assertEquals("Blob metadata file should have correct checksum", CHECKSUM, bcm.getBlobFiles()[0].getChecksum());
    }
}
