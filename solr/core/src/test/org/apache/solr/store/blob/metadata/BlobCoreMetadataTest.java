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

    @Test
    public void buildCoreMetadataNoFiles() throws Exception {
        BlobCoreMetadata bcm = new BlobCoreMetadataBuilder(SHARED_BLOB_NAME).build();

        assertEquals("Blob metadata without any files should not have any files", 0, bcm.getBlobFiles().length);
        assertEquals("Blob metadata should have specified shared blob name", SHARED_BLOB_NAME, bcm.getSharedBlobName());
    }

    @Test
    public void buildCoreMetadataWithFile() throws Exception {
        BlobCoreMetadata bcm = new BlobCoreMetadataBuilder(SHARED_BLOB_NAME)
            .addFile(new BlobCoreMetadata.BlobFile("solrFilename", "blobFilename", 123000L)).build();

        assertEquals("Blob metadata should have specified shared blob name", SHARED_BLOB_NAME, bcm.getSharedBlobName());
        assertEquals("Blob metadata should have the correct number of added files", 1, bcm.getBlobFiles().length);
        assertEquals("Blob metadata file should have correct solr filename", "solrFilename", bcm.getBlobFiles()[0].getSolrFileName());
        assertEquals("Blob metadata file should have correct blob store filename", "blobFilename", bcm.getBlobFiles()[0].getBlobName());
    }
}
