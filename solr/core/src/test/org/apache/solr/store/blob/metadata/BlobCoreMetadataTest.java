package org.apache.solr.store.blob.metadata;

import org.junit.Assert;
import org.junit.Test;

import org.apache.solr.store.blob.client.BlobCoreMetadata;
import org.apache.solr.store.blob.client.BlobCoreMetadataBuilder;

/**
 * Unit tests for {@link BlobCoreMetadata} and its builder {@link BlobCoreMetadataBuilder}.
 * See some more related tests in {@link ToFromJsonTest}.
 *
 * @author iginzburg
 * @since 214/solr.6
 */
public class BlobCoreMetadataTest extends Assert {

    final String CORE_NAME = "myCoreNameBlobTest";

    @Test
    public void buildCoreMetadataNoFiles() throws Exception {
        BlobCoreMetadata bcm = new BlobCoreMetadataBuilder(CORE_NAME, 456L).build();

        assertEquals("Blob metadata without any files should not have any files", 0, bcm.getBlobFiles().length);
        assertEquals("Blob metadata should have specified core name", CORE_NAME, bcm.getSharedBlobName());
        assertEquals("Blob metadata should have specified generation", 456L, bcm.getGeneration());
    }

    @Test
    public void buildCoreMetadataWithFile() throws Exception {
        BlobCoreMetadata bcm = new BlobCoreMetadataBuilder(CORE_NAME, 456L).addFile(new BlobCoreMetadata.BlobFile("solrFilename", "blobFilename", 123000L)).build();

        assertEquals("Blob metadata should have specified core name", CORE_NAME, bcm.getSharedBlobName());
        assertEquals("Blob metadata should have specified generation", 456L, bcm.getGeneration());
        assertEquals("Blob metadata should have the correct number of added files", 1, bcm.getBlobFiles().length);
        assertEquals("Blob metadata file should have correct solr filename", "solrFilename", bcm.getBlobFiles()[0].getSolrFileName());
        assertEquals("Blob metadata file should have correct blob store filename", "blobFilename", bcm.getBlobFiles()[0].getBlobName());
    }
}
