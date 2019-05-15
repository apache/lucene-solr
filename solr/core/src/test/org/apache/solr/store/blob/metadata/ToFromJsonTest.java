package org.apache.solr.store.blob.metadata;

import org.junit.Assert;
import org.junit.Test;

import org.apache.solr.store.blob.client.BlobCoreMetadata;
import org.apache.solr.store.blob.client.BlobCoreMetadataBuilder;
import org.apache.solr.store.blob.client.ToFromJson;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Unit tests for {@link ToFromJson}. Is not in the same package for access to {@link BlobCoreMetadata}.
 *
 * @author iginzburg
 * @since 214/solr.6
 */
public class ToFromJsonTest extends Assert {
    final String CORE_NAME = "myCoreNameJsonTest";

    @Test
    public void jsonCoreMetadataNoFiles() throws Exception {
        BlobCoreMetadata bcm = new BlobCoreMetadataBuilder(CORE_NAME, 666L, 777L).build();

        verifyToJsonAndBack(bcm);
    }

    @Test
    public void jsonCoreMetadataFile() throws Exception {
        BlobCoreMetadataBuilder bb = new BlobCoreMetadataBuilder(CORE_NAME, 777L, 888L);

        BlobCoreMetadata bcm = bb.addFile(new BlobCoreMetadata.BlobFile("solrFilename", "blobFilename", 123000L)).build();

        verifyToJsonAndBack(bcm);
    }

    @Test
    public void jsonCoreMetadataMultiFiles() throws Exception {
        BlobCoreMetadataBuilder bb = new BlobCoreMetadataBuilder(CORE_NAME, 123L, 567L);
        Set<BlobCoreMetadata.BlobFile> files = new HashSet<>(Arrays.asList(
                new BlobCoreMetadata.BlobFile("solrFilename11", "blobFilename11", 1234L),
                new BlobCoreMetadata.BlobFile("solrFilename21", "blobFilename21", 2345L),
                new BlobCoreMetadata.BlobFile("solrFilename31", "blobFilename31", 3456L),
                new BlobCoreMetadata.BlobFile("solrFilename41", "blobFilename41", 4567L)
        ));
        for (BlobCoreMetadata.BlobFile f : files) {
            bb.addFile(f);
        }

        // Note the builder does not necessarily keep the added files in order.
        BlobCoreMetadata bcm = bb.build();

        verifyToJsonAndBack(bcm);

        assertEquals("blob core metadata should have core name specified to builder", CORE_NAME, bcm.getCoreName());
        assertEquals("blob core metadata should have sequence number specified to builder",123L, bcm.getSequenceNumber());
        assertEquals("blob core metadata should have generation specified to builder",567L, bcm.getGeneration());

        // Files are not necessarily in the same order
        assertEquals("blob core metadata should have file set specified to builder", files, new HashSet<>(Arrays.asList(bcm.getBlobFiles())));
    }


    private void verifyToJsonAndBack(BlobCoreMetadata b) throws Exception {
        ToFromJson<BlobCoreMetadata> converter = new ToFromJson<>();
        String json = converter.toJson(b);
        BlobCoreMetadata b2 = converter.fromJson(json, BlobCoreMetadata.class);
        assertEquals("Conversion from blob metadata to json and back expected to return an equal object", b, b2);
    }
}
