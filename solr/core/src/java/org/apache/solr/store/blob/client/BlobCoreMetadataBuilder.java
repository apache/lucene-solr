package org.apache.solr.store.blob.client;

import java.util.*;

/**
 * Builder for {@link BlobCoreMetadata}.
 */
public class BlobCoreMetadataBuilder {

    final private String sharedBlobName;
    final private Set<BlobCoreMetadata.BlobFile> blobFiles;
    final private Set<BlobCoreMetadata.BlobConfigFile> blobConfigFiles;

    public BlobCoreMetadataBuilder(String sharedBlobName) {
        this.sharedBlobName = sharedBlobName;
        this.blobFiles = new HashSet<>();
        this.blobConfigFiles = new HashSet<>();
    }

    /**
     * Builder used for "cloning" then modifying an existing instance of {@link BlobCoreMetadata}.
     */
    public BlobCoreMetadataBuilder(BlobCoreMetadata bcm) {
        this.sharedBlobName = bcm.getSharedBlobName();
        this.blobFiles = new HashSet<>(Arrays.asList(bcm.getBlobFiles()));
        this.blobConfigFiles = new HashSet<> (Arrays.asList(bcm.getBlobConfigFiles()));
    }

    public String getSharedBlobName() {
        return this.sharedBlobName;
    }

    /**
     * Builds a {@link BlobCoreMetadata} for a non existing core of a given name.
     */
    static public BlobCoreMetadata buildEmptyCoreMetadata(String sharedBlobName) {
        return (new BlobCoreMetadataBuilder(sharedBlobName)).build();
    }

    /**
     * Adds a file to the set of "active" files listed in the metadata
     */
    public BlobCoreMetadataBuilder addFile(BlobCoreMetadata.BlobFile f) {
        this.blobFiles.add(f);
        return this;
    }

    /**
     * Removes a file from the set of "active" files listed in the metadata
     */
    public BlobCoreMetadataBuilder removeFile(BlobCoreMetadata.BlobFile f) {
        boolean removed = this.blobFiles.remove(f);
        assert removed; // If we remove things that are not there, likely a bug in our code
        return this;
    }
    
    /**
     * Adds a config file to the set of "active" config files listed in the metadata
     */
    public BlobCoreMetadataBuilder addConfigFile(BlobCoreMetadata.BlobConfigFile f) {
        this.blobConfigFiles.add(f);
        return this;
    }

    public BlobCoreMetadata build() {
        // TODO make this fail if we find more than one segments_N files.
        BlobCoreMetadata.BlobFile[] blobFilesArray = this.blobFiles.toArray(new BlobCoreMetadata.BlobFile[this.blobFiles.size()]);
        BlobCoreMetadata.BlobConfigFile[] blobConfigFilesArray = this.blobConfigFiles.toArray(new BlobCoreMetadata.BlobConfigFile[this.blobConfigFiles.size()]);

        return new BlobCoreMetadata(this.sharedBlobName, blobFilesArray, blobConfigFilesArray);
    }
}
