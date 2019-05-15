package org.apache.solr.store.blob.client;

import java.util.*;

/**
 * Builder for {@link BlobCoreMetadata}.
 *
 * @author iginzburg
 * @since 214/solr.6
 */
public class BlobCoreMetadataBuilder {
    /**
     * Generation and sequence number in metadata of cores not existing on the Blob Store.
     */
    public static final long UNDEFINED_VALUE = -1L;

    final private String coreName;
    final private long sequenceNumber;
    final private long generation;
    final private Set<BlobCoreMetadata.BlobFile> blobFiles;
    final private Set<BlobCoreMetadata.BlobConfigFile> blobConfigFiles;
    final private Set<BlobCoreMetadata.BlobFileToDelete> blobFilesToDelete;

    public BlobCoreMetadataBuilder(String coreName, long sequenceNumber, long generation) {
        this.coreName = coreName;
        this.sequenceNumber = sequenceNumber;
        this.generation= generation;
        this.blobFiles = new HashSet<>();
        this.blobConfigFiles = new HashSet<>();
        this.blobFilesToDelete = new HashSet<>();
    }

    /**
     * Builder used for "cloning" then modifying an existing instance of {@link BlobCoreMetadata}.
     * The new sequence number and generation have to be passed in because they are final and can't be set later.
     */
    public BlobCoreMetadataBuilder (BlobCoreMetadata bcm, long sequenceNumber, long generation) {
        this.coreName = bcm.getCoreName();
        this.sequenceNumber = sequenceNumber;
        this.generation = generation;
        this.blobFiles = new HashSet<>(Arrays.asList(bcm.getBlobFiles()));
        this.blobConfigFiles = new HashSet<> (Arrays.asList(bcm.getBlobConfigFiles()));
        this.blobFilesToDelete = new HashSet<>(Arrays.asList(bcm.getBlobFilesToDelete()));
    }

    public String getCoreName() {
        return this.coreName;
    }

    /**
     * Builds a {@link BlobCoreMetadata} for a non existing core of a given name.
     */
    static public BlobCoreMetadata buildEmptyCoreMetadata(String coreName) {
        return (new BlobCoreMetadataBuilder(coreName, UNDEFINED_VALUE, UNDEFINED_VALUE)).build();
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

    /**
     * Adds a file to the set of files to delete listed in the metadata<p>
     * This method should always be called with {@link #removeFile(BlobCoreMetadata.BlobFile)} above. Possibly it's
     * better to only have a single method doing both operations (TODO).
     */
    public BlobCoreMetadataBuilder addFileToDelete(BlobCoreMetadata.BlobFileToDelete f) {
        this.blobFilesToDelete.add(f);
        return this;
    }

    /**
     * Returns an iterator on the set of files to delete.
     * The returned iterator will be used to remove files from the set (as they are enqueued for hard delete from the Blob store).
     */
    public Iterator<BlobCoreMetadata.BlobFileToDelete> getDeletedFilesIterator() {
        return this.blobFilesToDelete.iterator();
    }
    
    /**
     * Removes a file from the set of "deleted" files listed in the metadata
     */
    public BlobCoreMetadataBuilder removeFilesFromDeleted(Set<BlobCoreMetadata.BlobFileToDelete> files) {
        int originalSize = this.blobFilesToDelete.size();
        boolean removed = this.blobFilesToDelete.removeAll(files);
        int totalRemoved = originalSize - this.blobFilesToDelete.size();
        
        // If we remove things that are not there, likely a bug in our code
        assert removed && (totalRemoved == files.size()); 
        return this;
    }

    public BlobCoreMetadata build() {
        // TODO make this fail if we find more than one segments_N files.
        BlobCoreMetadata.BlobFile[] blobFilesArray = this.blobFiles.toArray(new BlobCoreMetadata.BlobFile[this.blobFiles.size()]);
        BlobCoreMetadata.BlobConfigFile[] blobConfigFilesArray = this.blobConfigFiles.toArray(new BlobCoreMetadata.BlobConfigFile[this.blobConfigFiles.size()]);
        BlobCoreMetadata.BlobFileToDelete[] blobFilesToDeleteArray = this.blobFilesToDelete.toArray(new BlobCoreMetadata.BlobFileToDelete[this.blobFilesToDelete.size()]);

        return new BlobCoreMetadata(this.coreName, blobFilesArray, blobConfigFilesArray, blobFilesToDeleteArray, sequenceNumber, generation);
    }
}
