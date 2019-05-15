package org.apache.solr.store.blob.client;

import java.util.*;

/**
 * Object defining metadata stored in blob store for each stored core and its builders.
 * This metadata includes all actual segment files as well as the segments_N file of the commit point.
 * That object is serialized to/from Json and stored in the blob store as a blob.
 *
 * @author iginzburg
 * @since 214/solr.6
 */
public class BlobCoreMetadata {

    /**
     * Name of the core as store in the Blob server (same name as the one used on the Search server unless tests do tricks).
     */
    private final String coreName;

    /**
     * Tracking is done using sequence numbers that are strictly incremented on each indexing batch.
     * 
     * Config files update (synonyms, promoted results) do not lead to increased sequence Number (or increased generation
     * for that matter) so detecting the need for a config push/pull is implemented by comparing {@link BlobConfigFile#updatedAt}
     * and {@link org.apache.solr.store.blob.metadata.ServerSideCoreMetadata.CoreConfigFileData#updatedAt}
     */
    private final long sequenceNumber;

    /**
     * Encryption does increase generation but not sequence number, so we need to compare (sequence number, generation) between
     * two cores to tell which one is fresher.
     */
    private final long generation;

    /**
     * Unique identifier of this metadata, that changes on every update to the metadata (except generating a new corrupt metadata
     * through {@link #getCorruptOf}).
     */
    private final String uniqueIdentifier;

    /**
     * Indicates that a Solr (search) server pulled this core and was then unable to open or use it. This flag is used as
     * an indication to servers pushing blobs for that core into Blob Store to push a complete set of files if they have
     * a locally working copy rather than just diffs (files missing on Blob Store).
     */
    private final boolean isCorrupt;

    /**
     * Indicates that this core has been deleted by the client. This flag is used as a marker to prevent other servers
     * from pushing their version of this core to blob and to allow local copy cleanup.
     */
    private final boolean isDeleted;

    /**
     * The array of files that constitute the current commit point of the core (as known by the Blob store).
     * This array is not ordered! There are no duplicate entries in it either (see how it's built in {@link BlobCoreMetadataBuilder}).
     */
    private final BlobFile[] blobFiles;

    /**
     * The array of config files that are associated with the core. 
     */
    private final BlobConfigFile[] blobConfigFiles;

    /**
     * Files marked for delete but not yet removed from the Blob store. Each such file contains information indicating when
     * it was marked for delete so we can actually remove the corresponding blob (and the entry from this array in the metadata)
     * when it's safe to do so even if there are (unexpected) conflicting updates to the blob store by multiple solr servers...
     * TODO: we might want to separate the metadata blob with the deletes as it's not required to always fetch the delete list when checking freshness of local core...
     */
    private final BlobFileToDelete[] blobFilesToDelete;

    /**
     * This is the constructor called by {@link BlobCoreMetadataBuilder}.
     * It always builds non "isCorrupt" and non "isDeleted" metadata. 
     * The only way to build an instance of "isCorrupt" metadata is to use {@link #getCorruptOf} and for "isDeleted" use {@link #getDeletedOf()}
     */
    BlobCoreMetadata(String coreName, BlobFile[] blobFiles, BlobConfigFile[] blobConfigFiles,
                     BlobFileToDelete[] blobFilesToDelete, long sequenceNumber, long generation) {
        this(coreName, blobFiles, blobConfigFiles, blobFilesToDelete, sequenceNumber, generation, UUID.randomUUID().toString(), false,
                false);
    }

    private BlobCoreMetadata(String coreName, BlobFile[] blobFiles, BlobConfigFile[] blobConfigFiles,
                             BlobFileToDelete[] blobFilesToDelete, long sequenceNumber, long generation,
                             String uniqueIdentifier, boolean isCorrupt, boolean isDeleted) {
        this.coreName = coreName;
        this.blobFiles = blobFiles;
        this.blobConfigFiles = blobConfigFiles;
        this.blobFilesToDelete = blobFilesToDelete;
        this.sequenceNumber = sequenceNumber;
        this.generation = generation;
        this.uniqueIdentifier = uniqueIdentifier;
        this.isCorrupt = isCorrupt;
        this.isDeleted = isDeleted;
    }

    /**
     * Given a non corrupt {@link BlobCoreMetadata} instance, creates an equivalent one based on it but marked as corrupt.<p>
     * The new instance keeps all the rest of the metadata unchanged, including the {@link #uniqueIdentifier}.
     */
    public BlobCoreMetadata getCorruptOf() {
        assert !isCorrupt;
        return new BlobCoreMetadata(coreName, blobFiles, blobConfigFiles, blobFilesToDelete, sequenceNumber, generation, uniqueIdentifier, true, isDeleted);
    }

    /**
     * Given a {@link BlobCoreMetadata} instance, creates an equivalent one based on it but marked as deleted.
     * <p>
     * The new instance keeps all the rest of the metadata unchanged, including the {@link #uniqueIdentifier}.
     */
    public BlobCoreMetadata getDeletedOf() {
        assert !isDeleted;
        return new BlobCoreMetadata(coreName, blobFiles, blobConfigFiles, blobFilesToDelete, sequenceNumber, generation,
                uniqueIdentifier, isCorrupt, true);
    }

    /**
     * Returns true if the Blob metadata was marked as deleted
     */
    public boolean getIsDeleted() {
        return isDeleted;
    }

    /**
     * Returns the core name corresponding to this metadata
     */
    public String getCoreName() {
        return coreName;
    }

    /**
     * Returns true if the Blob metadata was marked as corrupt. In which case, the core should not be pulled from the Blob Store
     * as it is useless.
     */
    public boolean getIsCorrupt() {
        return isCorrupt;
    }

    /**
     * Unique identifier of this blob core metadata. Allows quickly seeing that the core metadata has changed without comparing
     * the whole content.<p>
     * {@link #getCorruptOf()} is the only call allowing the creation of two instances of {@link BlobCoreMetadata} having
     * the same unique identifier.
     */
    public String getUniqueIdentifier() {
        return uniqueIdentifier;
    }

    public long getSequenceNumber() {
        return this.sequenceNumber;
    }

    public long getGeneration() {
        return this.generation;
    }

    public BlobFile[] getBlobFiles() {
        return blobFiles;
    }

    public BlobConfigFile[] getBlobConfigFiles() {
        return blobConfigFiles;
    }

    public BlobFileToDelete[] getBlobFilesToDelete() {
        return blobFilesToDelete;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BlobCoreMetadata that = (BlobCoreMetadata) o;

        if (this.sequenceNumber != that.sequenceNumber) return false;
        if (this.generation != that.generation) return false;
        if (this.isCorrupt != that.isCorrupt) return false;
        if (this.isDeleted != that.isDeleted) return false;
        if (!this.uniqueIdentifier.equals(that.uniqueIdentifier)) return false;
        if (!this.coreName.equals(that.coreName)) return false;

        // blobFiles array is not ordered so not using Arrays.equals here but rather Set comparison (we also know all elements are distinct in the array)
        Set<BlobFile> thisFiles = new HashSet<>(Arrays.asList(this.blobFiles));
        Set<BlobFile> thatFiles = new HashSet<>(Arrays.asList(that.blobFiles));
        if (!thisFiles.equals(thatFiles)) return false;

        // same for the conf files
        Set<BlobConfigFile> thisConfigFiles = new HashSet<>(Arrays.asList(this.blobConfigFiles));
        Set<BlobConfigFile> thatConfigFiles = new HashSet<>(Arrays.asList(that.blobConfigFiles));
        if (!thisConfigFiles.equals(thatConfigFiles)) return false;

        // same for the files to delete array
        Set<BlobFileToDelete> thisFilesToDelete = new HashSet<>(Arrays.asList(this.blobFilesToDelete));
        Set<BlobFileToDelete> thatFilesToDelete = new HashSet<>(Arrays.asList(that.blobFilesToDelete));
        return thisFilesToDelete.equals(thatFilesToDelete);
    }

    @Override
    public int hashCode() {
        int result = coreName.hashCode();
        result = 31 * result + uniqueIdentifier.hashCode();
        result = 31 * result + (int) (sequenceNumber ^ (sequenceNumber >>> 32));
        result = 31 * result + (int) (generation ^ (generation >>> 32));
        // The array of files is not ordered so need to compare as a set
        result = 31 * result + new HashSet<>(Arrays.asList(this.blobFiles)).hashCode();
        result = 31 * result + new HashSet<>(Arrays.asList(this.blobConfigFiles)).hashCode();
        result = 31 * result + new HashSet<>(Arrays.asList(this.blobFilesToDelete)).hashCode();
        result = 31 * result + (isCorrupt ? 1 : 0);
        result = 31 * result + (isDeleted ? 1 : 0);
        return result;
    }

    @Override
    public String toString() {
        return "coreName=" + coreName + " sequenceNumber=" + sequenceNumber + " generation=" + generation
                + " isCorrupt=" + isCorrupt + " uniqueIdentifier=" + uniqueIdentifier;
    }

    /**
     * A file (or blob) stored in the blob store.
     */
    public static class BlobFile {
        /**
         * Name the file should have on a Solr server retrieving it, not including the core specific part of the filename (i.e. the path)
         */
        private final String solrFileName;

        /**
         * Name of the blob representing the file on the blob store. This will initially be an absolute path on the Blob
         * server (for compatibility with {@link org.apache.solr.store.blob.client.LocalStorageClient}) but eventually might not include
         * the core name if cores are organized into per core S3 buckets).
         */
        private final String blobName;

        // TODO add some checksum here to verify blob files are not corrupt

        private final long fileSize;

        public BlobFile(String solrFileName, String blobName, long fileSize) {
            this.solrFileName = solrFileName;
            this.blobName = blobName;
            this.fileSize = fileSize;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            BlobFile blobFile = (BlobFile) o;

            if (fileSize != blobFile.fileSize) return false;
            if (!solrFileName.equals(blobFile.solrFileName)) return false;
            return blobName.equals(blobFile.blobName);
        }

        @Override
        public int hashCode() {
            int result = solrFileName.hashCode();
            result = 31 * result + blobName.hashCode();
            result = 31 * result + (int) (fileSize ^ (fileSize >>> 32));
            return result;
        }

        public String getSolrFileName() {
            return this.solrFileName;
        }

        public String getBlobName() {
            return this.blobName;
        }

        public long getFileSize() {
            return this.fileSize;
        }
        
    }

    /**
     * A file (or blob) representing config file stored in the blob store.
     */
    public static class BlobConfigFile extends BlobFile {
        /** Last updated time of the config file */
        private final long updatedAt;

        public BlobConfigFile(String solrFileName, String blobName, long fileSize, long updatedAt) {
            super(solrFileName, blobName, fileSize);
            this.updatedAt = updatedAt;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            BlobConfigFile that = (BlobConfigFile) o;
            return updatedAt == that.updatedAt;
        }

        @Override
        public int hashCode() {
            int result = super.hashCode();
            result = 31 * result + (int) (updatedAt ^ (updatedAt >>> 32));
            return result;
        }

        public long getUpdatedAt() {
            return this.updatedAt;
        }
    }

    /**
     * A file (or blob) stored in the blob store that should be deleted (after a certain "delay" to make sure it's not used
     * by a conflicting update to the core metadata on the Blob...).
     */
    public static class BlobFileToDelete extends BlobFile {

        // TODO using the delete timestamp for now but likely need something else:
        // deleted sequence number allows letting multiple sequence numbers to pass before deleting, whereas
        // an old delete (as told by its timestamp) might still be the latest update to the core if not a lot of indexing
        // activity, so hard to judge a delete is safe based on this. Possibly delete sequence and delete timestamp are
        // the safest bet, covering both cases of very high indexing activity cores (we might want to wait until timestamp
        // ages a bit given sequence number can increase quickly yet we could have a race with a server doing a slow update)
        // as well as slowly updating cores (if delete date is a week ago, even if sequence number hasn't changed, the
        // likelyhood of a really really slow update by another server causing a race is low).
        private final long deletedAt;

        public BlobFileToDelete(String solrFileName, String blobName, long fileSize, long deletedAt) {
            super(solrFileName, blobName, fileSize);

            this.deletedAt = deletedAt;
        }

        public BlobFileToDelete(BlobFile bf, long deletedAt) {
            super(bf.solrFileName, bf.blobName, bf.fileSize);

            this.deletedAt = deletedAt;
        }


        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;

            BlobFileToDelete that = (BlobFileToDelete) o;

            return deletedAt == that.deletedAt;
        }

        @Override
        public int hashCode() {
            int result = super.hashCode();
            result = 31 * result + (int) (deletedAt ^ (deletedAt >>> 32));
            return result;
        }

        public long getDeletedAt() {
            return this.deletedAt;
        }
    }
}
