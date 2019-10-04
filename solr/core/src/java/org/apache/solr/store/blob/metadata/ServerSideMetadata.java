package org.apache.solr.store.blob.metadata;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Objects;

import org.apache.commons.codec.binary.Hex;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.store.Directory;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.DirectoryFactory;
import org.apache.solr.core.SolrCore;
import org.apache.solr.store.blob.client.BlobCoreMetadata;
import org.apache.solr.store.blob.client.BlobException;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableSet;

/**
 * Object capturing the metadata of a shard index on a Solr node. 
 * 
 * This works in conjunction with {@link BlobCoreMetadata} to find the differences between 
 * local (Solr node) and remote (Blob store) commit point for a core.<p>
 * 
 * This object is somewhere between {@link org.apache.lucene.index.IndexCommit} and {@link org.apache.lucene.index.SegmentInfos}
 * and by implementing it separately we can add additional metadata to it as needed.
 */
public class ServerSideMetadata {
    
    /**
     * Files composing the core. They are are referenced from the core's current commit point's segments_N file
     * which is ALSO included in this collection.
     */
    private final ImmutableCollection<CoreFileData> files;

    /**
     * Hash of the directory content used to make sure the content doesn't change as we proceed to pull new files from Blob
     * (if we need to pull new files from Blob)
     */
    private final String directoryHash;

    private final SolrCore core;
    private final String coreName;
    private final CoreContainer container;

    /**
     * Given a core name, builds the local metadata
     * 
     * 
     * @throws Exception if core corresponding to <code>coreName</code> can't be found.
     */
    public ServerSideMetadata(String coreName, CoreContainer container) throws Exception {
        this.coreName = coreName;
        this.container = container;
        this.core = container.getCore(coreName);

        if (core == null) {
            throw new Exception("Can't find core " + coreName);
        }

        try {
            IndexCommit commit = core.getDeletionPolicy().getLatestCommit();
            if (commit == null) {
                throw new BlobException("Core " + coreName + " has no available commit point");
            }

            // Work around possible bug returning same file multiple times by using a set here
            // See org.apache.solr.handler.ReplicationHandler.getFileList()
            ImmutableCollection.Builder<CoreFileData> builder = new ImmutableSet.Builder<>();

            Directory coreDir = core.getDirectoryFactory().get(core.getIndexDir(), DirectoryFactory.DirContext.DEFAULT, core.getSolrConfig().indexConfig.lockType);
            try {
                // Capture now the hash and verify again if we need to pull content from the Blob store into this directory,
                // to make sure there are no local changes at the same time that might lead to a corruption in case of interaction
                // with the download.
                directoryHash = getSolrDirectoryHash(coreDir);

                for (String fileName : commit.getFileNames()) {
                    // Note we add here all segment related files as well as the commit point's segments_N file
                    // Note commit points do not contain lock (write.lock) files.
                    builder.add(new CoreFileData(fileName, coreDir.fileLength(fileName)));
                }
            } finally {
                core.getDirectoryFactory().release(coreDir);
            }
            files = builder.build();
        } finally {
            core.close();
        }
    }

    public String getCoreName() {
        return this.coreName;
    }

    public CoreContainer getCoreContainer() {
        return this.container;
    }

    public String getDirectoryHash() {
        return this.directoryHash;
    }

    public ImmutableCollection<CoreFileData> getFiles(){
        return this.files;
    }

    /**
     * Returns <code>true</code> if the contents of the directory passed into this method is identical to the contents of
     * the directory of the Solr core of this instance, taken at instance creation time.<p>
     *
     * Passing in the Directory (expected to be the directory of the same core used during construction) because it seems
     * safer than trying to get it again here...
     */
    public boolean isSameDirectoryContent(Directory coreDir) throws NoSuchAlgorithmException, IOException {
        return directoryHash.equals(getSolrDirectoryHash(coreDir));
    }

    /**
     * Computes a hash of a Solr Directory in order to make sure the directory doesn't change as we pull content into it (if we need to
     * pull content into it)
     */
    private String getSolrDirectoryHash(Directory coreDir) throws NoSuchAlgorithmException, IOException {
        MessageDigest digest = MessageDigest.getInstance("sha1"); // not sure MD5 is available in Solr jars

        String[] filesNames = coreDir.listAll();
        // Computing the hash requires items to be submitted in the same order...
        Arrays.sort(filesNames);

        for (String fileName : filesNames) {
            // .lock files come and go. Ignore them (we're closing the Index Writer before adding any pulled files to the Core)
            if (!fileName.endsWith(".lock")) {
                // Hash the file name and file size so we can tell if any file has changed (or files appeared or vanished)
                digest.update(fileName.getBytes());
                try {
                    digest.update(Long.toString(coreDir.fileLength(fileName)).getBytes());
                } catch (FileNotFoundException fnf) {
                    // The file was deleted between the listAll() and the check, use an impossible size to not match a digest
                    // for which the file is completely present or completely absent.
                    digest.update(Long.toString(-42).getBytes());
                }
            }
        }

        final String hash = new String(Hex.encodeHex(digest.digest()));
        return hash;
    }

    /**
     * Information we capture per local core file (segments_N file *included*)
     */
    public static class CoreFileData {
        /** Local file name, no path */
        public final String fileName;
        /** Size in bytes */
        public final long fileSize;

        CoreFileData(String fileName, long fileSize) {
            this.fileName = fileName;
            this.fileSize = fileSize;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            CoreFileData other = (CoreFileData) o;

            return Objects.equals(fileSize, other.fileSize) && 
                    Objects.equals(fileName, other.fileName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(fileName, fileSize);
        }

    }

    @Override
    public String toString() {
        return "collectionName=" + core.getCoreDescriptor().getCollectionName() +
            " shardName=" + core.getCoreDescriptor().getCloudDescriptor().getShardId() + 
            " coreName=" + core.getName();
    }
}
