package org.apache.solr.store.blob.metadata;

import com.google.common.collect.*;
import edu.umd.cs.findbugs.annotations.NonNull;
import search.blobstore.BlobException;
import search.blobstore.solr.BlobCoreMetadata;

import org.apache.commons.codec.binary.Hex;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.store.Directory;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.*;
import org.apache.solr.handler.*;
import searchserver.SfdcUserData;
import searchserver.handler.SearchPromotionRuleDataHandler;
import searchserver.util.SynonymUtil;

import java.io.*;
import java.security.*;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Object capturing the metadata of the local index on a Solr Server.<p>
 * This works in conjunction with {@link BlobCoreMetadata} to find the differences between local (Solr server) and remote
 * (Blob store) commit point for a core.<p>
 * This object is somewhere between {@link org.apache.lucene.index.IndexCommit} and {@link org.apache.lucene.index.SegmentInfos}
 * and by implementing it separately we can add SFDC specific data in it (such as sequence number) and don't have to deal
 * with the complexity of class hierarchies in the Solr implementation(s).
 *
 * @author iginzburg
 * @since 214/solr.6
 */
public class ServerSideCoreMetadata {
    /**
     * Files composing the core. They are are referenced from the core's current commit point's segments_N file
     * which is ALSO included in this collection.
     */
    private final ImmutableCollection<CoreFileData> files;

    /**
     * Config files associated with the core
     */
    private final ImmutableSet<CoreConfigFileData> configFiles;

    /**
     * Hash of the directory content used to make sure the content doesn't change as we proceed to pull new files from Blob
     * (if we need to pull new files from Blob)
     */
    private final String directoryHash;

    /**
     * Salesforce core freshness tracking is not generation based (as in normal master/slave Solr) but sequence number based...
     */
    private final long sequenceNumber;

    /**
     * ...but we need generation anyway because encryption updates the generation of a core without changing the sequence number.
     */
    private final long generation;

    private final String coreName;
    private final CoreContainer container;

    /**
     * Given a core name, builds the local metadata
     * @throws Exception if core corresponding to <code>coreName</code> can't be found.
     */
    public ServerSideCoreMetadata(@NonNull String coreName, CoreContainer container) throws Exception {
        this.coreName = coreName;
        this.container = container;

        SolrCore core = container.getCore(coreName);

        if (core == null) {
            throw new Exception("Can't find core " + coreName);
        }

        try {

            IndexCommit commit = core.getDeletionPolicy().getLatestCommit();

            if (commit == null) {
                throw new BlobException("Core " + coreName + " has no available commit point");
            }

            // Sequence number was once called replay count. data structures (and DB column names) haven't been renamed everywhere.
            SfdcUserData coreUserData = SfdcUserData.getMetadataFromIndexCommit(core, commit);
            sequenceNumber = coreUserData.replayCount;
            generation = commit.getGeneration();

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
            configFiles = getConfigFilesMetadata(core);
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

    public long getSequenceNumber() {
        return this.sequenceNumber;
    }

    public long getGeneration() {
        return this.generation;
    }

    public String getDirectoryHash() {
        return this.directoryHash;
    }

    public ImmutableCollection<CoreFileData> getFiles(){
        return this.files;
    }

    public ImmutableCollection<CoreConfigFileData> getConfigFiles() {
        return configFiles;
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
     * Tells whether blob config file is fresher than local config file. It does not rely on metadata computed at the creation time
     * of this object and returned by {@link #getConfigFiles()}. Rather it directly looks into local config directory. 
     * If config file does not exist locally or blob's updatedAt is greater than local's lastModified, returns true; otherwise, false.
     */
    public boolean isBlobConfigFileFresher(BlobCoreMetadata.BlobConfigFile blobConfigFile){
        SolrCore core = container.getCore(coreName);
        try {
            List<File> localConfigFile = getConfigFiles(core).stream()
                    .filter(f -> f.getName().equals(blobConfigFile.getSolrFileName()))
                    .collect(Collectors.toList());
            assert localConfigFile.size() <= 1;
            return localConfigFile.isEmpty() || (blobConfigFile.getUpdatedAt() > localConfigFile.get(0).lastModified());
        } finally {
            core.close();
        }
    }

    private ImmutableSet<CoreConfigFileData> getConfigFilesMetadata(SolrCore core) {
        Set<File> configFiles = getConfigFiles(core);
        return ImmutableSet.copyOf(
                configFiles.stream()
                        .filter(File::exists)
                        .map(f -> new CoreConfigFileData(f.getName(), f.length(), f.lastModified()))
                        .iterator());
    }

    /**
     * Returns all the config files that are meant to be persisted/synchronized with blob store.
     * Current non-blob replication does not replicate everything under config folder, rather it only
     * replicates synonym files (included by {@link ReplicationHandlerWithSynonyms#getConfFileInfoFromCache(NamedList, Map)})
     * and elevate.xml (included by {@link ReplicationHandler#inform(SolrCore)} when reading 
     * "config/requestHandler[@name='/replication']/lst[@name='master']/str[@name='confFiles']")
     * 
     * If you add a new set of config files make sure that once created they only become empty and do not get deleted(unless they are corrupt)
     * because in blob syncing presence of config file on one side(local or blob) means it is fresher.
     */
    private Set<File> getConfigFiles(SolrCore core) {
        Set<File> configFiles = Sets.newHashSet(SynonymUtil.getSynonymFilesForCore(core)); // all synonym files
        configFiles.add(SearchPromotionRuleDataHandler.getSearchPromotionRuleConfigFile(core)); // elevate.xml carrying promotion rules
        return configFiles;
    }

    /**
     * Function "inspired" by the SFDC provided method {@link org.apache.solr.handler.SnapPuller#getDirHash} computing a hash
     * of a Solr Directory in order to make sure the directory doesn't change as we pull content into it (if we need to
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

        CoreFileData(@NonNull String fileName, long fileSize) {
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
        return "coreName=" + coreName + " sequenceNumber=" + sequenceNumber + " generation=" + generation;
    }
    
    /**
     * Information captured per local config file
     */
    public static class CoreConfigFileData extends CoreFileData {
        /** Last updated time of the file */
        public final long updatedAt;

        CoreConfigFileData(@NonNull String fileName, long fileSize, long updatedAt) {
            super(fileName, fileSize);
            this.updatedAt = updatedAt;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            CoreConfigFileData other = (CoreConfigFileData) o;
            return Objects.equals(updatedAt, other.updatedAt);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), updatedAt);
        }
    }
}
