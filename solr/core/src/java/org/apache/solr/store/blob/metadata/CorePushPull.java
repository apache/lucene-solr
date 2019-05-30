package org.apache.solr.store.blob.metadata;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.DirectoryFactory;
import org.apache.solr.core.SolrCore;
import org.apache.solr.store.blob.client.BlobCoreMetadata;
import org.apache.solr.store.blob.client.BlobCoreMetadata.BlobConfigFile;
import org.apache.solr.store.blob.client.BlobCoreMetadata.BlobFile;
import org.apache.solr.store.blob.client.BlobCoreMetadataBuilder;
import org.apache.solr.store.blob.client.CoreStorageClient;
import org.apache.solr.store.blob.metadata.MetadataResolver.Action;
import org.apache.solr.store.blob.metadata.ServerSideCoreMetadata.CoreFileData;
import org.apache.solr.store.blob.process.BlobDeleteManager;
import org.apache.solr.store.blob.provider.BlobStorageProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class pushing updates from the local core to the Blob Store and pulling updates from Blob store to local core.
 * This class knows about the Solr core directory structure and can translate it into file system abstractions since that's
 * what needed by Blob store.
 *
 * @author iginzburg
 * @since 214/solr.6
 */
public class CorePushPull {

    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final PushPullData pushPullData;
    private final MetadataResolver resolver;
    private final ServerSideCoreMetadata solrServerMetadata;
    private final BlobCoreMetadata blobMetadata;
    private final CoreContainer container;

    private final BlobDeleteManager blobDeleteManager;

    /**
     * Creates an instance allowing pushing and pulling local core content to/from blob store.
     * Note that usually the passed in {@link ServerSideCoreMetadata} and {@link BlobCoreMetadata} instances will have
     * the same core name, but it can be different for testing push and pulls on a single server (i.e. pulling a given
     * Blob core into a differently named local core).
     */
    public CorePushPull(PushPullData data, MetadataResolver resolver, ServerSideCoreMetadata solrServerMetadata, BlobCoreMetadata blobMetadata) {
        this.pushPullData = data;
        this.container = getContainerFromServerMetadata(solrServerMetadata);
        this.resolver = resolver;
        this.solrServerMetadata = solrServerMetadata;
        this.blobMetadata = blobMetadata;

        this.blobDeleteManager = BlobDeleteManager.get();
    }
    
    /**
     * TODO remove this constructor because it'll likely be removed. We want all blob interactions
     * to have a PushPullData propagated to it
     * 
     * Creates an instance allowing pushing and pulling core content to/from blob store.
     * 
     * @param core local {@link SolrCore} to process pushing/pushing updates to/from blob store.
     */
    public CorePushPull(SolrCore core) throws Exception {
        // Do the sequence of actions required to pull a core from the Blob store.
        CoreStorageClient blobClient = BlobStorageProvider.get().getBlobStorageClient();
        // Get blob metadata
        this.blobMetadata = blobClient.pullCoreMetadata(core.getName());
        if (blobMetadata == null) {
            throw new Exception("Could not find blob metadata for " + core.getName());
        } else if (blobMetadata.getIsCorrupt()) {
            throw new Exception("Core in blobstore is corrupt for " + core.getName());
        } else if (blobMetadata.getIsDeleted()) { 
            throw new Exception("Blob metadata is marked deleted for " + core.getName()); 
        }

        // Get local metadata
        this.container = core.getCoreContainer();
        this.solrServerMetadata = new ServerSideCoreMetadata(core.getName(), container);

        this.resolver = new MetadataResolver(solrServerMetadata, blobMetadata);

        this.blobDeleteManager = BlobDeleteManager.get();
        
        // TODO this constructor is only used in a method in BlobStoreUtils which was used for a SFDC
        // component which will not port so we'll likely remove it
        this.pushPullData = null;

        logger.info("Server metadata=" + solrServerMetadata + " Blob metadata="
                + blobMetadata + " Resolver action=" + resolver.getAction());
    }

    /**
     * Extracted to be able to pass a null ServerSideCoreMetadata to the constructor in unit tests.
     */
    protected CoreContainer getContainerFromServerMetadata(ServerSideCoreMetadata solrServerMetadata) {
        return solrServerMetadata.getCoreContainer();
    }

    /**
     * Calls {@link #pushToBlobStore(long, int)}  with current epoch time and attempt no 0.  
     */
    public BlobCoreMetadata pushToBlobStore() throws Exception {
       return pushToBlobStore(System.currentTimeMillis(), 0);
    }
    
    /**
     * Writes to the Blob store all the files that should be written to it, then updates and writes the {@link BlobCoreMetadata}.
     * After that call, the Blob store is fully updated for the core.<p>
     * In case of exception, it means that the new {@link BlobCoreMetadata} has not been written which means that the old
     * content of the Blob store remains unchanged (all new files are written with new unique names so do not interfere)
     * but it also means some garbage collection will eventually be required because new written blobs are not accounted for.<p>
     *
     * This method (need to verify this is indeed the case!) can be used either to push small updates from the local core
     * to Blob or to completely overwrite the Blob content for the core with the local content.
     * 
     * @param requestQueuedTimeMs epoch time in milliseconds when the push request was queued(meaningful in case of async pushing)
     *                            only used for logging purposes
     * @param attempt 0 based attempt number (meaningful in case of pushing with retry mechanism)
     *                only used for logging purposes 
     */
    public BlobCoreMetadata pushToBlobStore(long requestQueuedTimeMs, int attempt) throws Exception {
        assert Action.PUSH.equals(resolver.getAction()) || Action.CONFLICT.equals(resolver.getAction()) 
                || (Action.CONFIG_CHANGE.equals(resolver.getAction()) && !resolver.getConfigFilesToPush().isEmpty());

        boolean onlyPushConfigFiles = Action.CONFIG_CHANGE.equals(resolver.getAction());
        boolean isSuccess = false;
        long startTimeMs = System.currentTimeMillis();

        try {
            SolrCore solrCore = container.getCore(pushPullData.getCoreName());
            if (solrCore == null) {
                throw new Exception("Can't find core " + pushPullData.getCoreName());
            }

            try {
                CoreStorageClient blob = BlobStorageProvider.get().getBlobStorageClient();
                // Creating the new BlobCoreMetadata as a modified clone of the existing one
                BlobCoreMetadataBuilder bcmBuilder = new BlobCoreMetadataBuilder(blobMetadata, solrServerMetadata.getGeneration());

                if (!onlyPushConfigFiles) {
                    // First copy index files over to a temp directory and then push to blob store from there. 
                    // This is to avoid holding a lock over index directory involving network operation.
                    //
                    // Ideally, we don't need to lock source index directory to make temp copy because...:
                    // -all index files are write-once (http://mail-archives.apache.org/mod_mbox/lucene-java-user/201509.mbox/%3C00c001d0ed85$ee839b20$cb8ad160$@thetaphi.de%3E)
                    // -there is a possibility of a missing segment file because of merge but that could have happened even before reaching this point.
                    //  Whatever the timing maybe that will result in a failure (exception) when copying to temp and that will abort the push
                    // -segment file deletion(because of merge) and copying to temp happening concurrently should be ok as well since delete will wait 
                    //  for copy to finish (https://stackoverflow.com/questions/2028874/what-happens-to-an-open-file-handle-on-linux-if-the-pointed-file-gets-moved-del)
                    // ...But SfdcFSDirectoryFactory, that is based off CachingDirectoryFactory, can return a cached instance of Directory and
                    // that cache is only based off path and is rawLockType agnostic. Therefore passing "none" as rawLockType will not be honored if same path
                    // was accessed before with some other rawLockType until CachingDirectoryFactory#doneWithDirectory is called. 
                    // There is an overload that takes forceNew boolean and supposed to be returning new instance but CachingDirectoryFactory does not seem to honor that. 
                    // It is not worth fighting that therefore we lock the source index directory before copying to temp directory. If this much locking turns out
                    // to be problematic we can revisit this.
                    // 
                    // And without source locking we really don't need temp directory, one reason to still might have it is to avoid starting a push to blob store 
                    // that can potentially be stopped in the middle because of a concurrent merge deleting the segment files being pushed. 

                    // create a temp directory (within the core local folder).
                    String tempIndexDirName = solrCore.getDataDir() + "index.push." + System.nanoTime();
                    Directory tempIndexDir = solrCore.getDirectoryFactory().get(tempIndexDirName, DirectoryFactory.DirContext.DEFAULT, solrCore.getSolrConfig().indexConfig.lockType);
                    try {
                        Directory indexDir = solrCore.getDirectoryFactory().get(solrCore.getIndexDir(), DirectoryFactory.DirContext.DEFAULT, solrCore.getSolrConfig().indexConfig.lockType);
                        try {
                            // copy index files to the temp directory
                            for (CoreFileData cfd : resolver.getFilesToPush()) {
                                copyFileToDirectory(indexDir, cfd.fileName, tempIndexDir);
                            }
                        } finally {
                            solrCore.getDirectoryFactory().release(indexDir);
                        }

                        // Removing from the core metadata the files that should no longer be there.
                        for (BlobCoreMetadata.BlobFile d : this.resolver.getBlobFilesToDelete()) {
                            bcmBuilder.removeFile(d);
                            BlobCoreMetadata.BlobFileToDelete bftd = new BlobCoreMetadata.BlobFileToDelete(d, System.currentTimeMillis());
                            bcmBuilder.addFileToDelete(bftd);
                        }

                        // Directory's javadoc says: "Java's i/o APIs not used directly, but rather all i/o is through this API"
                        // But this is untrue/totally false/misleading. SnapPuller has File all over.
                        for (CoreFileData cfd : resolver.getFilesToPush()) {
                            // Sanity check that we're talking about the same file (just sanity, Solr doesn't update files so should never be different)
                            assert cfd.fileSize == tempIndexDir.fileLength(cfd.fileName);

                            String blobPath = pushFileToBlobStore(blob, tempIndexDir, cfd.fileName, cfd.fileSize);
                            bcmBuilder.addFile(new BlobCoreMetadata.BlobFile(cfd.fileName, blobPath, cfd.fileSize));
                        }
                    } finally {
                        removeTempDirectory(solrCore, tempIndexDirName, tempIndexDir);
                    }
                }

                // at this point we are done with pushing indexing files
                // push config files if any
                if(!resolver.getConfigFilesToPush().isEmpty()) {
                    pushConfigFiles(blob, solrCore, bcmBuilder);
                }
                // so far we have decided not to pull config changes if we are pushing changes
                if(!resolver.getConfigFilesToPull().isEmpty()) {
                    logger.info(String.format(
                            "Lost opportunity: we could have pulled config changes when pushing core=%s", pushPullData.getCoreName()));
                }

                // Before writing back the core metadata to the Blob Store, identify files that were marked as deleted and that
                // are ready to be hard deleted. Those files get enqueued for delete (that will happen later async) and are removed
                // from the core metadata (if enqueue was not possible, the files are left in the delete section of the core metadata
                // and delete will be attempted again later).
                if (blobDeleteManager != null) {
                    enqueueForHardDelete(bcmBuilder);
                }

                BlobCoreMetadata newBcm = bcmBuilder.build();

                blob.pushCoreMetadata(blobMetadata.getSharedBlobName(), newBcm);
                isSuccess = true;
                return newBcm;
            } finally {
                solrCore.close();
            }
        } finally {
            long filesAffected = resolver.getConfigFilesToPush().size();
            long bytesTransferred = resolver.getConfigFilesToPush().stream().mapToLong(cfd -> cfd.fileSize).sum();
            if(!onlyPushConfigFiles){
                filesAffected += resolver.getFilesToPush().size();
                bytesTransferred += resolver.getFilesToPush().stream().mapToLong(cfd -> cfd.fileSize).sum();
            }
            logBlobAction(resolver.getAction(), filesAffected, bytesTransferred, requestQueuedTimeMs, attempt, startTimeMs, isSuccess);
        }
    }

    /**
     * Calls {@link #pullUpdateFromBlob(long, boolean, int)}  with current epoch time and attempt no. 0.
     * @param waitForSearcher <code>true</code> if this call should wait until the index searcher is created (so that any query
     *                     after the return from this method sees the new pulled content) or <code>false</code> if we request
     *                     a new index searcher to be eventually created but do not wait for it to be created (a query
     *                     following the return from this call might see the old core content).
     */
    public void pullUpdateFromBlob(boolean waitForSearcher) throws Exception {
         pullUpdateFromBlob(System.currentTimeMillis(), waitForSearcher, 0);
    }

    /**
     * We're doing here what replication does in {@link org.apache.solr.handler.IndexFetcher#fetchLatestIndex}.<p>
     *     TODO: check changes in Solr.7's IndexFetcher. Core reloading needed?
     *
     * This method will work in 2 cases:
     * <ol>
     * <li>Local core needs to fetch an update from Blob</li>
     * <li>Local core did not exist (was created empty before calling this method) and is fetched from Blob</li>
     * </ol>
     * 
     * @param requestQueuedTimeMs epoch time in milliseconds when the pull request was queued(meaningful in case of async pushing)
     *                            only used for logging purposes
     * @param waitForSearcher <code>true</code> if this call should wait until the index searcher is created (so that any query
     *                     after the return from this method sees the new pulled content) or <code>false</code> if we request
     *                     a new index searcher to be eventually created but do not wait for it to be created (a query
     *                     following the return from this call might see the old core content).
     * @param attempt 0 based attempt number (meaningful in case of pushing with retry mechanism in case of failure)
     *                only used for logging purposes 
     *
     * @throws Exception    In the context of the PoC and first runs, we ignore errors such as Blob Server not available
     *                      or network issues. We therefore consider all exceptions thrown by this method as a sign that
     *                      it is durably not possible to pull the core from the Blob Store.
     *                      TODO This has to be revisited before going to real prod, as environemnt issues can cause massive reindexing with this strategy
     */
    public void pullUpdateFromBlob(long requestQueuedTimeMs, boolean waitForSearcher, int attempt) throws Exception {
        assert Action.PULL.equals(resolver.getAction());

        boolean isSuccess = false;
        long startTimeMs = System.currentTimeMillis();
        try {

            SolrCore solrCore = container.getCore(pushPullData.getCoreName());

            if (solrCore == null) {
                throw new Exception("Can't find core " + pushPullData.getCoreName());
            }

            try {
                // We're here because we identified no conflicts in downloading from the Blob store the files missing locally.
                // In order to make sure there still are no conflicts (local Solr server on which we run might have updated the
                // core since we checked or might do so as we download files shortly), we'll download the needed files to a temp
                // dir and before moving them to the core directory, we will check the directory hasn't changed locally.

                // Create temp directory (within the core local folder).
                String tmpIdxDirName = solrCore.getDataDir() + "index.pull." + System.nanoTime();
                Directory tmpIndexDir = solrCore.getDirectoryFactory().get(tmpIdxDirName, DirectoryFactory.DirContext.DEFAULT, solrCore.getSolrConfig().indexConfig.lockType);

                try {
                    downloadFilesFromBlob(tmpIndexDir, resolver.getFilesToPull().values());

                    String indexDir = solrCore.getIndexDir();
                    Directory dir = solrCore.getDirectoryFactory().get(indexDir, DirectoryFactory.DirContext.DEFAULT, solrCore.getSolrConfig().indexConfig.lockType);

                    try {
                        // Close the index writer to stop changes to this core
                        solrCore.getUpdateHandler().getSolrCoreState().closeIndexWriter(solrCore, true);

                        boolean thrownException = false;
                        try {

                            // Make sure Solr core directory content hasn't changed since we decided what we want to pull from Blob
                            if (!solrServerMetadata.isSameDirectoryContent(dir)) {
                                // Maybe return something less agressive than trowing an exception? TBD once we end up calling this method :)
                                throw new Exception("Local Directory content " + indexDir + " has changed since Blob pull started. Aborting pull.");
                            }

                            // Copy all files into the Solr directory (there are no naming conflicts since we're doing an Action.PULL)
                            // Move the segments_N file last once all other are ok.
                            String segmentsN = null;
                            for (BlobCoreMetadata.BlobFile bf : resolver.getFilesToPull().values()) {
                                if (resolver.isSegmentsNFilename(bf)) {
                                    assert segmentsN == null;
                                    segmentsN = bf.getSolrFileName();
                                } else {
                                    // Copy all non segments_N files
                                    moveFileToDirectory(solrCore, tmpIndexDir, bf.getSolrFileName(), dir);
                                }
                            }

                            assert segmentsN != null;

                            // Copy segments_N file. From this point on the local core might be accessed and is up to date with Blob content
                            moveFileToDirectory(solrCore, tmpIndexDir, segmentsN, dir);
                        } catch (Exception e) {
                            // Used in the finally below to not mask an exception thrown from the try block above
                            thrownException = true;
                            throw e;
                        } finally {
                            try {
                                // The closed index writer must be opened back (in the finally bloc)
                                // TODO this has been observed to throw org.apache.lucene.index.CorruptIndexException on certain types of corruptions in Blob Store. We need to handle this correctly (maybe we already do).
                                solrCore.getUpdateHandler().getSolrCoreState().openIndexWriter(solrCore);
                            } catch (IOException ioe) {
                                // CorruptCoreHandler.notifyBlobPullFailure(container, coreName, blobMetadata);
                                if (!thrownException) {
                                    // Do not mask a previous exception with a more recent one so only throw the new one if none was thrown previously
                                    throw ioe;
                                }
                            }
                        }
                    } finally {
                        solrCore.getDirectoryFactory().release(dir);
                    }
                } finally {
                    removeTempDirectory(solrCore, tmpIdxDirName, tmpIndexDir);
                }

                // at this point we are done with pulling indexing files
                // now pull config files if any
                if(!resolver.getConfigFilesToPull().isEmpty()) {
                    pullConfigFiles(solrCore);
                    try {
                        // Following SnapPuller.reloadCore(): when config files are updated it reloads the core.
                        // Unlike SnapPuller this is done on current thread. I am not sure if there is a need to do otherwise.
                        // The assumption here is if there is any async need then the caller of current method would have already taken care of that.
                        container.reload(solrCore.getName());
                    } catch (Exception ex){
                        // TODO: W-5433771 handle corrupt config files. Today, if loading of core SynonymDataHandler#inform(SolrCore) 
                        // runs into exception we remove all the synonym files. Make sure we do not run into cycles with that.
                        logger.warn(String.format(
                                "Core reloading failed after pulling new config files from blob core=%s", pushPullData.getCoreName()), ex);
                        throw ex;
                    }
                }

                // so far we have decided not to push config changes if we are pulling changes
                if(!resolver.getConfigFilesToPush().isEmpty()) {
                    logger.info(String.format("Lost opportunity: we could have pushed config changes when pulling core=%s", pushPullData.getCoreName()));
                }

                try {
                    if (waitForSearcher) {
                        // Open and register a new searcher, we don't need it but we wait for it to be open.
                        Future[] waitSearcher = new Future[1];
                        solrCore.getSearcher(true, false, waitSearcher, true);
                        if (waitSearcher[0] == null) {
                            throw new Exception("Can't wait for index searcher to be created. Future queries might misbehave for core=" + pushPullData.getCoreName());
                        } else {
                            waitSearcher[0].get();
                        }

                    } else {
                        // Open and register a new searcher, but don't wait and we don't need it either.
                        solrCore.getSearcher(true, false, null, true);
                    }
                } catch (SolrException se) {
                    // CorruptCoreHandler.notifyBlobPullFailure(container, coreName, blobMetadata);
                    throw se;
                }
                isSuccess = true;
            } finally {
                solrCore.close();
            }
        } finally {
            long filesAffected = resolver.getFilesToPull().size() + resolver.getConfigFilesToPull().size();
            long bytesTransferred = resolver.getFilesToPull().values().stream().mapToLong(cfd -> cfd.getFileSize()).sum() + 
                    resolver.getConfigFilesToPull().values().stream().mapToLong(cfd -> cfd.getFileSize()).sum();
            logBlobAction(resolver.getAction(), filesAffected, bytesTransferred, requestQueuedTimeMs, attempt, startTimeMs, isSuccess);
        }
    }

    protected String getConfigDir(SolrCore core) {
        // Since we use configsets in Solr 7, the core.getResourceLoader().getConfigDir()
        // points to the config directory under the configset dir that is shared by _all_ cores.
        // We override this to a core specific directory instead to load our core specific file.
        // return CoreUtil.getCoreConfigDirectory(core.getResourceLoader()).getAbsolutePath();
        return null;
    }

    /**
     * Acquires lock on config directory, copy {@link MetadataResolver#getConfigFilesToPush()} to a temp directory, 
     * releases lock on config directory and then pushes files from temp directory to blob store
     * The reason to acquire lock is avoid reading a corrupt config file that can manifest from a concurrent update from core app
     * The reason to involve temp directory is avoid lock over config dir for network operation
     */
    private void pushConfigFiles(CoreStorageClient blob, SolrCore solrCore, BlobCoreMetadataBuilder bcmBuilder) throws Exception {
        assert (Action.PUSH.equals(resolver.getAction()) || Action.CONFIG_CHANGE.equals(resolver.getAction())) && !resolver.getConfigFilesToPush().isEmpty();

        // create a temp directory (within the core local folder).
        String tempConfigDirName = solrCore.getDataDir() + "config.push." + System.nanoTime();
        Directory tempConfigDir = solrCore.getDirectoryFactory().get(tempConfigDirName, DirectoryFactory.DirContext.META_DATA, solrCore.getSolrConfig().indexConfig.lockType);
        try {
            // acquire lock on config directory
            Directory configDir = solrCore.getDirectoryFactory().get(getConfigDir(solrCore),
                    DirectoryFactory.DirContext.META_DATA, solrCore.getSolrConfig().indexConfig.lockType);
            try {
                // copy config files to the temp directory
                for (ServerSideCoreMetadata.CoreConfigFileData cfd : resolver.getConfigFilesToPush()) {
                    copyFileToDirectory(configDir, cfd.fileName, tempConfigDir);
                }
            } finally {
                // release lock on config directory
                solrCore.getDirectoryFactory().release(configDir);
            }
            // push config files from temp config directory to blob store
            for (ServerSideCoreMetadata.CoreConfigFileData cfd : resolver.getConfigFilesToPush()) {
                String blobPath = pushFileToBlobStore(blob, tempConfigDir, cfd.fileName, cfd.fileSize);
                bcmBuilder.addConfigFile(new BlobCoreMetadata.BlobConfigFile(cfd.fileName, blobPath, cfd.fileSize, cfd.updatedAt));
            }
        } finally {
            removeTempDirectory(solrCore, tempConfigDirName, tempConfigDir);
        }
    }

    /**
     * Pushes a local file to blob store and returns a unique path to newly created blob  
     */
    private String pushFileToBlobStore(CoreStorageClient blob, Directory dir, String fileName, long fileSize) throws Exception {
        String blobPath;
        IndexInput ii = dir.openInput(fileName, IOContext.READONCE);
        try (InputStream is = new IndexInputStream(ii)) {
            blobPath = blob.pushStream(blobMetadata.getSharedBlobName(), is, fileSize);
        }
        return blobPath;
    }

    /**
     * Downloads {@link MetadataResolver#getConfigFilesToPull()} from blob store to a local temp directory,
     * acquires lock on config directory and then moves files from temp directory to config directory
     * The reason to acquire lock is avoid corrupting config file that can manifest from a concurrent update from core app
     * The reason to involve temp directory is avoid lock over config dir for network operation
     */
    private void pullConfigFiles(SolrCore solrCore) throws Exception {
        assert Action.PULL.equals(resolver.getAction()) && !resolver.getConfigFilesToPull().isEmpty();

        // create temp directory (within the core local folder).
        String tempConfigDirName = solrCore.getDataDir() + "config.pull." + System.nanoTime();
        Directory tempConfigDir = solrCore.getDirectoryFactory().get(tempConfigDirName, DirectoryFactory.DirContext.META_DATA, solrCore.getSolrConfig().indexConfig.lockType);
        try {
            // download config files to a local temp directory
            downloadFilesFromBlob(tempConfigDir, resolver.getConfigFilesToPull().values());

            // acquire lock on config directory
            Directory configDir = solrCore.getDirectoryFactory().get(getConfigDir(solrCore),
                    DirectoryFactory.DirContext.META_DATA, solrCore.getSolrConfig().indexConfig.lockType);
            try {
                //move downloaded config files from temp directory to core's config directory
                for (BlobConfigFile bcf : resolver.getConfigFilesToPull().values()) {
                    // Local config file could have changed since we decided to pull it from blob
                    // if it has changed we will skip it because it could have been updated locally through core app
                    // or by another pull ran before this pull
                    if (solrServerMetadata.isBlobConfigFileFresher(bcf)) {
                        moveFileToDirectory(solrCore, tempConfigDir, bcf.getSolrFileName(), configDir);
                        new File(getConfigDir(solrCore), bcf.getSolrFileName()).setLastModified(bcf.getUpdatedAt());
                    } else {
                        logger.info(
                        	String.format("Skipping pull of config file because blob copy is not fresher anymore, configFileName=%s core=%s",
                            bcf.getSolrFileName(), pushPullData.getCoreName()));
                    }
                }
            } finally {
                // release lock on config directory
                solrCore.getDirectoryFactory().release(configDir);
            }
        } finally {
            removeTempDirectory(solrCore, tempConfigDirName, tempConfigDir);
        }
    }

    private void removeTempDirectory(SolrCore solrCore, String tempDirName, Directory tempDir) throws IOException {
        try {
            // Remove temp directory
            solrCore.getDirectoryFactory().doneWithDirectory(tempDir);
            solrCore.getDirectoryFactory().remove(tempDir);
        } catch (Exception e) {
            logger.warn("Cannot remove temp directory " + tempDirName, e);
        } finally {
            solrCore.getDirectoryFactory().release(tempDir);
        }
    }

    /**
     * This method is to be used when the local core exists but is in conflict with the Blob store version and it has
     * been decided (by whoever calls this method) that the blob core version should prevail and replace the local core.
     */
    public BlobCoreMetadata replaceLocalCoreByBlobs() throws Exception {
        assert Action.CONFLICT.equals(resolver.getAction());


        throw new IllegalStateException("Method not implemented yet");
    }
    
    public boolean shouldPerformPull() {
        return Action.PULL.equals(resolver.getAction());
    }

    /**
     * Given a metadata builder (ready to be written back to the Blob store), identifies blob files that are ripe for
     * hard delete, enqueues them for delete and if the enqueue was successful, removes them from the metadata builder.<p>
     *
     * There is a slight mix of two abstraction levels here: on the metadata builder we manipulate a local data structure
     * (without guarantee that it will be written back to Blob store given there are no transactions here), and on the enqueue
     * the blob files for delete we actually enqueue them and threads might start processing the deletes right away.<p>
     *
     * This is ok, as the deletes are not related to any specific transaction, we're just removing files that need to be removed
     * (even if the operation in which they are removed eventually doesn't succeed, such as a core push for example).
     * @param bcmBuilder The builder for the core metadata for which files ripe for hard delete should be enqueued for
     *                   delete (if possible) and removed from the set of file to delete.
     */
    void enqueueForHardDelete(BlobCoreMetadataBuilder bcmBuilder) throws Exception {
        Iterator<BlobCoreMetadata.BlobFileToDelete> it = bcmBuilder.getDeletedFilesIterator();
        Set<BlobCoreMetadata.BlobFileToDelete> filesToDelete = new HashSet<>();
        while (it.hasNext()) {
            BlobCoreMetadata.BlobFileToDelete ftd = it.next();
            if (okForHardDelete(ftd)) {
                filesToDelete.add(ftd);
            }
        }        

        if (enqueueForDelete(bcmBuilder.getSharedBlobName(), filesToDelete)) {
            bcmBuilder.removeFilesFromDeleted(filesToDelete);
        }
    }
    
    protected boolean enqueueForDelete(String sharedBlobName, Set<BlobCoreMetadata.BlobFileToDelete> blobFiles) {
        if (blobFiles == null || blobFiles.isEmpty()) {
            return false;
        }
        
        Set<String> blobNames = blobFiles.stream()
                                .map(blobFile -> blobFile.getBlobName())
                                .collect(Collectors.toCollection(HashSet::new));
        
        return blobDeleteManager.enqueueForDelete(sharedBlobName, blobNames);
    }

    /**
     * Returns true if a deleted blob file (i.e. a file marked for delete but not deleted yet) can be hard deleted now.
     */
    protected boolean okForHardDelete(BlobCoreMetadata.BlobFileToDelete file) {
        // For now we only check how long ago the file was marked for delete.
        return System.currentTimeMillis() - file.getDeletedAt() >= blobDeleteManager.getDeleteDelayMs();
    }

    /**
     * Logs soblb line for push or pull action 
     * TODO: This is for callers of this method.
     * fileAffected and bytesTransferred represent correct values only in case of success
     * In case of failure(partial processing) we are not accurate.
     * Do we want to change that? If yes, then in case of pull is downloading of files locally to temp folder is considered
     * transfer or moving from temp dir to final destination. One option could be to just make them -1 in case of failure.
     */
    private void logBlobAction(Action action, long filesAffected, long bytesTransferred, long requestQueuedTimeMs, int attempt, long startTimeMs, boolean isSuccess) throws Exception {
        assert Action.PUSH.equals(resolver.getAction()) || Action.PULL.equals(resolver.getAction()) || Action.CONFIG_CHANGE.equals(resolver.getAction());

        long localGenerationNumber = solrServerMetadata.getGeneration();
        long blobGenerationNumber = blobMetadata.getGeneration();
        long now = System.currentTimeMillis();
        long runTime = now - startTimeMs;
        long startLatency = now - requestQueuedTimeMs;

        CoreStorageClient blobClient = BlobStorageProvider.get().getBlobStorageClient();
        String message = String.format("coreName=%s action=%s storageProvider=%s bucketRegion=%s bucketName=%s "
                + "runTime=%s startLatency=%s bytesTransferred=%s attempt=%s localGenerationNumber=%s "
                + "blobGenerationNumber=%s filesAffected=%s isSuccess=%s",
                pushPullData.getCoreName(), action.name(), blobClient.getStorageProvider().name(), blobClient.getBucketRegion(), 
                blobClient.getBucketName(), runTime, startLatency, bytesTransferred, attempt, localGenerationNumber,
                blobGenerationNumber, filesAffected, isSuccess);
        logger.info(message);
    }

    /**
     * Downloads files from the Blob store 
     * @param destDir (temporary) directory into which files should be downloaded.
     * @param filesToDownload blob files to be downloaded
     */
    private void downloadFilesFromBlob(Directory destDir, Collection<? extends BlobFile> filesToDownload) throws Exception {
        CoreStorageClient blobClient = BlobStorageProvider.get().getBlobStorageClient();

        // Synchronously download all Blob blobs (remember we're running on an async thread, so no need to be async twice unless
        // we eventually want to parallelize downloads of multiple blobs, but for the PoC we don't :)
        for (BlobFile bf: filesToDownload) {
            logger.info("About to create " + bf.getSolrFileName() + " for core " + pushPullData.getCoreName());
            IndexOutput io = destDir.createOutput(bf.getSolrFileName(), DirectoryFactory.IOCONTEXT_NO_CACHE);

            try (OutputStream outStream = new IndexOutputStream(io);
                InputStream bis = blobClient.pullStream(bf.getBlobName())) {
                IOUtils.copy(bis, outStream);
            }
        }
    }

    /**
     * Copies {@code fileName} from {@code fromDir} to {@code toDir}
     */
    private void copyFileToDirectory(Directory fromDir, String fileName, Directory toDir) throws IOException {
        toDir.copyFrom(fromDir, fileName, fileName, DirectoryFactory.IOCONTEXT_NO_CACHE);
    }

    /**
     * Moves {@code fileName} from {@code fromDir} to {@code toDir}
     */
    private void moveFileToDirectory(SolrCore solrCore, Directory fromDir, String fileName, Directory toDir) throws IOException {
        // We don't need to keep the original files so we move them over.
        solrCore.getDirectoryFactory().move(fromDir, toDir, fileName, DirectoryFactory.IOCONTEXT_NO_CACHE);
    }

    /**
     * Wraps an {@link IndexInput} into an {@link InputStream}, while correctly converting the SIGNED bytes returned by
     * the {@link IndexInput} into the "unsigned bytes stored in an integer" expected to be returned by {@link InputStream#read()}.<p>
     * Class {@link org.apache.solr.util.PropertiesInputStream} does almost the same thing but
     * {@link org.apache.solr.util.PropertiesInputStream#read()} does not unsign the returned value, breaking the contract
     * of {@link InputStream#read()} as stated in its Javadoc "Returns: the next byte of data, or -1 if the end of the stream is reached.".<p>
     *
     * Performance is likely lower using this class than doing direct file manipulations. To keep in mind if we have streaming perf issues.
     */
    static class IndexInputStream extends InputStream {
        private final IndexInput indexInput;

        IndexInputStream(IndexInput indexInput) {
            this.indexInput = indexInput;
        }

        @Override
        public int read() throws IOException {
            try {
                return indexInput.readByte() & 0xff;
            } catch (EOFException e) {
                return -1;
            }
        }

        @Override
        public void close() throws IOException {
            super.close();
            indexInput.close();
        }
    }

    /**
     * This class wraps an {@link IndexOutput} inside an {@link OutputStream}. It happens to be identical to the (at least
     * current) implementation of {@link org.apache.solr.util.PropertiesOutputStream} but copying it nonetheless for future proofing.<p>
     * Note then that in theory, {@link org.apache.solr.util.PropertiesOutputStream#write(int)} should have refused negative values as parameters
     * to retain symmetry with {@link org.apache.solr.util.PropertiesInputStream}... That would have saved me a day of debug.<p>
     *
     * Performance is likely lower using this class than doing direct file manipulations. To keep in mind if we have streaming perf issues.
     */
    static class IndexOutputStream extends OutputStream {
        private final IndexOutput indexOutput;

        IndexOutputStream(IndexOutput indexOutput) {
            this.indexOutput = indexOutput;
        }

        @Override
        public void write(int b) throws IOException {
            indexOutput.writeByte((byte) b);
        }

        @Override
        public void close() throws IOException {
            super.close();
            indexOutput.close();
        }
    }
}
