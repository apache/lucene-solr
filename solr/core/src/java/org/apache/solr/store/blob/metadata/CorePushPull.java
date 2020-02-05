/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.store.blob.metadata;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.io.IOUtils;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.DirectoryFactory;
import org.apache.solr.core.SolrCore;
import org.apache.solr.store.blob.client.BlobCoreMetadata;
import org.apache.solr.store.blob.client.BlobCoreMetadata.BlobFile;
import org.apache.solr.store.blob.client.BlobCoreMetadataBuilder;
import org.apache.solr.store.blob.client.CoreStorageClient;
import org.apache.solr.store.blob.client.ToFromJson;
import org.apache.solr.store.blob.metadata.ServerSideMetadata.CoreFileData;
import org.apache.solr.store.blob.metadata.SharedStoreResolutionUtil.SharedMetadataResolutionResult;
import org.apache.solr.store.blob.process.BlobDeleteManager;
import org.apache.solr.store.blob.util.BlobStoreUtils;
import org.apache.solr.store.shared.metadata.SharedShardMetadataController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class pushing updates from the local core to the Blob Store and pulling updates from Blob store to local core.
 * This class knows about the Solr core directory structure and can translate it into file system abstractions since that's
 * what needed by Blob store.
 */
public class CorePushPull {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final CoreStorageClient coreStorageClient;
    
    // contains metadata needed to interact with the shared store
    private final PushPullData pushPullData;
    // contains the listing of files that need to be pushed or pulled to/from the shared store
    private final SharedMetadataResolutionResult resolvedMetadataResult;
    
    private final ServerSideMetadata solrServerMetadata;
    private final BlobCoreMetadata blobMetadata;
    private final CoreContainer container;
    private final BlobDeleteManager deleteManager;

    /**
     * Creates an instance allowing pushing and pulling local core content to/from blob store.
     */
    public CorePushPull(CoreStorageClient client, BlobDeleteManager deleteManager,
        PushPullData data, SharedMetadataResolutionResult resolvedMetadataResult, 
        ServerSideMetadata solrServerMetadata, BlobCoreMetadata blobMetadata) {
      this.coreStorageClient = client;
      this.deleteManager = deleteManager;
      this.container = getContainerFromServerMetadata(solrServerMetadata);  
      this.pushPullData = data;
      this.resolvedMetadataResult = resolvedMetadataResult;
      this.solrServerMetadata = solrServerMetadata;
      this.blobMetadata = blobMetadata;
    }

    /**
     * Extracted to be able to pass a null ServerSideCoreMetadata to the constructor in unit tests.
     */
    protected CoreContainer getContainerFromServerMetadata(ServerSideMetadata solrServerMetadata) {
        return solrServerMetadata.getCoreContainer();
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
     * @param currentMetadataSuffix suffix of the core.metadata file corresponding to {@link CorePushPull#blobMetadata}
     *                              TODO: there is an existing todo with delete logic where this parameter is consumed  
     *                                    with that add a metadataSuffix field to {@link BlobCoreMetadata} 
     * @param newMetadataSuffix suffix of the new core.metadata file to be created as part of this push
     */
    public BlobCoreMetadata pushToBlobStore(String currentMetadataSuffix, String newMetadataSuffix) throws Exception {
      long startTimeMs = BlobStoreUtils.getCurrentTimeMs();
      SolrCore solrCore = container.getCore(pushPullData.getCoreName());
      if (solrCore == null) {
        throw new Exception("Can't find core " + pushPullData.getCoreName());
      }
      
      FileTransferCounter counter = new FileTransferCounter();
      boolean isSuccessful = false;

      try {
        // Creating the new BlobCoreMetadata as a modified clone of the existing one
        BlobCoreMetadataBuilder bcmBuilder = new BlobCoreMetadataBuilder(blobMetadata, solrServerMetadata.getGeneration());

        /*
         * Removing from the core metadata the files that are stored on the blob store but no longer needed.
         *
         * When this method is executed, the content of the index on Blob is to be replaced with the local content.
         * The assumption (or normal flow) is for local to refresh from Blob, update locally then push the
         * changes to Blob.
         * When merges happen locally the update has to mark old segment files for delete (and for example the
         * previous "segments_N" is to be deleted as a new higher generation segment has been created).
         */
        for (BlobCoreMetadata.BlobFile d : resolvedMetadataResult.getFilesToDelete()) {
            bcmBuilder.removeFile(d);
            BlobCoreMetadata.BlobFileToDelete bftd = new BlobCoreMetadata.BlobFileToDelete(d, BlobStoreUtils.getCurrentTimeMs());
            bcmBuilder.addFileToDelete(bftd);
        }

        // add the old core.metadata file to delete
        if (!currentMetadataSuffix.equals(SharedShardMetadataController.METADATA_NODE_DEFAULT_VALUE)) {
          // TODO This may be inefficient but we'll likely remove this when CorePushPull is refactored to have deletion elsewhere
          //      could be added to resolvedMetadataResult#getFilesToDelete()
          ToFromJson<BlobCoreMetadata> converter = new ToFromJson<>();
          String json = converter.toJson(blobMetadata);
          int bcmSize = json.getBytes().length;

          String blobCoreMetadataName = BlobStoreUtils.buildBlobStoreMetadataName(currentMetadataSuffix);
          String coreMetadataPath = blobMetadata.getSharedBlobName() + "/" + blobCoreMetadataName;
          // so far checksum is not used for metadata file
          BlobCoreMetadata.BlobFileToDelete bftd = new BlobCoreMetadata.BlobFileToDelete("", coreMetadataPath, bcmSize, BlobCoreMetadataBuilder.UNDEFINED_VALUE, BlobStoreUtils.getCurrentTimeMs());
          bcmBuilder.addFileToDelete(bftd);
        }

        // When we build solrServerMetadata we requested to reserve the commit point for some short duration. Assumption is
        // it took less than this duration to get here (didn't do anything blocking) and now we actually save the commit
        // point for the (potentially long) time it takes to push all files to the Blob store.
        Directory coreIndexDir = solrCore.getDirectoryFactory().get(solrCore.getIndexDir(), DirectoryFactory.DirContext.DEFAULT, solrCore.getSolrConfig().indexConfig.lockType);
        solrCore.getDeletionPolicy().saveCommitPoint(solrServerMetadata.getGeneration());
        
        try {
          // Directory's javadoc says: "Java's i/o APIs not used directly, but rather all i/o is through this API"
          // But this is untrue/totally false/misleading. IndexFetcher has File all over.
          for (CoreFileData cfd : resolvedMetadataResult.getFilesToPush()) {
            // Sanity check that we're talking about the same file (just sanity, Solr doesn't update files so should never be different)
            assert cfd.getFileSize() == coreIndexDir.fileLength(cfd.getFileName());

            String blobPath = pushFileToBlobStore(coreStorageClient, coreIndexDir, cfd.getFileName(), cfd.getFileSize());
            bcmBuilder.addFile(new BlobCoreMetadata.BlobFile(cfd.getFileName(), blobPath, cfd.getFileSize(), cfd.getChecksum()));

            counter.incrementActualFilesTransferred();
            counter.incrementBytesTransferred(cfd.getFileSize());
          }
        } finally {
          solrCore.getDeletionPolicy().releaseCommitPoint(solrServerMetadata.getGeneration());
          solrCore.getDirectoryFactory().release(coreIndexDir);
        }

        // delete what we need
        enqueueForHardDelete(bcmBuilder);

        BlobCoreMetadata newBcm = bcmBuilder.build();

        String blobCoreMetadataName = BlobStoreUtils.buildBlobStoreMetadataName(newMetadataSuffix);
        coreStorageClient.pushCoreMetadata(blobMetadata.getSharedBlobName(), blobCoreMetadataName, newBcm);
        isSuccessful = true;
        return newBcm;
      } finally {
        solrCore.close();

        counter.setExpectedFilesTransferred(resolvedMetadataResult.getFilesToPush().size());
        logBlobAction("PUSH", counter, isSuccessful, startTimeMs, 0, startTimeMs);
      }
    }

    /**
     * Calls {@link #pullUpdateFromBlob(long, boolean, int)}  with current relative time and attempt no. 0.
     * @param waitForSearcher <code>true</code> if this call should wait until the index searcher is created (so that any query
     *                     after the return from this method sees the new pulled content) or <code>false</code> if we request
     *                     a new index searcher to be eventually created but do not wait for it to be created (a query
     *                     following the return from this call might see the old core content).
     */
    public void pullUpdateFromBlob(boolean waitForSearcher) throws Exception {
         pullUpdateFromBlob(BlobStoreUtils.getCurrentTimeMs(), waitForSearcher, 0);
    }

    /**
     * We're doing here what replication does in {@link org.apache.solr.handler.IndexFetcher#fetchLatestIndex(boolean, boolean)}.<p>
     *
     * This method will work in 2 cases:
     * <ol>
     * <li>Local core needs to fetch an update from Blob</li>
     * <li>Local core did not exist (was created empty before calling this method) and is fetched from Blob</li>
     * </ol>
     * 
     * @param requestQueuedTimeMs relative time in milliseconds when the pull request was queued(meaningful in case of async pushing)
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
     *                      TODO This has to be revisited before going to real prod, as environment issues can cause massive reindexing with this strategy
     */
    public void pullUpdateFromBlob(long requestQueuedTimeMs, boolean waitForSearcher, int attempt) throws Exception {
        long startTimeMs = BlobStoreUtils.getCurrentTimeMs();
        boolean isSuccessful = false;
        FileTransferCounter counter = new FileTransferCounter();

        try {
          SolrCore solrCore = container.getCore(pushPullData.getCoreName());
          if (solrCore == null) {
            throw new Exception("Can't find core " + pushPullData.getCoreName());
          }
          // if there is a conflict between local and blob contents we will move the core to a new index directory
          final boolean createNewIndexDir = resolvedMetadataResult.isLocalConflictingWithBlob();
          boolean coreSwitchedToNewIndexDir = false;
          try {
            // Create temp directory (within the core local folder).
            // If we are moving index to a new directory because of conflict then this will be that new directory.
            // Even if we are not moving to a newer directory we will first download files from blob store into this temp directory.
            // Then we will move files from temp directory to index directory. This is to avoid leaving a download half done
            // in case of failure as well as to limit the time during which we close then reopen the index writer to take
            // into account the new files. In theory nothing should be changing the local directory as we pull files from
            // Blob store, but let's be defensive (we're checking further down that local dir hasn't changed in the meantime).
            String tempIndexDirName = "index.pull." + System.nanoTime();
            String tempIndexDirPath = solrCore.getDataDir() + tempIndexDirName;
            Directory tempIndexDir = solrCore.getDirectoryFactory().get(tempIndexDirPath, DirectoryFactory.DirContext.DEFAULT, solrCore.getSolrConfig().indexConfig.lockType);
            try {
              String indexDirPath = solrCore.getIndexDir();
              Collection<BlobFile> filesToDownload;
              if (createNewIndexDir) {
                // This is an optimization to not download everything from blob if possible
                // This made sense for some rolling start scenario in TLOG replicas and makes here too
                // https://issues.apache.org/jira/browse/SOLR-11920
                // https://issues.apache.org/jira/browse/SOLR-11815
                // TODO: We might want to skip this optimization when healing a locally corrupt core
                Directory indexDir = solrCore.getDirectoryFactory().get(indexDirPath, DirectoryFactory.DirContext.DEFAULT, solrCore.getSolrConfig().indexConfig.lockType);
                try {
                  filesToDownload = initializeNewIndexDirWithLocallyAvailableFiles(indexDir, tempIndexDir);
                } finally {
                  solrCore.getDirectoryFactory().release(indexDir);
                }
              } else {
                filesToDownload = resolvedMetadataResult.getFilesToPull();
              }

              counter.setExpectedFilesTransferred(filesToDownload.size());
              downloadFilesFromBlob(tempIndexDir, filesToDownload, counter);

              Directory indexDir = solrCore.getDirectoryFactory().get(indexDirPath, DirectoryFactory.DirContext.DEFAULT, solrCore.getSolrConfig().indexConfig.lockType);
              try {
                if (!createNewIndexDir) {
                  // TODO should we call solrCore.closeSearcher() here? IndexFetcher.fetchLatestIndex() does call it.
                  // Close the index writer to stop changes to this core
                  solrCore.getUpdateHandler().getSolrCoreState().closeIndexWriter(solrCore, true);
                }

                boolean thrownException = false;
                try {
                  // Make sure Solr core directory content hasn't changed since we decided what we want to pull from Blob
                  if (!solrServerMetadata.isSameDirectoryContent(indexDir)) {
                    // Maybe return something less aggressive than throwing an exception? TBD once we end up calling this method :)
                    throw new Exception("Local Directory content " + indexDirPath + " has changed since Blob pull started. Aborting pull.");
                  }

                  if (createNewIndexDir) {
                    // point index to the new directory. Method call below always returns true BTW.
                    coreSwitchedToNewIndexDir = solrCore.modifyIndexProps(tempIndexDirName);
                  } else {
                    moveFilesFromTempToIndexDir(solrCore, tempIndexDir, indexDir);
                  }
                } catch (Exception e) {
                  // Used in the finally below to not mask an exception thrown from the try block above
                  thrownException = true;
                  throw e;
                } finally {
                  try {
                    // TODO this has been observed to throw org.apache.lucene.index.CorruptIndexException on certain types of corruptions in Blob Store. We need to handle this correctly (maybe we already do).
                    if (!createNewIndexDir) {
                      // The closed index writer must be opened back (in the finally bloc)
                      solrCore.getUpdateHandler().getSolrCoreState().openIndexWriter(solrCore);
                    } else if (coreSwitchedToNewIndexDir) {
                      solrCore.getUpdateHandler().newIndexWriter(true);
                    }
                  } catch (IOException ioe) {
                    // TODO corrupt core handling happened here
                    // CorruptCoreHandler.notifyBlobPullFailure(container, coreName, blobMetadata);
                    if (!thrownException) {
                      // Do not mask a previous exception with a more recent one so only throw the new one if none was thrown previously
                      throw ioe;
                    }
                  }
                }
              } finally {
                try {
                  if (coreSwitchedToNewIndexDir) {
                    solrCore.getDirectoryFactory().doneWithDirectory(indexDir);
                    solrCore.getDirectoryFactory().remove(indexDir);
                  }
                } catch (Exception e) {
                  log.warn("Cannot remove previous index directory " + indexDir, e);
                } finally {
                  solrCore.getDirectoryFactory().release(indexDir);
                }
              }
            } finally {
              try {
                if (!coreSwitchedToNewIndexDir) {
                  solrCore.getDirectoryFactory().doneWithDirectory(tempIndexDir);
                  solrCore.getDirectoryFactory().remove(tempIndexDir);
                }
              } catch (Exception e) {
                log.warn("Cannot remove temp directory " + tempIndexDirPath, e);
              } finally {
                solrCore.getDirectoryFactory().release(tempIndexDir);
              }
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
              // TODO corrupt core handling happened here
              // CorruptCoreHandler.notifyBlobPullFailure(container, coreName, blobMetadata);
              throw se;
            }
            isSuccessful = true;
          } finally {
            solrCore.close();
          }
        } finally {
          logBlobAction("PULL", counter, isSuccessful, requestQueuedTimeMs, attempt, startTimeMs);
        }
    }

    private void moveFilesFromTempToIndexDir(SolrCore solrCore, Directory tmpIndexDir, Directory dir) throws IOException {
      // Copy all files into the Solr directory
      // Move the segments_N file last once all other are ok.
      String segmentsN = null;
      for (BlobFile bf : resolvedMetadataResult.getFilesToPull()) {
        if (SharedStoreResolutionUtil.isSegmentsNFilename(bf)) {
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
    }

    private Collection<BlobFile> initializeNewIndexDirWithLocallyAvailableFiles(Directory indexDir, Directory newIndexDir) {
      Collection<BlobFile> filesToDownload = new HashSet<>();
        for (BlobFile blobFile : resolvedMetadataResult.getFilesToPull()) {
          try (final IndexInput indexInput = indexDir.openInput(blobFile.getSolrFileName(), IOContext.READONCE)) {
            long length = indexInput.length();
            long checksum  = CodecUtil.retrieveChecksum(indexInput);
            if (length == blobFile.getFileSize() && checksum == blobFile.getChecksum()) {
              copyFileToDirectory(indexDir, blobFile.getSolrFileName(), newIndexDir);
            } else {
              filesToDownload.add(blobFile);
            }
          } catch (Exception ex){
            // Either file does not exist locally or copy not succeeded, we will download from blob store
            filesToDownload.add(blobFile);
          }
        }
      return filesToDownload;
    }

    /**
     * Pushes a local file to blob store and returns a unique path to newly created blob
     */
    @VisibleForTesting
    protected String pushFileToBlobStore(CoreStorageClient blob, Directory dir, String fileName, long fileSize) throws Exception {
        String blobPath;
        IndexInput ii = dir.openInput(fileName, IOContext.READONCE);
        try (InputStream is = new IndexInputStream(ii)) {
            blobPath = blob.pushStream(blobMetadata.getSharedBlobName(), is, fileSize, fileName);
        }
        return blobPath;
    }

    /**
     * Log for push or pull action 
     */
    private void logBlobAction(String action, FileTransferCounter counter, boolean isSuccessful, long requestQueuedTimeMs, int attempt,
        long startTimeMs) throws Exception {
      long now = BlobStoreUtils.getCurrentTimeMs();
      long runTime = now - startTimeMs;
      long startLatency = now - requestQueuedTimeMs;

      String message = String.format(Locale.ROOT,
            "PushPullData=[%s] action=%s storageProvider=%s bucketRegion=%s bucketName=%s "
              + "runTime=%s startLatency=%s bytesTransferred=%s attempt=%s expectedFilesAffected=%s actualFilesAffected=%s isSuccessful=%s "
              + "localGeneration=%s blobGeneration=%s ",
          pushPullData.toString(), action, coreStorageClient.getStorageProvider().name(), coreStorageClient.getBucketRegion(),
          coreStorageClient.getBucketName(), runTime, startLatency, counter.getBytesTransferred(), attempt, counter.getExpectedFilesTransferred(), 
          counter.getActualFilesTransferred(), isSuccessful, solrServerMetadata.getGeneration(), blobMetadata.getGeneration());
      log.info(message);
    }

    /**
     * Downloads files from the Blob store 
     * @param destDir (temporary) directory into which files should be downloaded.
     * @param filesToDownload blob files to be downloaded
     * @param counter wrapper object around expected and actual number of files downloaded, and number of bytes transferred successfully
     */
    @VisibleForTesting
    protected void downloadFilesFromBlob(Directory destDir, Collection<? extends BlobFile> filesToDownload, FileTransferCounter counter) throws Exception {
      // Synchronously download all Blob blobs (remember we're running on an async thread, so no need to be async twice unless
      // we eventually want to parallelize downloads of multiple blobs, but for the PoC we don't :)
      for (BlobFile bf: filesToDownload) {
        log.info("About to create " + bf.getSolrFileName() + " for core " + pushPullData.getCoreName() +
            " from index on blob " + pushPullData.getSharedStoreName());

          try (IndexOutput io = destDir.createOutput(bf.getSolrFileName(), DirectoryFactory.IOCONTEXT_NO_CACHE);
              OutputStream outStream = new IndexOutputStream(io);
            InputStream bis = coreStorageClient.pullStream(bf.getBlobName())) {
            IOUtils.copy(bis, outStream);

            counter.incrementActualFilesTransferred();
            counter.incrementBytesTransferred(bf.getFileSize());
          }
      }
    }

    /**
     * Copies {@code fileName} from {@code fromDir} to {@code toDir}
     */
    private void copyFileToDirectory(Directory fromDir, String fileName, Directory toDir) throws IOException {
      // TODO: Consider optimizing with org.apache.lucene.store.HardlinkCopyDirectoryWrapper
      toDir.copyFrom(fromDir, fileName, fileName, DirectoryFactory.IOCONTEXT_NO_CACHE);
    }

    /**
     * Moves {@code fileName} from {@code fromDir} to {@code toDir}
     */
    private void moveFileToDirectory(SolrCore solrCore, Directory fromDir, String fileName, Directory toDir) throws IOException {
      // We don't need to keep the original files so we move them over.
      // TODO: Consider optimizing with org.apache.lucene.store.HardlinkCopyDirectoryWrapper
      solrCore.getDirectoryFactory().move(fromDir, toDir, fileName, DirectoryFactory.IOCONTEXT_NO_CACHE);
    }
    
    @VisibleForTesting
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
    
    /**
     * Returns true if a deleted blob file (i.e. a file marked for delete but not deleted yet) can be hard deleted now.
     */
    @VisibleForTesting
    protected boolean okForHardDelete(BlobCoreMetadata.BlobFileToDelete file) {
      // For now we only check how long ago the file was marked for delete.
      return BlobStoreUtils.getCurrentTimeMs() - file.getDeletedAt() >= deleteManager.getDeleteDelayMs();
    }
    
    @VisibleForTesting
    protected boolean enqueueForDelete(String coreName, Set<BlobCoreMetadata.BlobFileToDelete> blobFiles) {
      if (blobFiles == null || blobFiles.isEmpty()) {
        return false;
      }
      Set<String> blobNames = blobFiles.stream()
                                .map(blobFile -> blobFile.getBlobName())
                                .collect(Collectors.toCollection(HashSet::new));
      return deleteManager.enqueueForDelete(coreName, blobNames);
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
    
    /**
     * FileTransferCounter contains counts of expected and actual files transferred, and actual bytes transferred in blob operations
     */
    static class FileTransferCounter {
      private long expectedFilesTransferred;
      private long actualFilesTransferred;
      private long bytesTransferred;
      
      FileTransferCounter() {
        this.expectedFilesTransferred = 0;
        this.actualFilesTransferred = 0;
        this.bytesTransferred = 0;
      }
      
      FileTransferCounter(long expectedFilesTransferred, long actualFilesTransferred, long bytesTransferred) {
        this.expectedFilesTransferred = expectedFilesTransferred;
        this.actualFilesTransferred = actualFilesTransferred;
        this.bytesTransferred = bytesTransferred;
      }
      
      public long getExpectedFilesTransferred() {
        return expectedFilesTransferred;
      }

      public long getActualFilesTransferred() {
        return actualFilesTransferred;
      }
      
      public long getBytesTransferred() {
        return bytesTransferred;
      }
      
      public void incrementActualFilesTransferred() {
        this.actualFilesTransferred++;
      }
      
      public void incrementBytesTransferred(long additionalBytesTransferred) {
        this.bytesTransferred += additionalBytesTransferred;
      }
      
      public void setExpectedFilesTransferred(long expectedFilesTransferred) {
        this.expectedFilesTransferred = expectedFilesTransferred;
      }

      public void setActualFilesTransferred(long actualFilesTransferred) {
        this.actualFilesTransferred = actualFilesTransferred;
      }

      public void setBytesTransferred(long bytesTransferred) {
        this.bytesTransferred = bytesTransferred;
      }
    }
}
