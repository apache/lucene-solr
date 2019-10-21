package org.apache.solr.store.blob.metadata;

import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.solr.common.SolrException;
import org.apache.solr.store.blob.client.BlobCoreMetadata;
import org.apache.solr.store.blob.client.BlobCoreMetadata.BlobFile;
import org.apache.solr.store.blob.metadata.ServerSideMetadata.CoreFileData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class used to compare local {@link ServerSideMetadata} and remote 
 * {@link BlobCoreMetadata}, metadata of shard index data on the local solr node
 * and remote shared store (blob).
 */
public class SharedStoreResolutionUtil {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static String SEGMENTS_N_PREFIX = "segments_";

  public static class SharedMetadataResolutionResult {
    // local files missing on blob
    private final Collection<CoreFileData> filesToPush;
    // blob files needed to be pulled
    private final Collection<BlobFile> filesToPull;
    // blob files needed to be deleted
    private final Collection<BlobFile> filesToDelete;
    // Whether the local index contents conflict with contents to be pulled from blob. If they do we will move the
    // core to new index dir when pulling blob contents
    // Two cases:
    //  1. local index is at higher generation number than blob's generation number
    //  2. same index file exist in both places with different size/checksum
    private final boolean localConflictingWithBlob;
    
    
    public SharedMetadataResolutionResult(Collection<CoreFileData> filesToPush, 
        Collection<BlobFile> filesToPull, Collection<BlobFile> filesToDelete, boolean localConflictingWithBlob) {
      if (filesToPush == null) {
        this.filesToPush = Collections.emptySet();
      } else {
        this.filesToPush = filesToPush;
      }
      
      if (filesToPull == null) {
        this.filesToPull = Collections.emptySet();
      } else {
        this.filesToPull = filesToPull;
      }

      if (filesToDelete == null) {
        this.filesToDelete = Collections.emptySet();
      } else {
        this.filesToDelete = filesToDelete;
      }

      this.localConflictingWithBlob = localConflictingWithBlob;
    }
    
    public Collection<CoreFileData> getFilesToPush() {
      return filesToPush;
    }
    
    public Collection<BlobFile> getFilesToPull() {
      return filesToPull;
    }

    public Collection<BlobFile> getFilesToDelete() {
      return filesToDelete;
    }

    public boolean isLocalConflictingWithBlob(){
      return localConflictingWithBlob;
    }
  }
  
  private SharedStoreResolutionUtil() {}
  
  /**
   * Simply resolves the differences between metadata of {@link ServerSideMetadata} and {@link BlobCoreMetadata},
   * returning an instance of {@link SharedMetadataResolutionResult}. SharedStoreResolutionResult contains the listing files that
   * either need to be pushed or pull, without prescribing what action is needed to be taken. It is up to the caller to make
   * that decision. 
   * 
   * @param local the shard metadata located on the solr node
   * @param distant the shard metadata located on the shared store provider
   */
  public static SharedMetadataResolutionResult resolveMetadata(ServerSideMetadata local, BlobCoreMetadata distant) {
    Map<String, CoreFileData> localFilesMissingOnBlob = new HashMap<>();
    Map<String, BlobFile> blobFilesMissingLocally = new HashMap<>();
    Map<String, CoreFileData> allLocalFiles = new HashMap<>();
    
    if (local == null && distant == null) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Cannot resolve if both local and remote metadata is null"); 
    }
    
    if (local != null) {
      // Prepare local files for lookup by file name
      
      // for files to push only the current commit point matters
      for (CoreFileData cfd : local.getLatestCommitFiles()) {
          localFilesMissingOnBlob.put(cfd.getFileName(), cfd);
      }

      // for files to pull all the index files present locally matters
      for (CoreFileData cfd : local.getAllCommitsFiles()) {
        allLocalFiles.put(cfd.getFileName(), cfd);
      }
      // TODO we're not dealing here with local core on Solr server being corrupt. Not part of PoC at this stage but eventually need a solution
      // (fetch from Blob unless Blob corrupt as well...)
    }
    
    if (distant == null
        || distant.getBlobFiles().length == 0) {
      // The shard index data does not exist on the shared store. All we can do is push. 
      // We've computed localFilesMissingOnBlob above, and blobFilesMissingLocally is empty as it should be.
      return new SharedMetadataResolutionResult(localFilesMissingOnBlob.values(), blobFilesMissingLocally.values(), blobFilesMissingLocally.values(), false);
    }
    
    // Verify we find one and only one segments_N file to download from Blob.
    String segmentsN = null;

    for (BlobFile bf : distant.getBlobFiles()) {
      if (isSegmentsNFilename(bf)) {
        if (segmentsN != null) {
          // TODO - for now just log and propagate the error up, this class shouldn't do corruption checking now
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Blob store for shard shared name " + 
              distant.getSharedBlobName() + " has conflicting files " + segmentsN + " and " + bf.getSolrFileName());
        } else {
          segmentsN = bf.getSolrFileName();
        }
      }
      blobFilesMissingLocally.put(bf.getSolrFileName(), bf);
    }
    
    if (segmentsN == null) {
      // TODO - for now just log and propagate the error up, this class shouldn't do corruption checking now
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Blob store for shard shared name " + 
          distant.getSharedBlobName() + " has missing segments file");
    }
    
    if (local == null) {
      // The shard index data does not exist locally. All we can do is pull.  
      // We've computed blobFilesMissingLocally and localFilesMissingOnBlob is empty as it should be.
      return new SharedMetadataResolutionResult(localFilesMissingOnBlob.values(), blobFilesMissingLocally.values(), blobFilesMissingLocally.values(), false);
    }

    boolean localConflictingWithBlob = false;
    // Verify there are no inconsistencies between local index and blob index files
    for (BlobFile bf : distant.getBlobFiles()) {
      // We remove from map of local files those already present remotely since they don't have to be pushed.
      localFilesMissingOnBlob.remove(bf.getSolrFileName());
      CoreFileData cf = allLocalFiles.get(bf.getSolrFileName());
      if (cf != null) {
        // The blob file is present locally. Check if there is a conflict between local and distant (blob) versions of that file.
        blobFilesMissingLocally.remove(bf.getSolrFileName());
        if (cf.getFileSize() != bf.getFileSize() || cf.getChecksum() != bf.getChecksum()) {
          String message = String.format("Size/Checksum conflicts sharedShardName=%s coreName=%s fileName=%s blobName=%s" +
                  " localSize=%s blobSize=%s localChecksum=%s blobCheckSum=%s",
              distant.getSharedBlobName(), local.getCoreName(), bf.getSolrFileName(), bf.getBlobName(),
              cf.getFileSize(), bf.getFileSize(), cf.getChecksum(), bf.getChecksum());
          log.info(message);
          localConflictingWithBlob = true;
        }
      }
    }

    if(!localConflictingWithBlob) {
      // If local index generation number is higher than blob even than we will declare a conflict.
      // Since in the presence of higher generation number locally, blob contents cannot establish their legitimacy.
      localConflictingWithBlob = local.getGeneration() > distant.getGeneration();
    }
    // If there is a conflict we will switch index to a newer directory and pull all blob files.
    // Later in the pipeline at the actual time of pull(CorePushPull#pullUpdateFromBlob) there is an optimization to make use of local index directory
    // for already available files instead of downloading from blob. It was possible to design that into the contract of this 
    // resolver to produce list of files to be pulled from blob and list of files to be pulled(read copied) from local index directory.
    // But that would have unnecessarily convoluted the design of this resolver.
    Collection<BlobFile> filesToPull = localConflictingWithBlob ? Arrays.asList(distant.getBlobFiles()) : blobFilesMissingLocally.values();
    return new SharedMetadataResolutionResult(localFilesMissingOnBlob.values(), filesToPull, blobFilesMissingLocally.values(), localConflictingWithBlob);
  }
  
  /** Identify the segments_N file in Blob files. */
  protected static boolean isSegmentsNFilename(BlobFile bf) {
    return bf.getSolrFileName().startsWith(SEGMENTS_N_PREFIX);
  }
}
