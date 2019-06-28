package org.apache.solr.store.blob.metadata;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.solr.common.SolrException;
import org.apache.solr.store.blob.client.BlobCoreMetadata;
import org.apache.solr.store.blob.client.BlobCoreMetadata.BlobFile;
import org.apache.solr.store.blob.metadata.ServerSideMetadata.CoreFileData;

/**
 * Utility class used to compare local {@link ServerSideMetadata} and remote 
 * {@link BlobCoreMetadata}, metadata of shard index data on the local solr node
 * and remote shared store (blob).
 */
public class SharedStoreResolutionUtil {  
  private static String SEGMENTS_N_PREFIX = "segments_";

  public static class SharedMetadataResolutionResult {
    // local files missing on blob
    private final Collection<CoreFileData> filesToPush;
    // blob files needed to be pulled
    private final Collection<BlobFile> filesToPull;
    
    public SharedMetadataResolutionResult(Collection<CoreFileData> localFilesMissingOnBlob, 
        Collection<BlobFile> blobFilesMissingLocally) {
      if (localFilesMissingOnBlob == null) {
        this.filesToPush = Collections.emptySet();
      } else {
        this.filesToPush = localFilesMissingOnBlob;
      }
      
      if (blobFilesMissingLocally == null) {
        this.filesToPull = Collections.emptySet();
      } else {
        this.filesToPull = blobFilesMissingLocally;
      }
    }
    
    public SharedMetadataResolutionResult(Map<String, CoreFileData> localFilesMissingOnBlob, 
        Map<String, BlobFile> blobFilesMissingLocally) {
      this(localFilesMissingOnBlob.values(), blobFilesMissingLocally.values());
    }
    
    public Collection<CoreFileData> getFilesToPush() {
      return filesToPush;
    }
    
    public Collection<BlobFile> getFilesToPull() {
      return filesToPull;
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
    
    if (local == null && distant == null) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Cannot resolve if both local and remote metadata is null"); 
    }
    
    if (local != null) {
      // Prepare local files for lookup by file name
      for (CoreFileData cfd : local.getFiles()) {
          localFilesMissingOnBlob.put(cfd.fileName, cfd);
      }
      // TODO we're not dealing here with local core on Solr server being corrupt. Not part of PoC at this stage but eventually need a solution
      // (fetch from Blob unless Blob corrupt as well...)
    }
    
    if (distant == null
        || distant.getBlobFiles().length == 0) {
      // The shard index data does not exist on the shared store. All we can do is push. 
      // We've computed localFilesMissingOnBlob above, and blobFilesMissingLocally is empty as it should be.
      return new SharedMetadataResolutionResult(localFilesMissingOnBlob, blobFilesMissingLocally);
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
      // The shard index data does not exist on the shared store. All we can do is pull.  
      // We've computed blobFilesMissingLocally and localFilesMissingOnBlob is empty as it should be.
      return new SharedMetadataResolutionResult(localFilesMissingOnBlob, blobFilesMissingLocally);
    }
    
    // Verify there are no inconsistencies between local index and blob index files
    for (BlobFile bf : distant.getBlobFiles()) {
      // We remove from map of local files those already present remotely since they don't have to be pushed.
      CoreFileData cf = localFilesMissingOnBlob.remove(bf.getSolrFileName());
      if (cf != null) {
        // The blob file is present locally (by virtue of having been in localFilesMissingOnBlob initialized with
        // all local files). Check if there is a conflict between local and distant (blob) versions of that file.
        blobFilesMissingLocally.remove(bf.getSolrFileName());
        // Later we could add checksum verification etc. here
        if (cf.fileSize != bf.getFileSize()) {
          // TODO - for now just log and propagate the error up
          String errorMessage = "Size conflict for shared shard name: " + distant.getSharedBlobName() +". File " + 
              bf.getSolrFileName() + " (Blob name " + bf.getBlobName() + ") local size " + 
              cf.fileSize + " blob size " + bf.getFileSize() + " (core " + local.getCoreName() + ")";
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, errorMessage);
        }
      }
    }
    return new SharedMetadataResolutionResult(localFilesMissingOnBlob, blobFilesMissingLocally);
  }
  
  /** Identify the segments_N file in Blob files. */
  protected static boolean isSegmentsNFilename(BlobFile bf) {
    return bf.getSolrFileName().startsWith(SEGMENTS_N_PREFIX);
  }
}
