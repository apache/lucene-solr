package org.apache.solr.store.blob.metadata;

import edu.umd.cs.findbugs.annotations.NonNull;
import search.blobstore.solr.BlobCoreMetadata;
import search.blobstore.solr.BlobCoreMetadata.BlobConfigFile;
import search.blobstore.solr.BlobCoreMetadata.BlobFile;
import search.blobstore.solr.BlobCoreMetadataBuilder;
import searchserver.blobstore.metadata.ServerSideCoreMetadata.CoreConfigFileData;
import searchserver.blobstore.metadata.ServerSideCoreMetadata.CoreFileData;

import java.util.*;

/**
 * Class comparing local {@link ServerSideCoreMetadata} and remote {@link BlobCoreMetadata}, and building the plan (when
 * appropriate) of which files to copy over (push) to Blob and which files should be deleted from Blob. Note there's no
 * need to track which files need to be deleted locally from a Solr index (after we pull it from the Blob store) because
 * Solr does it itself.
 *
 * @author iginzburg
 * @since 214/solr.6
 */
public class MetadataResolver {

    private static String SEGMENTS_N_PREFIX = "segments_";

    /**
     * Actions that should be taken to bring the local and Blob cores to parity
     */
    public enum Action {
        /** Local core is fresher. Some files should be pushed to Blob store */
        PUSH,
        /** Blob core is fresher. Some files should be pulled */
        PULL,
        /** There are only config file changes, otherwise, Blob and local cores are equivalent w.r.to indexing files.*/
        CONFIG_CHANGE,
        /** Blob and local cores are equivalent, nothing to do */
        EQUIVALENT,
        /** The two cores do not seem compatible, one of the two should be chosen to overwrite the other */
        CONFLICT,
        /** The Blob core metadata description is incorrect */
        BLOB_CORRUPT,
        /** The core is marked as deleted in Blob */
        BLOB_DELETED
    }

    private final Action action;

    /** Files to push. To eventually be made immutable so API contract is clearer. */
    private Map<String, CoreFileData> localFilesMissingOnBlob = new HashMap<>();
    /** Files to pull. To eventually be made immutable so API contract is clearer. */
    private Map<String, BlobFile> blobFilesMissingLocally = new HashMap<>();
    
    /** Config files to push. To eventually be made immutable so API contract is clearer. */
    private Map<String, CoreConfigFileData> localConfigFilesMissingOnBlob = new HashMap<>();
    /** Config files to pull. To eventually be made immutable so API contract is clearer. */
    private Map<String, BlobConfigFile> blobConfigFilesMissingLocally = new HashMap<>();

    /** REPLACE or BLOB_CORRUPT actions set an error message explaining the reason. Undefined otherwise. */
    private String errorMessage;

    /** Make sure we don't try to use twice the same instance, it's not meant for this */
    private boolean resolveDone = false;

    // TODO data structures that capture what has to be done (action and parameters for the action...)

    /**
     * Returns the set of files to push to bring the distant (Blob) core version in sync with the local core, or null
     * if not possible (i.e. Blob is fresher or the two cores can't be reconciled).
     */
    public MetadataResolver(ServerSideCoreMetadata local, BlobCoreMetadata distant) {
        action = resolve(local, distant);
    }

    public Action getAction() {
        return this.action;
    }

    /**
     * @return a <code>null</code> string or a human readable explanation of an issue.
     */
    public String getMessage() {
        return (action == Action.CONFLICT || action == Action.BLOB_CORRUPT) ? errorMessage : null;
    }

    /**
     * Returns the local files that need to be pushed to the Blob store (new segments_N included)
     */
    public Collection<CoreFileData> getFilesToPush() {
        if (action != Action.PUSH && action != Action.BLOB_CORRUPT) {
            throw new IllegalStateException("No files to push to blob when action is " + action);
        }
        return localFilesMissingOnBlob.values();
    }
    
    /**
     * Returns the config files that need to be pushed to the Blob store
     */
    public Collection<CoreConfigFileData> getConfigFilesToPush() {
        if (action != Action.PUSH && action != Action.BLOB_CORRUPT && action != Action.CONFIG_CHANGE && action != Action.PULL) {
            throw new IllegalStateException("No config files to push to blob when action is " + action);
        }
        return localConfigFilesMissingOnBlob.values();
    }

    /**
     * Returns the blob files that need to be marked for delete as the new version of the core is pushed to Blob
     * (old segments_N blob included).
     */
    Collection<BlobFile> getBlobFilesToDelete() {
        if (action != Action.PUSH) {
            throw new IllegalStateException("No files to delete on blob when action is " + action);
        }
        return blobFilesMissingLocally.values();
    }

    /**
     * Returns the blob files that need to be pulled into local core to bring it up to date, including the Blob's segments_N file.<p>
     * Note there is no list of local files to delete when pulling from blob (as opposed to when pushing to Blob) because
     * Solr itself manages the local files that need to go away.
     */
    public Map<String, BlobFile> getFilesToPull() {
        if (action != Action.PULL) {
            throw new IllegalStateException("No files to pull to blob when action is " + action);
        }
        return blobFilesMissingLocally;
    }
    
    /**
     * Returns the blob config files that need to be pulled into local core to bring it up to date
     */
    public Map<String, BlobConfigFile> getConfigFilesToPull() {
        if (action != Action.PULL && action != Action.CONFIG_CHANGE && action != Action.PUSH) {
            throw new IllegalStateException("No config files to pull to blob when action is " + action);
        }
        return blobConfigFilesMissingLocally;
    }

    /** Identify the segments_N file in Blob files. */
    boolean isSegmentsNFilename(@NonNull BlobFile bf) {
        return bf.getSolrFileName().startsWith(SEGMENTS_N_PREFIX);
    }

    /**
     * Returns the appropriate action resulting from the comparison of the local core and the blob core.
     */
    Action resolve(ServerSideCoreMetadata local, BlobCoreMetadata distant) {
        if (resolveDone) {
            throw new IllegalStateException("Each instance of MetadataResolver can only be used once.");
        }
        resolveDone = true;

        if (local == null && distant == null) { 
            throw new IllegalStateException("Cannot resolve if both local and remote metadata is null"); 
        }

        if (local != null) {
            // Prepare local files for lookup by file name
            for (CoreFileData cfd : local.getFiles()) {
                localFilesMissingOnBlob.put(cfd.fileName, cfd);
            }
            
            // Prepare local config files for lookup by file name
            for (CoreConfigFileData cfd : local.getConfigFiles()) {
                localConfigFilesMissingOnBlob.put(cfd.fileName, cfd);
            }
            // TODO we're not dealing here with local core on Solr server being corrupt. Not part of PoC at this stage but eventually need a solution
            // (fetch from Blob unless Blob corrupt as well...)
        }

        if (distant == null
                || (distant.getSequenceNumber() == BlobCoreMetadataBuilder.UNDEFINED_VALUE
                    && distant.getGeneration() == BlobCoreMetadataBuilder.UNDEFINED_VALUE
                    && distant.getBlobFiles().length == 0
                    && distant.getBlobFilesToDelete().length == 0)) {
            // Core does not exist on blob. All we can do is push it there.
            // We've computed localFilesMissingOnBlob above, and blobFilesMissingLocally is empty as it should be.
            return Action.PUSH;
        }

        if (distant.getIsDeleted()) { 
            return Action.BLOB_DELETED;
		}

        // The Blob Store core could have explicitly been marked as corrupt by a search server running into issues with
        // the core content. If that's the case, do not pull it...
        // TODO at this point we might want to trigger something so we end up pushing our version of the core to the Blob store if we have a working copy?
        if (distant.getIsCorrupt()) {
            return Action.BLOB_CORRUPT;
        }
        
        // Verify we find one and only one segments_N file to download from Blob.
        String segmentsN = null;

        // Keep track of Blob files that would need to be marked for delete
        for (BlobFile bf : distant.getBlobFiles()) {
            if (isSegmentsNFilename(bf)) {
                if (segmentsN != null) {
                    errorMessage = "Blob store for core " + distant.getCoreName() + " has conflicting files "
                            + segmentsN + " and " + bf.getSolrFileName();
                    // As we return here, blobFilesMissingLocally will not be accessible and localFilesMissingOnBlob does
                    // contain all local files of latest commit point so we're ok.
                    // TODO: iginzburg March 1st 2018 unclear why the comment above. We shouldn't care about local files
                    return Action.BLOB_CORRUPT;
                } else {
                    segmentsN = bf.getSolrFileName();
                }
            }
            blobFilesMissingLocally.put(bf.getSolrFileName(), bf);
        }

        if (segmentsN == null) {
            errorMessage = "Blob store for core " + distant.getCoreName() + " does not contain a segments_N file";
            // As above, blobFilesMissingLocally not accessible and localFilesMissingOnBlob ok
            return Action.BLOB_CORRUPT;
        }

        for (BlobConfigFile bf : distant.getBlobConfigFiles()) {
            blobConfigFilesMissingLocally.put(bf.getSolrFileName(), bf);
        }

        if (local == null) {
            // Core does not exist locally. All we can do is pull it from blob
            // We've computed blobFilesMissingLocally and localFilesMissingOnBlob is empty as it should be.
            return Action.PULL;
        }

        // Verify there are no inconsistencies between local (core) and blob index files
        for (BlobFile bf : distant.getBlobFiles()) {
            // We remove from map of local files those already present remotely since they don't have to be pushed.
            CoreFileData cf = localFilesMissingOnBlob.remove(bf.getSolrFileName());
            if (cf != null) {
                // The blob file is present locally (by virtue of having been in localFilesMissingOnBlob initialized with
                // all local files). Check if there is a conflict between local and distant (blob) versions of that file.
                blobFilesMissingLocally.remove(bf.getSolrFileName());
                // Later we could add checksum verificiation etc. here
                if (cf.fileSize != bf.getFileSize()) {
                    errorMessage = "Size conflict. File " + bf.getSolrFileName() + " (Blob name " + bf.getBlobName() + ") local size " + cf.fileSize
                            + " blob size " + bf.getFileSize() + " (core " + local.getCoreName() + ")";
                    return Action.CONFLICT;
                }
            }
        }

        // Reconcile config files that need to be pushed/pulled
        // Unlike index files, it is valid to have both config files to be pushed and pulled at the same time
        // e.g. Server1 creates synonym_en.txt and hangs before updating blob. Failover happens and Server2 creates synonym_fr.txt and
        // get an indexing batch and pushes it to blob. Server1 comes back and find itself behind blob(because of indexing on Server2) and gets a pull. 
        // But at that point it was also valid to push synonym_en.txt along with pulling synonym_fr.txt and segment files.
        // The important point to note is that config file updates do not cause sequence number and generation number to change.
        // Therefore, unlike index files it is valid for config files to flow in both directions in single sync.
        //
        // Although for now we only:
        // push config files: 1.when we push indexing updates (Action#PUSH)
        //                    2.when we just push config files by themselves (Action#CONFIG_CHANGE),
        //                      so far, we have decided not to do its counter part in pull.
        // pull config files: 3.when we pull indexing updates (Action#PULL)
        //
        // Additionally, when a config file(18 synonym_<lang>.txt files and 1 elevate.xml) is created it can become blank
        // but it not deleted(until unless it is corrupt). Therefore, we are not in the business of deleting config files
        // when syncing with blob. And presence of config file on one side(local or blob) means it is fresher.
        // https://docs.google.com/document/d/1o3njbMQQcFZ_TfvzKVun78NE-eNHita9zdnQfa_611I
        for (BlobConfigFile bcf : distant.getBlobConfigFiles()) {
            CoreConfigFileData lcf = localConfigFilesMissingOnBlob.get(bcf.getSolrFileName());
            if (lcf != null) {
                if (lcf.updatedAt == bcf.getUpdatedAt()) {
                    // If there is no difference in config file no need to push/pull
                    blobConfigFilesMissingLocally.remove(bcf.getSolrFileName());
                    localConfigFilesMissingOnBlob.remove(bcf.getSolrFileName());
                } else if (lcf.updatedAt > bcf.getUpdatedAt()) {
                    blobConfigFilesMissingLocally.remove(bcf.getSolrFileName());
                } else {
                    localConfigFilesMissingOnBlob.remove(bcf.getSolrFileName());
                }
            }
        }

        if (local.getSequenceNumber() == distant.getSequenceNumber() && local.getGeneration() == distant.getGeneration()) {
            // Unlike indexing update, config file updates are second class citizens and do not cause sequence number and generation number to change.
            // Therefore, if we have any config files to push or pull we can still process them without involving indexing updates
            if(!localConfigFilesMissingOnBlob.isEmpty() || !blobConfigFilesMissingLocally.isEmpty()){
                return  Action.CONFIG_CHANGE;
            }
            return Action.EQUIVALENT;
        } else if (local.getSequenceNumber() > distant.getSequenceNumber() || (local.getSequenceNumber() == distant.getSequenceNumber() && local.getGeneration() > distant.getGeneration())) {
            // Local core is fresher than blob image
            // We need to push to blob localFilesMissingOnBlob and to mark for delete from blob blobFilesMissingLocally
            // These sets include the segments_N files to push and delete.
            // This is a rather straightforward file push to blob
            return Action.PUSH;
        } else {
            // Blob image is more up to date than local core.
            // We need to pull from blob blobFilesMissingLocally which include the blob's segments_N (that needs to be pulled last).
            // Look at org.apache.solr.handler.SnapPuller.fetchLatestIndex() to see how it's done, we likely need to do
            // something similar
            return Action.PULL;
        }
    }
}
