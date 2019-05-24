package org.apache.solr.store.blob.process;

import searchserver.replication.OrphanedCoreDeleter;

/**
 * Enumerates all possible results of core sync tasks
 * 
 * @author iginzburg
 * @since 214/solr.6
 */
public enum CoreSyncStatus {
    /** Core pushed/pulled successfully to/from Blob store */
    SUCCESS(true, false),
    /** Config files pushed successfully to Blob store. There were no indexing files involved in the push */
    SUCCESS_CONFIG(true, false),
    /** There were some configs files to be pulled but no indexing files involved. 
     * So far we have decided not to pull config files alone. 
     * If/when we decide to do that we will use {@link #SUCCESS_CONFIG} and get rid of this.
     * */
    SKIP_CONFIG(true, false),
    /** There was no need to push/pull the core to/from the blob store */
    SUCCESS_EQUIVALENT(true, false),
    /** Core failed to pull from or push to Blob store because of "local" error in case of push (likely logged earlier) and unknown in case of pull */
    FAILURE(false, true),
    /** Core was not found locally when pulling. Consider we're fine (success) and let BlobCoreSyncer handle this. TODO revisit? */
    LOCAL_MISSING_FOR_PULL(true, false),
    /** Core was not found locally when we were expecting to push updates to Blob Store.
     * We have to fail because nothing to push */
    LOCAL_MISSING_FOR_PUSH(false, false),
    /** Core was not pulled from blob because it was not found on blob */
    BLOB_MISSING(false, false),
    /** Core was not pushed/pulled because Blob version conflicts with local version */
    BLOB_CONFLICT(false, false),
    /** Core was not pushed/pulled because core corrupted on Blob */
    BLOB_CORRUPT(false, false),
    /** Core was not pushed because Blob is deleted, likely local copy will be eventually cleaned up by {@link OrphanedCoreDeleter} */
    BLOB_DELETED_FOR_PUSH(true, false),
    /** Core was not pulled because Blob is deleted */
    BLOB_DELETED_FOR_PULL(false, false),
    /** Core was not pushed because Blob version more up to date */
    BLOB_FRESHER(true, false),
    /** No attempt to push/pull the core was made because another task was working on it */
    CONCURRENT_SYNC(false, true),
    /** No attempt to push/pull the core was made as system was shutting down */
    SHUTTING_DOWN(false, false);

    final private boolean isTransientError;
    final private boolean isSuccess;

    CoreSyncStatus(boolean isSuccess, boolean isTransientError) {
        assert !(isSuccess && isTransientError);
        this.isSuccess = isSuccess;
        this.isTransientError = isTransientError;
    }

    /**
     * @return <code>true</code> when it makes sense to retry running the same task again, i.e. when the task has failed
     *         and when the cause of the failure might disappear by itself without any specific actions.<br>
     *         It is accepted by most that it does not make sense to retry an action when it is known the cause of
     *         failure will not go away by itself.<br>
     *         It is also generally agreed upon that even if the cause of the failure <b>might</b> go away by itself, it
     *         is likely a bad idea to retry forever.
     */
    public boolean isTransientError() {
        return isTransientError;
    }

    /**
     * @return <code>true</code> if content was successfully synced from Blob OR if no sync was required.
     */
    public boolean isSuccess() {
        return isSuccess;
    }
}
