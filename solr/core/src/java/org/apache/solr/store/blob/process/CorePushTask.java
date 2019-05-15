package org.apache.solr.store.blob.process;

import java.util.HashMap;

import org.apache.lucene.index.IndexNotFoundException;
import org.apache.solr.core.CoreContainer;

import com.google.common.collect.Maps;

import org.apache.solr.store.blob.client.CoreStorageClient;
import org.apache.solr.store.blob.client.BlobCoreMetadata;
import org.apache.solr.store.blob.client.BlobCoreMetadataBuilder;
import org.apache.solr.store.blob.metadata.*;
import org.apache.solr.store.blob.provider.BlobStorageProvider;
import org.apache.solr.store.blob.util.DeduplicatingList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.lang.invoke.MethodHandles;

/**
 * Code for pushing updates on a specific core to the Blob store. This class does not implement {@link Runnable} because
 * launching execution on it is very specific to {@link CorePusherThread}.<p>
 *
 * A callback is used (by {@link CorePusherFeeder}) to handle the corresponding work item (consider done, reenqueue,
 * accept a permanent failure).<p>
 *
 * @author iginzburg
 * @since 214/solr.6
 */
public class CorePushTask implements DeduplicatingList.Deduplicatable<String> {
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    /**
     * Minimum delay between to push retries for a given core.<p>
     * This doesn't really belong here in theory, but for now it does since it's a simpler implementation
     * (see comments around where this is used)
     */
    private static final long MIN_RETRY_DELAY_MS = 10000;

    /** Cores currently being pushed and timestamp of push start (to identify stuck ones in logs) */
    private static final HashMap<String, Long> pushesInFlight = Maps.newHashMap();

    private final CoreContainer coreContainer;
    private final String coreName;
    private final long queuedTimeMs;
    private int attempts;
    private long lastAttemptTimestamp;
    private final PushCoreCallback callback;

    CorePushTask(CoreContainer coreContainer, String coreName, PushCoreCallback callback) {
        this(coreContainer, coreName, System.currentTimeMillis(), 0, 0L, callback);
    }

    private CorePushTask(CoreContainer coreContainer, String coreName, long queuedTimeMs, int attempts, long lastAttemptTimestamp , PushCoreCallback callback) {
        this.coreContainer = coreContainer;
        this.coreName = coreName;
        this.queuedTimeMs = queuedTimeMs;
        this.attempts = attempts;
        this.lastAttemptTimestamp = lastAttemptTimestamp;
        this.callback = callback;
    }

    /**
     * Needed for the {@link CorePushTask} to be used in a {@link DeduplicatingList}.
     */
    @Override
    public String getDedupeKey() {
        return this.coreName;
    }

    /**
     * Needed for the {@link CorePushTask} to be used in a {@link DeduplicatingList}.
     */
    static class PushTaskMerger implements DeduplicatingList.Merger<String, CorePushTask> {
        /**
         * Given two tasks (that have not yet started executing!) that target the same core (and would basically
         * do the same things were they both executed), returns a merged task that can replace both and that retains the
         * oldest enqueue time and the smallest number of attempts, so we don't "lose" retries because of the
         * merge yet we correctly report that tasks might have been waiting for execution for a long while.
         * @return a merged {@link CorePushTask} that can replace the two tasks passed as parameters.
         */
        @Override
        public CorePushTask merge(CorePushTask task1, CorePushTask task2) {
            // The asserts below are not guaranteed by construction but we know that's the case
            assert task1.coreContainer == task2.coreContainer;
            assert task1.callback == task2.callback;

            int mergedAttempts;
            long mergedLatAttemptsTimestamp;

            // Synchronizing on the tasks separately to not risk deadlock (even though in practice there's only one concurrent
            // call to this method anyway since it's called from DeduplicatingList.addDeduplicated() and we syncrhonize on the
            // list there).
            synchronized (task1) {
                mergedAttempts = task1.attempts;
                mergedLatAttemptsTimestamp = task1.lastAttemptTimestamp;
            }

            synchronized (task2) {
                // We allow more opportunities to try as the core is changed again by Solr...
                mergedAttempts = Math.min(mergedAttempts, task2.attempts);
                // ...and base the delay computation on the time of last attempt
                mergedLatAttemptsTimestamp = Math.max(mergedLatAttemptsTimestamp, task2.lastAttemptTimestamp);
            }

            // We merge the tasks.
            return new CorePushTask(task1.coreContainer, task1.coreName, Math.min(task1.queuedTimeMs, task2.queuedTimeMs),
                    mergedAttempts, mergedLatAttemptsTimestamp, task1.callback);
        }
    }

    public synchronized void setAttempts(int attempts) {
        this.attempts = attempts;
    }

    public synchronized int getAttempts() {
        return this.attempts;
    }

    synchronized void setLastAttemptTimestamp(long lastAttemptTimestamp) {
        this.lastAttemptTimestamp = lastAttemptTimestamp;
    }

    /**
     * This method is only used in this class for now because the "reenqueue with delay" implementation is imperfect.
     * Longer term, such a reenqueue should be handled outside this class.
     */
    synchronized long getLastAttemptTimestamp() {
        return this.lastAttemptTimestamp;
    }

    public synchronized String getCoreName() {
        return this.coreName;
    }

    /**
     * Pushes the local core updates to the Blob store then calls the task callback to notify the {@link CorePusherFeeder}
     * of success or failure of the operation, give an indication of the reason the the periodic pusher can decide to retry
     * or not.<p>
     *
     * Note there is a hacky implementation of observing a delay between retries (related to MIN_RETRY_DELAY_MS) that likely
     * needs to be cleaned up at some point.
     */
    void pushCoreToBlob() throws InterruptedException {
        if (coreContainer.isShutDown()) {
            this.callback.finishedPush(this, CoreSyncStatus.SHUTTING_DOWN, null);
            // TODO could throw InterruptedException here or interrupt ourselves if we wanted to signal to CorePusherThread to stop everything.
            return;
        }

        synchronized (pushesInFlight) {
            Long pushInFlightTimestamp = pushesInFlight.get(getCoreName());
            if (pushInFlightTimestamp != null) {
                // Another push is in progress, we'll retry later.
                // Note we can't just cancel this push, because the other push might be working on a previous commit point.
                long prevPushMs = System.currentTimeMillis() - pushInFlightTimestamp;
                logger.warn("Skipping core push for " + getCoreName() + " because another thread is currently pushing it (started " + prevPushMs + " ms ago). Will retry.");
                this.callback.finishedPush(this, CoreSyncStatus.CONCURRENT_SYNC, null);
                return;
            } else {
                pushesInFlight.put(getCoreName(), System.currentTimeMillis());
            }
        }

        // Copying the non final variables so we're clean wrt the Java memory model and values do not change as we go
        // (even though we know that no other thread can be working on this CorePushTask when we handle it here).
        final int attemptsCopy = getAttempts();
        final long lasAttemptTimestampCopy = getLastAttemptTimestamp();

        // Retry management is hacky for now: when retrying, if the previous retry was too recent, sleep
        // for a while. Given that retries are added to the tail of the queue, if they get to execute quickly afterwards,
        // it means the queue is not very full and delaying one thread is ok.
        //
        // Longer term, we could have the CorePusherThread try to fetch from a new delay queue first and if no work item
        // is ready there, go fetch from the main queue. When a processed item needs replay, it is inserted back into the
        // MAIN queue (not the delay queue), and if the system is loaded, by the time it gets processed the delay would
        // have passed and there's no need to use the delay queue.
        // If the delay has not expired as the task is processed (the case that makes us sleep() below), only then the
        // task is inserted into the delay queue.
        // In order to protect against all threads waiting on an empty main queue and none processing items from the delay
        // queue, a DeduplicatingList.removeFirst(timeout) will be introduced, and the timeout will be set by the threads
        // to be after the expiration of the waiting item in the delay queue (if there's one). That way if no new tasks
        // arrive in the main queue, the thread will go process the item from the delay queue.
        // In case no item in the delay queue is waiting, the thread can block indefinitely on the main queue (as currently
        // done). If a future item makes it into the delay queue, it will have been inserted by some worker thread (CorePusherThread)
        // and as that thread goes to process its next work item, it will set a timeout to eventually process the delay queue.
        // The above improvement is captured in W-4659893.
        if (attemptsCopy != 0) {
            long now = System.currentTimeMillis();
            if (now - lasAttemptTimestampCopy < MIN_RETRY_DELAY_MS) {
                Thread.sleep(MIN_RETRY_DELAY_MS - now + lasAttemptTimestampCopy);
            }
        }

        try {
            CoreSyncStatus statusToReportWithSuccessfulPush = CoreSyncStatus.SUCCESS;
            try {
                // Do the sequence of actions required to push a core to the Blob store.
                ServerSideCoreMetadata serverMetadata = new ServerSideCoreMetadata(coreName, coreContainer);

                CoreStorageClient blobClient = BlobStorageProvider.get().getBlobStorageClient();
                
                BlobCoreMetadata blobMetadata = blobClient.pullCoreMetadata(serverMetadata.getCoreName());
                if (blobMetadata == null) {
                    blobMetadata = BlobCoreMetadataBuilder.buildEmptyCoreMetadata(serverMetadata.getCoreName());
                    logger.info("BlobCoreMetadata does not exist on the BlobStore. Pushing a new Metadata object.");
                }
                
                MetadataResolver resolver = new MetadataResolver(serverMetadata, blobMetadata);

                // Resolver message, can be null.
                final String message = resolver.getMessage();

                switch (resolver.getAction()) {
                    case PUSH:
                        statusToReportWithSuccessfulPush = CoreSyncStatus.SUCCESS;
                        // Fall through to actually push the core to Blob
                        break;
                    case PULL:
                        // Somehow the blob is fresher than we are, so not pushing anything to it
                        this.callback.finishedPush(this, CoreSyncStatus.BLOB_FRESHER, message);
                        return;
                    case CONFIG_CHANGE:
                        // it is possible that config files to push are empty and config files to pull are non-empty
                        if(resolver.getConfigFilesToPush().isEmpty()){
                            this.callback.finishedPush(this, CoreSyncStatus.SUCCESS_EQUIVALENT, message);
                            return;
                        }
                        statusToReportWithSuccessfulPush = CoreSyncStatus.SUCCESS_CONFIG;
                        // Fall through to push config changes
                        break;
                    case EQUIVALENT:
                        // Blob already got all that it needs. Possibly a previous task was delayed enough and pushed the
                        // changes enqueued twice (and we are the second task to run)
                        this.callback.finishedPush(this, CoreSyncStatus.SUCCESS_EQUIVALENT, message);
                        return;
                    case CONFLICT:
                        // Well, this is the kind of things we hope do not occur too often. Unclear who wins here.
                        // TODO more work required to address this.
                        this.callback.finishedPush(this, CoreSyncStatus.BLOB_CONFLICT, message);
                        return;
                    case BLOB_CORRUPT:
                        // Blob being corrupt at this stage should be pretty straightforward: remove whatever the blob has
                        // for the core and push our local version. Leaving this for later though
                        // TODO likely replace Blob content with local core
                        this.callback.finishedPush(this, CoreSyncStatus.BLOB_CORRUPT, message);
                        return;
                    case BLOB_DELETED:
                        // Avoid pushing cores that are marked for delete in blob. The local copy will be eventually cleaned up by {@link OrphanCoreDeleter}
                        this.callback.finishedPush(this, CoreSyncStatus.BLOB_DELETED_FOR_PUSH, message);
                        return;
                    default:
                        // Somebody added a value to the enum without saying?
                        logger.warn("Unexpected enum value " + resolver.getAction() + ", please update the code");
                        this.callback.finishedPush(this, CoreSyncStatus.FAILURE, message);
                        return;
                }

                CorePushPull cp = new CorePushPull(coreName, resolver, serverMetadata, blobMetadata);
                cp.pushToBlobStore(queuedTimeMs, attemptsCopy);
                
            } finally {
                // Remove ourselves from the in flight set before calling the callback method (just in case it takes forever)
                synchronized (pushesInFlight) {
                    pushesInFlight.remove(getCoreName());
                }
            }

            this.callback.finishedPush(this, statusToReportWithSuccessfulPush, null);
        } catch (InterruptedException e) {
            throw e;
        } catch (IndexNotFoundException infe) {
            this.callback.finishedPush(this, CoreSyncStatus.LOCAL_MISSING_FOR_PUSH, null);
            logger.info("Failed (attempt=" + attemptsCopy + ") to push core " + getCoreName()
                    + " because no longer exists", infe);
        } catch (Exception e) {
            this.callback.finishedPush(this, CoreSyncStatus.FAILURE, e.getMessage());
            logger.warn("Failed (attempt=" + attemptsCopy + ") to push core " + getCoreName(), e);
        }
    }

    /**
     * A callback for {@link CorePushTask} to notify after the push attempt completes.
     * The callback can enqueue a new attempt to try again the core if the attempt failed.
     */
    public interface PushCoreCallback {
        /**
         * The task to push the given coreInfo has completed.
         *
         * @param pushTask  The core push task that has finished.
         * @param status How things went for the task
         * @param message Human readable message explaining a failure, or <code>null</code> if no message available.
         *
         */
        void finishedPush(CorePushTask pushTask, CoreSyncStatus status, String message)
                throws InterruptedException;
    }
}
