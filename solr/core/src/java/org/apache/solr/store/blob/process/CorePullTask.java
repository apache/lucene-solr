package org.apache.solr.store.blob.process;

import java.io.File;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import com.google.common.base.Throwables;
import com.google.common.collect.*;
import org.apache.solr.core.*;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import search.blobstore.client.CoreStorageClient;
import search.blobstore.solr.BlobCoreMetadata;
import searchserver.SfdcConfig;
import searchserver.blobstore.metadata.*;
import searchserver.blobstore.process.CorePullerFeeder.PullCoreInfo;
import searchserver.blobstore.provider.BlobStorageProvider;
import searchserver.blobstore.util.DeduplicatingList;
import searchserver.core.*;
import searchserver.logging.SearchLogger;
import searchserver.util.CoreUtil;

/**
 * Code for pulling updates on a specific core to the Blob store. see {@CorePushTask} for the push version of this.
 * 
 * @author iginzburg/msiddavanahalli
 * @since 214/solr.6
 */
public class CorePullTask implements DeduplicatingList.Deduplicatable<String> {
    private static final SearchLogger logger = new SearchLogger(CorePullTask.class);

    /**
     * Minimum delay between to pull retries for a given core. Setting this higher than the push retry to reduce noise
     * we get from a flood of queries for a stale core
     */
    private static final long MIN_RETRY_DELAY_MS = 20000;

    /** Cores currently being pulled and timestamp of pull start (to identify stuck ones in logs) */
    private static final HashMap<String, Long> pullsInFlight = Maps.newHashMap();

    /** Cores unknown locally that got created as part of the pull process but for which no data has been pulled yet
     * from Blob store. If we ignore this transitory state, these cores can be accessed locally and simply look empty.
     * We'd rather treat threads attempting to access such cores like threads attempting to access an unknown core and
     * do a pull (or more likely wait for an ongoing pull to finish).<p>
     *
     * When this lock has to be taken as well as {@link #pullsInFlight}, then {@link #pullsInFlight} has to be taken first.
     * Reading this set implies acquiring the monitor of the set (as if @GuardedBy("itself")), but writing to the set
     * additionally implies holding the {@link #pullsInFlight}. This guarantees that while {@link #pullsInFlight}
     * is held, no element in the set is changing.
     */
    private static final Set<String> coresCreatedNotPulledYet = Sets.newHashSet();
    
    private final CoreContainer coreContainer;
    private final PullCoreInfo pullCoreInfo;
    private final long queuedTimeMs;
    private int attempts;
    private long lastAttemptTimestamp;
    private final PullCoreCallback callback;

    CorePullTask(@NonNull CoreContainer coreContainer, @NonNull PullCoreInfo pullCoreInfo, @NonNull PullCoreCallback callback) {
        this(coreContainer, pullCoreInfo, System.currentTimeMillis(), 0, 0L, callback);
    }

    private CorePullTask(CoreContainer coreContainer, PullCoreInfo pullCoreInfo, long queuedTimeMs, int attempts,
                         long lastAttemptTimestamp, PullCoreCallback callback) {
        this.coreContainer = coreContainer;
        this.pullCoreInfo = pullCoreInfo;
        this.queuedTimeMs = queuedTimeMs;
        this.attempts = attempts;
        this.lastAttemptTimestamp = lastAttemptTimestamp;
        this.callback = callback;
    }

    /**
     * Returns a _hint_ that the given core might be locally empty because it is awaiting pull from Blob store.
     * This is just a hint because as soon as the lock is released when the method returns, the status of the core could change.
     */
    public static boolean isEmptyCoreAwaitingPull(String corename) {
        synchronized (coresCreatedNotPulledYet) {
            return coresCreatedNotPulledYet.contains(corename);
        }
    }

    /**
     * Needed for the {@link CorePullTask} to be used in a {@link DeduplicatingList}.
     */
    @Override
    public String getDedupeKey() {
        return this.pullCoreInfo.coreName;
    }

    /**
     * Needed for the {@link CorePullTask} to be used in a {@link DeduplicatingList}.
     */
    static class PullTaskMerger implements DeduplicatingList.Merger<String, CorePullTask> {
        /**
         * Given two tasks (that have not yet started executing!) that target the same core (and would basically do the
         * same things were they both executed), returns a merged task that can replace both and that retains the oldest
         * enqueue time and the smallest number of attempts, so we don't "lose" retries because of the merge yet we
         * correctly report that tasks might have been waiting for execution for a long while.
         * 
         * @return a merged {@link CorePullTask} that can replace the two tasks passed as parameters.
         */
        @Override
        public CorePullTask merge(CorePullTask task1, CorePullTask task2) {
            // The asserts below are not guaranteed by construction but we know that's the case
            assert task1.coreContainer == task2.coreContainer;
            assert task1.callback == task2.callback;

            int mergedAttempts;
            long mergedLatAttemptsTimestamp;

            // Synchronizing on the tasks separately to not risk deadlock (even though in practice there's only one
            // concurrent
            // call to this method anyway since it's called from DeduplicatingList.addDeduplicated() and we syncrhonize
            // on the
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

            PullCoreInfo mergedPullCoreInfo = CorePullerFeeder.PullCoreInfoMerger.mergePullCoreInfos(task1.pullCoreInfo, task2.pullCoreInfo);
            // We merge the tasks.
            return new CorePullTask(task1.coreContainer, mergedPullCoreInfo,
                    Math.min(task1.queuedTimeMs, task2.queuedTimeMs), mergedAttempts, mergedLatAttemptsTimestamp,
                    task1.callback);
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

    public PullCoreInfo getPullCoreInfo() {
        return pullCoreInfo;
    }

    /**
     * Pulls the local core updates from the Blob store then calls the task callback to notify the
     * {@link CorePullerFeeder} of success or failure of the operation, give an indication of the reason the periodic
     * puller can decide to retry or not.
     */
    void pullCoreFromBlob() throws InterruptedException {
        BlobCoreMetadata blobMetadata = null;
        if (coreContainer.isShutDown()) {
            this.callback.finishedPull(this, blobMetadata, CoreSyncStatus.SHUTTING_DOWN, null);
            // TODO could throw InterruptedException here or interrupt ourselves if we wanted to signal to
            // CorePullerThread to stop everything.
            return;
        }

        synchronized (pullsInFlight) {
            Long pullInFlightTimestamp = pullsInFlight.get(pullCoreInfo.coreName);
            if (pullInFlightTimestamp != null) {
                // Another pull is in progress, we'll retry later.
                // Note we can't just cancel this pull, because the other pull might be working on a previous commit
                // point.
                long prevPullMs = System.currentTimeMillis() - pullInFlightTimestamp;
                logger.log(Level.WARNING, null,
                        "Skipping core pull for " + pullCoreInfo.coreName
                                + " because another thread is currently pulling it (started " + prevPullMs
                                + " ms ago). Will retry.");
                this.callback.finishedPull(this, blobMetadata, CoreSyncStatus.CONCURRENT_SYNC, null);
                return;
            } else {
                pullsInFlight.put(pullCoreInfo.coreName, System.currentTimeMillis());
            }
        }

        // Copying the non final variables so we're clean wrt the Java memory model and values do not change as we go
        // (even though we know that no other thread can be working on this CorePullTask when we handle it here).
        final int attemptsCopy = getAttempts();
        final long lasAttemptTimestampCopy = getLastAttemptTimestamp();


        if (attemptsCopy != 0) {
            long now = System.currentTimeMillis();
            if (now - lasAttemptTimestampCopy < MIN_RETRY_DELAY_MS) {
                Thread.sleep(MIN_RETRY_DELAY_MS - now + lasAttemptTimestampCopy);
            }
        }

        CoreSyncStatus syncStatus = CoreSyncStatus.FAILURE;
        // Auxiliary information related to pull outcome. It can be metadata resolver message which can be null or exception detail in case of failure 
        String message = null;
        try {
            // Do the sequence of actions required to pull a core from the Blob store.
            CoreStorageClient blobClient = BlobStorageProvider.get().getBlobStorageClient();
            blobMetadata = blobClient.pullCoreMetadata(pullCoreInfo.coreName);
            if (blobMetadata == null) {
                syncStatus = CoreSyncStatus.BLOB_MISSING;
                this.callback.finishedPull(this, blobMetadata, syncStatus, null);
                return;
            } else if (blobMetadata.getIsDeleted()) {
                syncStatus = CoreSyncStatus.BLOB_DELETED_FOR_PULL;
                this.callback.finishedPull(this, blobMetadata, syncStatus, "deleted flag is set on core in Blob store. Not pulling.");
                return;
            } else if (blobMetadata.getIsCorrupt()) {
                // TODO this might mean we have no local core at this stage. If that's the case, we may need to do something about it so that Core App does not immediately reindex into a new core...
                //      likely changes needed here for W-5388477 Blob store corruption repair
                syncStatus = CoreSyncStatus.BLOB_CORRUPT;
                this.callback.finishedPull(this, blobMetadata, syncStatus, "corrupt flag is set on core in Blob store. Not pulling.");
                return;
            }

            if (!coreExists(pullCoreInfo.coreName)) {
                if (pullCoreInfo.createCoreIfAbsent) {
                    // We set the core as created awaiting pull before creating it, otherwise it's too late.
                    // If we get to this point, we're setting the "created not pulled yet" status of the core here (only place
                    // in the code where this happens) and we're clearing it in the finally below.
                    // We're not leaking entries in coresCreatedNotPulledYet that might stay there forever...
                    synchronized (pullsInFlight) {
                        synchronized (coresCreatedNotPulledYet) {
                            coresCreatedNotPulledYet.add(pullCoreInfo.coreName);
                        }
                    }
                    createCore(pullCoreInfo.coreName);
                } else {
                    syncStatus = CoreSyncStatus.LOCAL_MISSING_FOR_PULL;
                    this.callback.finishedPull(this, blobMetadata, syncStatus, null);
                    return;
                }
            }

            // Get local metadata
            ServerSideCoreMetadata serverMetadata = new ServerSideCoreMetadata(pullCoreInfo.coreName, coreContainer);

            MetadataResolver resolver = new MetadataResolver(serverMetadata, blobMetadata);

            message = resolver.getMessage();

            switch (resolver.getAction()) {
                case PULL:
                    // pull the core from Blob
                    CorePushPull cp = new CorePushPull(pullCoreInfo.coreName, resolver, serverMetadata, blobMetadata);
                    // The following call can fail if blob is corrupt (in non trivial ways, trivial ways are identified by other cases)
                    cp.pullUpdateFromBlob(queuedTimeMs, pullCoreInfo.waitForSearcher, attemptsCopy);
                    // pull was successful
                    if(isEmptyCoreAwaitingPull(pullCoreInfo.coreName)){
                        // the javadoc for pulledBlob suggests that it is only meant to be called if we pulled from scratch
                        // therefore only limiting this call when we created the local core for this pull ourselves
                        BlobTransientLog.get().getCorruptCoreTracker().pulledBlob(pullCoreInfo.coreName, blobMetadata);
                    }
                    syncStatus = CoreSyncStatus.SUCCESS;
                    break;
                case PUSH:
                    // Somehow the local copy is fresher than blob. Do nothing
                    syncStatus = CoreSyncStatus.SUCCESS_EQUIVALENT;
                    break;
                case CONFIG_CHANGE:
                    // it is possible that config files to pull are empty and config files to push are non-empty
                    if (resolver.getConfigFilesToPull().isEmpty()) {
                        syncStatus = CoreSyncStatus.SUCCESS_EQUIVALENT;
                    } else {
                        // so far we have decided not to pull config only changes
                        syncStatus = CoreSyncStatus.SKIP_CONFIG;
                    }
                    break;
                case EQUIVALENT:
                    // Local already has all that it needs. Possibly a previous task was delayed enough and pulled the
                    // changes enqueued twice (and we are the second task to run)
                    syncStatus = CoreSyncStatus.SUCCESS_EQUIVALENT;
                    break;
                case CONFLICT:
                    // Well, this is the kind of things we hope do not occur too often. Unclear who wins here.
                    // TODO more work required to address this.
                    syncStatus = CoreSyncStatus.BLOB_CONFLICT;
                    break;
                case BLOB_CORRUPT:
                    // Blob being corrupt at this stage should be pretty straightforward: remove whatever the blob has
                    // for the core and push our local version. Leaving this for later though
                    // TODO mark Blob Core as corrupt (set flag)

                    // TODO likely replace Blob content with local core
                    // TODO local core is not made corrupt by this, but maybe local core doesn't exist as a result (if there was no local core before trying to pull)
                    // TODO if no local core exists, set state in corrupt core tracker to deleted_pullable? Other cases to manage?
                    syncStatus = CoreSyncStatus.BLOB_CORRUPT;
                    break;
                default:
                    // Somebody added a value to the enum without saying?
                    logger.log(Level.SEVERE, null,
                            "Unexpected enum value " + resolver.getAction() + ", please update the code");
                    syncStatus = CoreSyncStatus.FAILURE;
                    break;
            }
        } catch (InterruptedException e) {
            throw e;
        } catch (Exception e) {
            syncStatus = CoreSyncStatus.FAILURE;
            message = Throwables.getStackTraceAsString(e);
            logger.log(Level.WARNING, null, "Failed (attempt=" + attemptsCopy + ") to pull core " + pullCoreInfo.coreName, e);
        } finally {
            // Remove ourselves from the in flight set before calling the callback method (just in case it takes
            // forever)
            synchronized (pullsInFlight) {
                // No matter how the pull ends (success or any kind of error), we don't want to consider the core as awaiting pull,
                // since it doesn't anymore (code is inline here rather than in a method or in notifyEndOfPull() to make
                // it clear how coresCreatedNotPulledYet is managed).
                synchronized (coresCreatedNotPulledYet) {
                    // TODO: Can we move this business of core creation and deletion outside of this task so that
                    //       we may not sub-optimally repeatedly create/delete core in case of reattempt of a transient pull error?
                    //       or get whether a reattempt will be made or not, and if there is a guaranteed reattempt then do not delete it
                    if (coresCreatedNotPulledYet.remove(pullCoreInfo.coreName)) {
                        if (!syncStatus.isSuccess()) {
                            // If we created the core and we could not pull successfully then we should cleanup after ourselves by deleting it
                            // otherwise queries can incorrectly return 0 results from that core.
                            if(coreExists(pullCoreInfo.coreName)) {
                                try {
                                    // try to delete core within 3 minutes. In future when we time box our pull task then we 
                                    // need to make sure this value is within that bound. 
                                    CoreDeleter.deleteCoreByName(coreContainer, pullCoreInfo.coreName, 3, TimeUnit.MINUTES);
                                } catch (Exception ex) {
                                    // TODO: should we gack?
                                    //       can we do anything more here since we are unable to delete and we are leaving an empty core behind
                                    //       when we should not. Should we keep the core in coresCreatedNotPulledYet and try few more times
                                    //       but at some point we would have to let it go
                                    //       So may be, few more attempts here and then gack
                                    logger.log(Level.SEVERE, null, "CorePullTask successfully created local core but failed to pull it" +
                                            " and now is unable to delete that local core " + pullCoreInfo.coreName, ex);
                                }
                            }
                        }
                    }
                }
                pullsInFlight.remove(pullCoreInfo.coreName);
            }
        }
        this.callback.finishedPull(this, blobMetadata, syncStatus, message);
    }


    /**
     * Returns true if the given core exists.
     */
    private boolean coreExists(String coreName) {

        SolrCore core = null;
        CoreMetadata.CoreLocation location = CoreMetadataProvider.get(coreContainer).getCoreLocation(coreName);
        File coreIndexDir = location.getIndexDir();
        if (coreIndexDir.exists()) {
            core = coreContainer.getCore(coreName);
        }

        logger.log(Level.INFO, "", "Core " + coreName + " expected in dir " + coreIndexDir.getAbsolutePath() + " exists=" + coreIndexDir.exists()
                + " and location.instanceDirectory.getAbsolutePath()=" + location.getInstanceDirectory().getAbsolutePath());

        if (core != null) {
            // Core exists.
            core.close();
            return true;
        } else {
            return false;
        }
    }

    /**
     * Creates a local (empty) core. This is required before we can fill this core with data pulled from Blob.
     */
    private void createCore(String coreName) throws Exception {

        logger.log(Level.INFO, "", "About to create local core " + coreName);

        // The location here might be different from the location used in coreExists() above. This is ok, if the core
        // did not exist and we're creating it, it's ok to create on another drive (and hopefully the HDD/SSD core placement
        // code will go away with the move to Blob based Storage of cores).
        CoreMetadata.CoreLocation location = CoreMetadataProvider.get(coreContainer).getCoreLocation(coreName);

        CoreMetadataProvider.get(coreContainer).executeExclusive(coreName, coreContainer, () -> {
            Map<String, String> params = CoreUtil.getCoreCreationParams(SfdcConfig.get(), location, null);
            params.put(CoreDescriptor.CORE_DATADIR, location.getDataDirectory().getPath());
            params.put(CoreDescriptor.CORE_CONFIGSET, "coreset");
            params.put(CoreDescriptor.CORE_TRANSIENT, "true");
            params.put(CoreDescriptor.CORE_LOADONSTARTUP, "false");

            // In certain corrupt core scenarios, an index may not exist although we have a core descriptor for it.
            // In this case, we still wish to pull from our blob, so we'll remove the descriptor (after copying its
            // properties above, if any) in order to allow the create call to succeed (otherwise will fail with core
            // already exists error).
            if (null != coreContainer.getTransientCache().removeTransientDescriptor(coreName)) {
                logger.log(Level.FINE, null, "BlobCoreSyncer: removed an existing core descriptor for core=" + coreName +
                        ", inCache=" + location.getInCache() + ", location=" + location.getIndexDir() == null ? "null" : location.getIndexDir().getAbsolutePath());
            }

            coreContainer.create(coreName, location.getInstanceDirectory().toPath(), params, false);
            // we do not call open() on the new core to not increase its ref count.
            return null;
        }, 3, TimeUnit.MINUTES, CoreMetadata.CoreStatus.Creating);
    }

    /**
     * A callback for {@link CorePullTask} to notify after the pull attempt completes. The callback can enqueue a new
     * attempt to try again the core if the attempt failed.
     */
    public interface PullCoreCallback {
        /**
         * The task to pull the given coreInfo has completed.
         *
         * @param pullTask
         *            The core push task that has finished.
         * @param blobMetadata
         *            The blob metadata used to make the pull attempt
         * @param status
         *            How things went for the task
         * @param message
         *            Human readable message explaining a failure, or <code>null</code> if no message available.
         */
        void finishedPull(@NonNull CorePullTask pullTask, @Nullable BlobCoreMetadata blobMetadata, CoreSyncStatus status, @Nullable String message)
                throws InterruptedException;
    }
}
