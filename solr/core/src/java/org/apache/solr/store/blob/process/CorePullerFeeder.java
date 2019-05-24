package org.apache.solr.store.blob.process;

import java.util.logging.Level;

import org.apache.solr.core.CoreContainer;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import search.blobstore.solr.BlobCoreMetadata;
import searchserver.SfdcConfig;
import searchserver.SfdcConfigProperty;
import searchserver.blobstore.metadata.BlobCoreSyncer;
import searchserver.blobstore.util.DeduplicatingList;
import searchserver.logging.SearchLogger;

/**
 * A pull version of {@link CoreSyncFeeder} then will continually ({@link #feedTheMonsters()}) to load up a work queue (
 * {@link #pullTaskQueue}) with such tasks {@link CorePullTask} to keep the created threads busy :) The tasks will be
 * pulled from {@link CorePullTracker} to which Solr code notifies queried cores which are stale locally and need to be
 * fetched from blob.
 * <p>
 * See {@link CorePusherFeeder} for the push version of this class
 *
 * @author msiddavanahalli
 * @since 214/solr.6
 */
public class CorePullerFeeder extends CoreSyncFeeder {

    private static final SearchLogger logger = new SearchLogger(CorePullerFeeder.class);

    /**
     * This is the registry for the (single) instance of {@link CorePullerFeeder} being created, so it can be destroyed.
     */
    private volatile static CorePullerFeeder runningFeeder = null;

    private final CorePullTask.PullCoreCallback callback;

    protected final DeduplicatingList<String, CorePullTask> pullTaskQueue;
    protected static final String PULLER_THREAD_PREFIX = "puller";

    private static final int numPullerThreads = Integer
            .parseInt(SfdcConfig.get().getSfdcConfigProperty(SfdcConfigProperty.BlobStorePullThreads));

    private CorePullerFeeder(CoreContainer cores) {
        super(cores, numPullerThreads);
        this.pullTaskQueue = new DeduplicatingList<>(ALMOST_MAX_WORKER_QUEUE_SIZE, new CorePullTask.PullTaskMerger());
        this.callback = new CorePullResult();
    }

    public static void init(CoreContainer cores) {
            assert runningFeeder == null;

            CorePullerFeeder cpf = new CorePullerFeeder(cores);
            Thread t = new Thread(cpf);
            t.setName("blobPullerFeeder-" + t.getName());
            t.start();

            runningFeeder = cpf;

            logger.log(Level.INFO, null, "Started CorePullerFeeder");
    }

    @Override
    public Runnable getSyncer() {
        return new CorePullerThread(this, pullTaskQueue);
    }

    @Override
    String getMonsterThreadName() {
        return PULLER_THREAD_PREFIX;
    }

    @Override
    void feedTheMonsters() throws InterruptedException {
        CorePullTracker tracker = CorePullTracker.get();
        final long minMsBetweenLogs = 15000;
        long lastLoggedTimestamp = 0L;
        long syncsEnqueuedSinceLastLog = 0; // This is the non-deduped count
        while (shouldContinueRunning()) {
            // This call will block if there are no stale cores queried and nothing to pull
            PullCoreInfo pci = tracker.getCoreToPull();

            // Add the core to the list consumed by the thread doing the actual work
            CorePullTask pt = new CorePullTask(cores, pci, callback);
            pullTaskQueue.addDeduplicated(pt, /* isReenqueue */ false);
            syncsEnqueuedSinceLastLog++;

            // Log if it's time (we did at least one pull otherwise we would be still blocked in the calls above)
            final long now = System.currentTimeMillis();
            final long msSinceLastLog = now - lastLoggedTimestamp;
            if (msSinceLastLog > minMsBetweenLogs) {
                logger.log(Level.INFO, null, "Since last pull log " + msSinceLastLog + " ms ago, added "
                        + syncsEnqueuedSinceLastLog + " cores to pull from blob. Last one is " + pci.coreName);
                lastLoggedTimestamp = now;
                syncsEnqueuedSinceLastLog = 0;
            }
        }
    }

    public static void shutdown() {
        final CoreSyncFeeder rf = runningFeeder;
        runningFeeder = null;
        if (rf != null) {
            rf.close();
        }
    }

    /**
     * Structure with whatever data we need to track on each core we need to pull from Blob store. This will be
     * deduplicated on core name (the same core requiring two pulls from Blob will only be recorded one if the first
     * pull has not been processed yet).
     */
    static class PullCoreInfo implements DeduplicatingList.Deduplicatable<String> {
        final String coreName;
        final boolean waitForSearcher;
        final boolean createCoreIfAbsent;

        PullCoreInfo(String coreName, boolean createCoreIfAbsent, boolean waitForSearcher) {
            this.coreName = coreName;
            this.waitForSearcher = waitForSearcher;
            this.createCoreIfAbsent = createCoreIfAbsent;
        }

        @Override
        public String getDedupeKey() {
            return coreName;
        }
    }

    /**
     * We only want one entry in the list for each core, so when a second entry arrives, we merge them on core name.
     */
    static class PullCoreInfoMerger implements DeduplicatingList.Merger<String, PullCoreInfo> {
        @Override
        public PullCoreInfo merge(PullCoreInfo v1, PullCoreInfo v2) {
            return mergePullCoreInfos(v1, v2);
        }

        static PullCoreInfo mergePullCoreInfos(PullCoreInfo v1, PullCoreInfo v2) {
            assert v1.coreName.equals(v2.coreName);

            // if one needs to wait then merged will have to wait as well 
            final boolean waitForSearcher = v1.waitForSearcher || v2.waitForSearcher;

            // if one wants to create core if absent then merged will have to create as well 
            final boolean createCoreIfAbsent = v1.createCoreIfAbsent || v2.createCoreIfAbsent;

            return new PullCoreInfo(v1.coreName, createCoreIfAbsent, waitForSearcher);
        }
    }

    /**
     * When a {@link CorePullerThread} finishes its work, it's calling an instance of this class.
     */
    private class CorePullResult implements CorePullTask.PullCoreCallback {

        @Override
        public void finishedPull(@NonNull CorePullTask pullTask, @Nullable BlobCoreMetadata blobMetadata, CoreSyncStatus status, @Nullable String message)
                throws InterruptedException {
            try {
                // TODO given for now we consider environment issues as blob/corruption issues, not sure retrying currently makes sense. See comment in CorePushPull.pullUpdateFromBlob() regarding thrown exception
                if (status.isTransientError() && pullTask.getAttempts() < MAX_ATTEMPTS) {
                    pullTask.setAttempts(pullTask.getAttempts() + 1);
                    pullTask.setLastAttemptTimestamp(System.currentTimeMillis());
                    pullTaskQueue.addDeduplicated(pullTask, true);
                    return;
                }
                PullCoreInfo pullCoreInfo = pullTask.getPullCoreInfo();
                if (status.isSuccess()) {
                    logger.log(Level.INFO, null,
                            String.format("Pulling core %s succeeded. Last status=%s attempts=%s . %s",
                                    pullCoreInfo.coreName, status, pullTask.getAttempts(), message == null ? "" : message));
                } else {
                    logger.log(Level.WARNING, null,
                            String.format("Pulling core %s failed. Giving up. Last status=%s attempts=%s . %s",
                                    pullCoreInfo.coreName, status, pullTask.getAttempts(), message == null ? "" : message));
                }
                BlobCoreSyncer.finishedPull(pullCoreInfo.coreName, status, blobMetadata, message);
            } catch (InterruptedException ie) {
                close();
                throw ie;
            }
        }
    }
}