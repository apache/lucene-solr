package org.apache.solr.store.blob.process;

import org.apache.solr.core.CoreContainer;

import org.apache.solr.store.blob.util.DeduplicatingList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.lang.invoke.MethodHandles;

/**
 * A push version of {@link CoreSyncFeeder} then will continually ({@link #feedTheMonsters()}) to load up a work queue (
 * {@link #pushTasksQueue}) with such tasks {@link CorePushTask} to keep the created threads busy :) The tasks will be
 * pulled from {@link CoreUpdateTracker} to which Solr code notified cores having changed locally that need pushing to
 * Blob store.
 * <p>
 * Retries are handled in a non elegant way. W-4659893 is taking care of improving this (eventually).
 * <p>
 * See {@link CorePullerFeeder} for the pull version of this class
 *
 * @author iginzburg
 * @since 214/solr.6
 */
public class CorePusherFeeder extends CoreSyncFeeder {

    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    /**
     * This is the registry for the (single) instance of {@link CorePusherFeeder} being created, so it can be destroyed.
     */
    private volatile static CorePusherFeeder runningFeeder = null;

    private static final int numPusherThreads = 5;

    private final CorePushTask.PushCoreCallback callback;

    protected final DeduplicatingList<String, CorePushTask> pushTasksQueue;
    protected static final String PUSHER_THREAD_PREFIX = "pusher";
    private CorePusherFeeder(CoreContainer cores) {
        super(cores, numPusherThreads);
        this.pushTasksQueue = new DeduplicatingList<>(ALMOST_MAX_WORKER_QUEUE_SIZE, new CorePushTask.PushTaskMerger());
        this.callback = new CorePushResult();
    }

    public static void init(CoreContainer cores) {
        // Only enable the core pusher feeder (and create its thread and the thread its thread creates) if configured to
        // do so. Note that if this property is disabled, we also not enqueue core changes in CoreUpdateTracker
        assert runningFeeder == null;

        CorePusherFeeder cpf = new CorePusherFeeder(cores);
        Thread t = new Thread(cpf);
        t.setName("blobPusherFeeder-" + t.getName());
        t.start();

        runningFeeder = cpf;

        logger.info("EnableBlobBackgroundPushing is true, started CorePusherFeeder");
    }

    @Override
    public CorePusherThread getSyncer() {
        return new CorePusherThread(this, pushTasksQueue);
    }

    @Override
    String getMonsterThreadName() {
        return PUSHER_THREAD_PREFIX;
    }

    @Override
    void feedTheMonsters() throws InterruptedException {
        CoreUpdateTracker tracker = CoreUpdateTracker.get();
        final long minMsBetweenLogs = 15000;
        long lastLoggedTimestamp = 0L;
        long pushesSinceLastLog = 0;
        while (shouldContinueRunning()) {
            // This call will block if there are no changed cores needing to be pushed
            PushCoreInfo pci = tracker.getCoreToPush();

            // Add the core to the list consumed by the thread doing the actual work
            CorePushTask pt = new CorePushTask(cores, pci.coreName, callback);
            pushTasksQueue.addDeduplicated(pt, false);
            pushesSinceLastLog++;

            // Log if it's time (we did at least one push otherwise we would be still blocked in the calls above)
            final long now = System.currentTimeMillis();
            final long msSinceLastLog = now - lastLoggedTimestamp;
            if (msSinceLastLog > minMsBetweenLogs) {
                logger.info("Since last push log " + msSinceLastLog + " ms ago, added "
                        + pushesSinceLastLog + " cores to push to blob. Last one is " + pci.coreName);
                lastLoggedTimestamp = now;
                pushesSinceLastLog = 0;
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
     * Structure with whatever data we need to track on each core we need to push to Blob store. This will be
     * deduplicated on core name (the same core requiring two pushes with Blob will only be recorded one if the first
     * push has not been processed yet).
     */
    static class PushCoreInfo implements DeduplicatingList.Deduplicatable<String> {
        final String coreName;

        PushCoreInfo(String coreName) {
            this.coreName = coreName;
        }

        @Override
        public String getDedupeKey() {
            return coreName;
        }
    }

    /**
     * We only want one entry in the list for each core, so when a second entry arrives, we merge them on core name.
     */
    static class PushCoreInfoMerger implements DeduplicatingList.Merger<String, PushCoreInfo> {
        @Override
        public PushCoreInfo merge(PushCoreInfo v1, PushCoreInfo v2) {
            assert v1.coreName.equals(v2.coreName);

            return new PushCoreInfo(v1.coreName);
        }
    }

    /**
     * When a {@link CorePusherThread} finishes its work, it's calling an instance of this class.
     */
    private class CorePushResult implements CorePushTask.PushCoreCallback {

        @Override
        public void finishedPush(CorePushTask pushTask, CoreSyncStatus status, String message)
                throws InterruptedException {
            try {
                if (status.isTransientError() && pushTask.getAttempts() < MAX_ATTEMPTS) {
                    pushTask.setAttempts(pushTask.getAttempts() + 1);
                    pushTask.setLastAttemptTimestamp(System.currentTimeMillis());
                    CorePusherFeeder.this.pushTasksQueue.addDeduplicated(pushTask, true);
                    return;
                }

                if (status.isSuccess()) {
                    logger.info(String.format("Pushing core %s succeeded. Last status=%s attempts=%s . %s", 
                                    pushTask.getCoreName(), status, pushTask.getAttempts(), message == null ? "" : message));
                } else {
                    logger.warn(String.format("Pushing core %s failed. Giving up. Last status=%s attempts=%s . %s", 
                                    pushTask.getCoreName(), status, pushTask.getAttempts(), message == null ? "" : message));
                }
            } catch (InterruptedException ie) {
                CorePusherFeeder.this.close();
                throw ie;
            }
        }
    }
}
