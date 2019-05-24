package org.apache.solr.store.blob.process;

import org.apache.lucene.util.NamedThreadFactory;
import searchserver.SfdcConfig;
import searchserver.SfdcConfigProperty;
import searchserver.logging.SearchLogger;

import java.util.Set;
import java.util.concurrent.*;
import java.util.logging.Level;

/**
 * Manager of blobs (files) to delete, putting them in a queue (if space left on the queue) then consumed and processed
 * by {@link BlobDeleterTask}
 *
 * @author iginzburg
 * @since 214/solr.6
 */
public class BlobDeleteManager {

    private static final SearchLogger logger = new SearchLogger(BlobDeleteManager.class);

    /**
     * This is the registry for the (single) instance of {@link BlobDeleteManager} being created, so it can be destroyed.
     * Null here means BlobDeleteManager was not initialized (maybe it will not be) and blob deletes are not accepted.
     */
    private volatile static BlobDeleteManager runningDeleter = null;

    /**
     * Limit to the number of blob files to delete accepted on the delete queue (and lost in case of server crash). When
     * the queue reaches that size, no more deletes are accepted (will be retried later for a core, next time it is pushed).
     * (note that tests in searchserver.blobstore.metadata.CorePushTest trigger a merge that enqueues more than 100 files to
     * be deleted. If that constant is reduced to 100 for example, some enqueues in the test will fail and there will be
     * files left to be deleted where we've expected none. So don't reduce it too much :)
     */
    private static final int ALMOST_MAX_DELETER_QUEUE_SIZE = 200;

    private final RejecterThreadPoolExecutor deleteExecutor;

    /**
     * After a core push has marked a file as deleted, wait at least this long before actually deleting its blob from the
     * blob store, just in case a concurrent (unexpected) update to the core metadata on the Blob store assumes the file
     * is still present... (given reading and writing core metadata on the blob are not transaction/atomic, if two updates
     * occur at the same time, the last writer wins and might "resuscitate" files marked for delete by the first one).<p>
     *
     * The delay here should be longer than twice the longest observed push operation, as logged from the end of {@link CorePushTask#pushCoreToBlob()}.
     * Why twice? because files are marked for delete before pushing happens, so by the time we consider if physical delete should happen,
     * there's already one push interval that has gone by. If we consider that just before the write of core.metadata back
     * to blob store by this first push operation another push starts and reads core.metadata, then that other push can again
     * take quite a long time before writing back core.metadata in which the files initially marked as deleted should not longer
     * be deleted. If we want these files to not be deleted before the second push completes, then the hard delete delay should
     * be twice the longest possible push. Note that if the second push requires these files as part of the core (no longer
     * deleted) then they would have disappeared from the deleted file list in core.metadata and no further delete enqueue
     * will be made for them. In other words, there's nothing to cancel, the strategy works by not enqueueing a physical
     * delete until we know for sure the file can be resuscitated...
     */
    private final long deleteDelayMs;

    private BlobDeleteManager(int almostMaxQueueSize, int numDeleterThreads, long deleteDelayMs) {
        NamedThreadFactory threadFactory = new NamedThreadFactory("BlobFileDeleter");

        // Note this queue MUST NOT BE BOUNDED, or we risk deadlocks given that BlobDeleterTask's reenqueue themselves upon failure
        BlockingQueue<Runnable> deleteQueue = new LinkedBlockingDeque<>();

        deleteExecutor = new RejecterThreadPoolExecutor(numDeleterThreads, deleteQueue, almostMaxQueueSize, threadFactory);

        this.deleteDelayMs = deleteDelayMs;
    }

    public static void init() {
        // BlobDeleteManager should be initialized only once in production and can be initialized multiple times in tests
        // but then (if asserts are enabled on your system...) should be destroyed (call to shutdown()) before being initialized again.
        assert runningDeleter == null;

        // Only enable the blob file deleter (and create its thread and the thread its thread creates) if configured to
        // do so.
        if (Boolean.parseBoolean(SfdcConfig.get().getSfdcConfigProperty(SfdcConfigProperty.EnableBlobFileDeleter))) {

            int numDeleterThreads = Integer.parseInt(SfdcConfig.get().getSfdcConfigProperty(SfdcConfigProperty.BlobFileDeleterThreads));
            long deleteDelayMs = Long.parseLong(SfdcConfig.get().getSfdcConfigProperty(SfdcConfigProperty.BlobFileDeleteDelay));

            runningDeleter = new BlobDeleteManager(ALMOST_MAX_DELETER_QUEUE_SIZE, numDeleterThreads, deleteDelayMs);

            logger.log(Level.INFO, null, "EnableBlobFileDeleter is true, BlobDeleteManager will accept deletes");
        } else {
            logger.log(Level.INFO, null, "EnableBlobFileDeleter is false, BlobDeleteManager will reject all deletes");
        }
    }

    public static void shutdown() {
        final BlobDeleteManager rd = runningDeleter;
        runningDeleter = null;
        if (rd != null) {
            // Note the call below does not wait until all previously enqueued tasks complete (but it does prevent the
            // pool from accepting new ones).
            rd.deleteExecutor.shutdown();
        }
    }

    /**
     * @return the instance of {@link BlobDeleteManager} to use or <code>null</code> if none defined.
     */
    public static BlobDeleteManager get() {
        return runningDeleter;
    }

    public long getDeleteDelayMs() {
        return deleteDelayMs;
    }

    /**
     * This method is called 'externally" (i.e. not by tasks needing to reenqueue) and enq
     * @return <code>true</code> if the delete was enqueued, <code>false</code> if can't be enqueued (deleter turned off
     * by config or current queue of blobs file deletes too full).
     */
    public boolean enqueueForDelete(String coreName, Set<String> blobNames) {
        BlobDeleteManager deleter = runningDeleter;

        if (deleter == null) {
            return false;
        }

        BlobDeleterTask command = new BlobDeleterTask(coreName, blobNames, deleter.deleteExecutor);
        return deleter.deleteExecutor.executeIfPossible(command);
    }

    /**
     * Subclass of {@link ThreadPoolExecutor} that has an additional command enqueue method {@link #executeIfPossible(Runnable)}
     * that rejects the enqueue if the underlying queue is over a configured size.<p>
     * The created thread pool executor has a fixed number of threads because the undelying blocking queue is unbounded.
     */
    private class RejecterThreadPoolExecutor extends ThreadPoolExecutor {
        private final int targetMaxQueueSize;
        /**
         * @param poolSize the number of threads to keep in the pool. There is a fixed number of threads in that pool,
         *                 because a {@link ThreadPoolExecutor} using an unbounded queue will not have the pool create more threads
         *                 than the core pool size, even tasks are slow to execute.
         * @param workQueue the queue to use for holding tasks before they are
         *        executed.  This queue will hold only the {@code Runnable}
         *        tasks submitted by the {@code execute} method.
         * @param targetMaxQueueSize max queue size to accept enqueues through {@link #executeIfPossible(Runnable)} but having
         *        no impact on enqueues through {@link #execute(Runnable)}.
         * @param threadFactory the factory to use when the executor
         *        creates a new thread
         */
        RejecterThreadPoolExecutor(int poolSize,
                                  BlockingQueue<Runnable> workQueue,
                                  int targetMaxQueueSize,
                                  ThreadFactory threadFactory) {
            super(poolSize, poolSize, 0L, TimeUnit.SECONDS, workQueue, threadFactory);
            this.targetMaxQueueSize = targetMaxQueueSize;
        }

        /**
         * Enqueues the passed <code>command</code> for execution just like a call to superclass' {@link ThreadPoolExecutor#execute(Runnable)}
         * if the work queue is not too full (below or at size <code>targetMaxQueueSize</code> passed in the constructor).
         * @param command the task to execute
         * @return <code>true</code> if the <code>command</code> was accepted and enqueued for execution (or executed),
         * <code>false</code> if the underlying queue was too full and the <code>command</code> was not enqueued for execution
         * (i.e. nothing happened).
         */
        boolean executeIfPossible(Runnable command) {
            if (getQueue().size() > targetMaxQueueSize) {
                return false;
            }

            try {
                execute(command);
            } catch (RejectedExecutionException ree) {
                // pool might be shutting down
                return false;
            }
            return true;
        }
    }
}
