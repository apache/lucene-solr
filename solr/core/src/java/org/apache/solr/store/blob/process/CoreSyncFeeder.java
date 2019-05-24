package org.apache.solr.store.blob.process;

import java.io.Closeable;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

import org.apache.solr.core.CoreContainer;

import com.force.commons.util.concurrent.NamedThreadFactory;

import lib.gack.GackLevel;
import searchserver.SfdcConfigProperty;
import searchserver.logging.SearchLogger;
import searchserver.logging.gack.SearchServerGack;

/**
 * A {@link Runnable} that will start a set of threads {@link CorePusherThread} or {@link CorePullerThread} to process tasks
 * pushing/pulling local core Class originally inspired by {@link searchserver.replication.PeerSyncer} but ended up
 * being quite different.
 *
 * @author iginzburg
 * @since 214/solr.6
 */
public abstract class CoreSyncFeeder implements Runnable, Closeable {

    private static final SearchLogger logger = new SearchLogger(CoreSyncFeeder.class);

    protected final CoreContainer cores;

    /**
     * Maximum number of elements in the queue, NOT counting re-inserts after failures. Total queue size might therefore
     * exceed this value by the number of syncThreads which is around 5 or 10...
     * <p>
     * see {@link SfdcConfigProperty#BlobStorePushThreads}) and {@link SfdcConfigProperty#BlobStorePullThreads})
     * <p>
     * Note that this queue sits behind other tracking queues (see {@link CoreUpdateTracker} and
     * {@link CorePullTracker ). The other queue has to be large, this one does not.
     */
    protected static final int ALMOST_MAX_WORKER_QUEUE_SIZE = 2000;

    /**
     * When a transient error occurs, the number of attempts to sync with Blob before giving up. Attempts are spaced by
     * at least 10 seconds (see {@link CorePushTask#MIN_RETRY_DELAY_MS} nad {@link CorePullTask#MIN_RETRY_DELAY_MS)
     * which means we'll retry for at least 90 seconds before giving up. This is something to adjust as the
     * implementation of the delay between retries is cleaned up, see {@link CorePushTask#pushCoreToBlob} and
     * {@link CorePullTask#pullCoreFromBlob().
     */
    protected static final int MAX_ATTEMPTS = 10;

    private final int numSyncThreads;

    /**
     * Used to interrupt close()
     */
    private volatile Thread executionThread;
    private volatile boolean closed = false;

    private final AtomicBoolean shouldContinue = new AtomicBoolean(true);

    protected CoreSyncFeeder(CoreContainer cores, int numSyncThreads) {
        this.numSyncThreads = numSyncThreads;
        this.cores = cores;
    }

    @Override
    public void run() {
        // Record where we run so we can be interrupted from close(). If we get interrupted before this point we will
        // not
        // close anything and not log anything, but that's ok we haven't started anything either.
        this.executionThread = Thread.currentThread();
        // If close() executed before runningFeeder was set, we need to exit
        if (closed) { return; }
        try {
            // We'll be submitting tasks to a queue from which threads are going to pick them up and execute them.
            // PeerSyncer uses a ThreadPoolExecutor for this. Here instead we explicitly start the threads to
            // simplify our life (and the code), because by not using an executor we're not forced to use a
            // BlockingQueue<Runnable> as the work queue and instead use a DeduplicatingList.
            NamedThreadFactory threadFactory = new NamedThreadFactory(getMonsterThreadName(), true);
            Set<Thread> syncerThreads = new HashSet<>();
            for (int i = 0; i < numSyncThreads; i++) {
                Thread t = threadFactory.newThread(this.getSyncer());
                syncerThreads.add(t);
            }

            try {
                // Starting the threads after having created them so that we can interrupt all threads in the finally
                for (Thread t : syncerThreads) {
                    t.start();
                }

                feedTheMonsters();
            } catch (Throwable e) {
                if (!closed) {
                    // Only gack if the close was not "planned", i.e. the thread is stopping without close() being explicitly called.
                    new SearchServerGack(GackLevel.SEVERE,
                            SearchServerGack.SearchServerGackSubject.BLOB_SYNC_FEEDER_EXITING,
                            "No more core syncs will will be done with Blob store. This searchserver should likely be restarted.",
                            "", e).send();
                }
            } finally {
                // If we stop, have our syncer "thread pool" stop as well since there's not much they can do anyway
                // then...
                for (Thread t : syncerThreads) {
                    t.interrupt();
                }
            }

        } finally {
            this.executionThread = null;
        }
    }

    boolean shouldContinueRunning() {
        // Theoretically shouldContinue should get set to false before the CoreContainer is shutdown,
        // but just in case we'll check both.
        return shouldContinue.get() && !this.cores.isShutDown();
    }

    @Override
    public void close() {
        closed = true;

        Thread thread = this.executionThread;
        if (thread != null) {
            this.executionThread = null; // race to set to null but ok to try to interrupt twice
            logger.log(Level.INFO, null,
                    String.format("Closing CoreSyncFeeder; interrupting execution thread %s.", thread.getName()));
            thread.interrupt();
        } else {
            logger.log(Level.WARNING, null, "Closing CoreSyncFeeder before any syncer thread was started. Weird.");
        }
    }

    abstract String getMonsterThreadName();

    abstract void feedTheMonsters() throws InterruptedException;

    abstract Runnable getSyncer();

}
