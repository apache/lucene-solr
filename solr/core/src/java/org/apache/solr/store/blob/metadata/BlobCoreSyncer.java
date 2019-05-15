package org.apache.solr.store.blob.metadata;

import java.util.*;
import java.util.concurrent.*;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
//import net.jcip.annotations.GuardedBy;
import org.apache.solr.common.SolrException;
import org.apache.solr.store.blob.client.BlobCoreMetadata;
import org.apache.solr.store.blob.PullInProgressException;
import org.apache.solr.store.blob.process.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.lang.invoke.MethodHandles;

/**
 * Class to sync between local and blob copies of a core using {@link org.apache.solr.store.blob.metadata.CorePushPull}
 *
 * @author msiddavanahalli
 * @since 214/solr.6
 */
public class BlobCoreSyncer {

    /**
     * Threads wait for at most this duration before giving up on async pull to finish and returning with a PullInProgressException.
     */
    public static final long PULL_WAITING_MS = TimeUnit.SECONDS.toMillis(5);

    /**
     * Max number of threads for a core that can concurrently wait for the pull
     * to complete instead of returning a PullInProgressException right away
     */
    public static final int MAX_PULL_WAITING_PER_CORE = 5;

    /**
     * Allow at most that number of threads to wait for async pulls to finish over all cores instead of returning
     * a PullInProgressException right away.
     */
    public static final int MAX_PULL_WAITING_TOTAL = 20;

    /** "Skipping pulling core" string is checked on Core App in SolrExchange.onResponseComplete()
     * and defined there in constant SolrExchange.CORE_BEING_PULLED. Change here, change there!
     * Use this string as prefix of any message returned in a PullInProgressException.
     */
    private static final String SKIPPING_PULLING_CORE = "Skipping pulling core";

    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    /** Core names currently being pulled from blob. Value is collection of objects used for synchronization by all waiting threads.
     * If both the locks on this map and on a specific SyncOnPullWait in the map are needed, the lock on the map must be acquired first.
     */
    // @GuardedBy("itself")
    private static final Map<String, Collection<SyncOnPullWait>> coreSyncsInFlight = Maps.newHashMap();

    // @GuardedBy("coreSyncsInFlight")
    private static int total_waiting_threads = 0;

    /**
     * @return Total number of threads across all cores waiting for their respective core to be pulled from blob store
     */
    @VisibleForTesting
    protected static int getTotalWaitingThreads() {
        synchronized (coreSyncsInFlight) {
            return total_waiting_threads;
        }
    }

    /**
     * Each thread waiting for async pull to complete uses its own new instance of this class. This is done
     * so that the pulling thread upon completion can do two things:
     * <ol><li>
     *    Notify each waiting (or about to wait) thread that the pull is done,
     * </li><li>
     *    Pass pull results to the waiting thread (for example indicate if an exception was thrown and the pull failed).
     * </li></ol>
     * A data structure in which all waiter threads share the same lock object (and the same flag/data indicating completion
     * and its result) can be considered simpler but has the problem of when can this data structure be freed, often resorting
     * to relatively complex code using weak references.<p>
     * In the implementation done here, the entry in the map of cores to "waiting lists" ({@link BlobCoreSyncer#coreSyncsInFlight})
     * can be removed at any time once all waiter threads have been notified, then the actual data structure for each
     * waiter will be reclaimable by the JVM when the waiter has finished using it, without complex tricks.
     */
    private static class SyncOnPullWait {
        private final CountDownLatch latch = new CountDownLatch(1);
        private Exception exception = null; // If non null once latch is counted down (to 0), exception encountered by pulling thread
    }

    /**
     * Returns a _hint_ that the given core might be locally empty because it is awaiting pull from Blob store.
     * This is just a hint because as soon as the lock is released when the method returns, the status of the core could change.
     * Because of that, in method {@link #pull(String, String, boolean, boolean)} we need to check again.
     */
    public static boolean isEmptyCoreAwaitingPull(String coreName) {
        return CorePullTask.isEmptyCoreAwaitingPull(coreName);
    }

    /**
     * This method is used to pull in updates from blob when there is no local copy of the core available and when a thread
     * needs the core to be available (i.e. waits for it).<p>
     *
     * A parameter <code>waitForSearcher</code> is therefore added and is always <code>true</code>
     * as of today, but it is added so any additional calls to this method need to decide if they want wait for searcher or not.
     * Note that when multiple threads wait for the same core, currently only the puller will create the index searcher and
     * either wait for it to be available (waitForSearcher) or not (!waitForSearcher). If another thread waiting for the pull to complete wanted
     * a waitForSearcher and the puller thread did not, the code should be modified. This doesn't happen as of today because this
     * method is always called with waitForSearcher being true...
     *
     * TODO if Blob is not available on pull, we might treat this as a missing core exception from Core app and reindex. Likely need to treat Blob unavailable differently from no such core on blob
     *
     * @param emptyCoreAwaitingPull <code>true</code> if this pull method is called after the core got created empty locally
     *                              but before the actual pull happened, in which case this method is called to wait until
     *                              the pull finishes to avoid erroneously returning no results.
     *
     * @throws PullInProgressException In case a thread does not wait or times out before the async pull is finished
     */
    public void pull(String corename, String collectionName, boolean waitForSearcher, boolean emptyCoreAwaitingPull) throws PullInProgressException {
        // Is there another thread already working on the async pull?
        final boolean pullAlreadyInProgress;
        // Indicates if thread waits for the pull to finish or too many waiters already
        final boolean iWait;
        // When iWait (is true), myWait is the object to use for the wait. Note pull thread might have completed before
        // we start wait, so we need to test the state of the myWait object first.
        final SyncOnPullWait myWait;

        // Capturing the number of waiting threads to log the reason we can't wait... This is mostly for autobuild debug and likely to be removed afterwards. -1 should never be logged
        int countCoreWaiters = -1;
        int countTotalWaiters = -1;

        // Only can have only one thread working on async pull of this core (and we do no logging while holding the lock)
        // Let's understand what our role and actions are while holding the global lock and then execute on them without the lock.
        synchronized (coreSyncsInFlight) {
            if (emptyCoreAwaitingPull && !isEmptyCoreAwaitingPull(corename)) {
                // Core was observed empty awaiting pull and is no longer awaiting pull. This means the pull happened.
                return;
            }
            countTotalWaiters = total_waiting_threads; // for logging

            Collection<SyncOnPullWait> collectionOfWaiters = coreSyncsInFlight.get(corename);
            if (collectionOfWaiters != null) {
                // Somebody is already working on async pull of this core. If possible let's add ourselves to the list of those who wait (then wait)
                pullAlreadyInProgress = true;
            } else {
                // We are the first thread trying to pull this core, before releasing the lock we must setup everything
                // to let the world know and let itself and other threads be able to wait until async pull is done if/when they so decide.
                collectionOfWaiters = new ArrayList<>(MAX_PULL_WAITING_PER_CORE);
                coreSyncsInFlight.put(corename, collectionOfWaiters);
                pullAlreadyInProgress = false;
            }
            int waiters = collectionOfWaiters.size();

            countCoreWaiters = waiters; // for logging

            iWait = total_waiting_threads < MAX_PULL_WAITING_TOTAL && waiters < MAX_PULL_WAITING_PER_CORE;
            if (iWait) {
                myWait = new SyncOnPullWait();
                // Increase total waiting count and add ourselves as waiting for the current core.
                total_waiting_threads++;
                collectionOfWaiters.add(myWait);
            } else {
                // We cannot throw pull in progress exception here because this whole synchronized block is calculating
                // subsequent actions and we might need to enqueue a pull before throwing that exception i.e. (!pullAlreadyInProgress)
                //
                // We can make it work that way if we really want to(although I don't see a reason) if we separate out this waiters calculation
                // into a separate synchronized block after the enqueuing of pull request block
                myWait = null; // Not used but has to be set
            }
        }

        // Verify that emptyCoreAwaitingPull implies pullAlreadyInProgress: if the core was previously observed empty awaiting pull,
        // it means a thread is already on it (and if that thread has finished we would have returned earlier from this call)
        assert !emptyCoreAwaitingPull || pullAlreadyInProgress;

        if (!pullAlreadyInProgress) {
            // We are the first in here for that core so we get to enqueue async pull.
            // If we are successful in enqueuing then pullFinished callback will take care of notifying and clearing
            // out state around waiting threads(including this one if it end up waiting as well in the next block).
            // That callback will take care of both successful and failed pulls.
            // But if we fail in enqueuing then catch block here will declare pull as failed and will take care of notifying
            // and clearing out state around waiting threads (it is possible that another thread has already started waiting before this thread
            // get a chance to run this block)
            try {
                logger.info("About to enqueue pull of core " + corename + " (countTotalWaiters=" + countTotalWaiters + ")");

                // enqueue an async pull
                CorePullTracker.get().enqueueForPull(corename, collectionName, true, waitForSearcher);

            } catch (Exception e) {
                // as mentioned above in case of failed enqueue we are responsible for clearing up all waiting state
                notifyEndOfPull(corename, e);
                if (e instanceof InterruptedException) {
                    // We swallow the InterruptedException and spit instead a SolrException.
                    // Need to let layers higher up know an interruption happened.
                    Thread.currentThread().interrupt();
                }

                String msg = "Failed to enqueue pull of core " + corename + " from blob";
                SolrException se;
                logger.warn(msg, e);
                if (e instanceof SolrException) {
                    se = (SolrException) e;
                } else {
                    // Wrapping in SolrException to percolate all the way to the catch Throwable at end of SolrDispatchFilter.doFilter()
                    // A failed pull should not appear as a missing core otherwise Core App reindexes in another core...
                    // TODO remains to be seen if all pull's() need to have this exception thrown or if some prefer silent failures.
                    se = new SolrException(SolrException.ErrorCode.SERVER_ERROR, msg, e);
                }
                throw se;
            }
        }

        // Now irrespective of us being a thread who initiated an async pull or just came after that, we might have
        // qualified for waiting for async pull to finish. If so we will just do that.
        if (iWait) {
            try {
                logger.info("About to wait for pull of core " + corename + " (countCoreWaiters=" + countCoreWaiters + " countTotalWaiters=" + countTotalWaiters + ")");

                // Let's wait a bit maybe the pull completes in which case we don't have to throw an exception back.
                // The other end of this lock activity happens in notifyEndOfPull() below
                try {
                    // The pull might have completed by now (i.e. completed since we left the synchronized(coreSyncsInFlight) block)
                    // and that's ok the await() below will not block if not needed
                    myWait.latch.await(PULL_WAITING_MS, TimeUnit.MILLISECONDS);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Interrupted while waiting for core pull " + corename, ie);
                }

                // Our wait finished...
                // Memory consistency effects of CountDownLatch: Until the count reaches zero, actions in a thread prior
                // to calling countDown() happen-before actions following a successful return from a corresponding await() in another thread.
                // We'll therefore see any updates done to the myWait object by the puller thread if it has finished pulling.
                if (myWait.latch.getCount() != 0) {
                    throwPullInProgressException(corename, "still in progress and didn't complete while we waited");
                } else {
                    // Pull has completed. Success or failure?
                    if (myWait.exception != null) {
                        // We wrap in a SolrException the original exception from the puller thread.
                        // (even if it's already a SolrException to make understanding what happens at runtime easier)
                        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Puller thread failed to pull", myWait.exception);
                    }

                    // The pull has finished during our wait.
                }
            } finally {
                // if wait is ended because of notifyEndOfPull() then it would have taken care of this state already and this essentially will be a no op.
                // But if it has ended because of some other reason e.g. InterruptedException, then we will remove this thread from list of waiters.
                //
                // pullFinished notification may come later but that should be ok since it will only see waiters that are still waiting or none.
                synchronized (coreSyncsInFlight) {
                    Collection<SyncOnPullWait> collectionOfWaiters = coreSyncsInFlight.get(corename);
                    if(collectionOfWaiters != null) {
                        if(collectionOfWaiters.remove(myWait)){
                            total_waiting_threads--;
                        }
                    }
                }
            }
        } else {
            // Pull is in progress and we're not waiting for it to finish.
            throwPullInProgressException(corename, "as it is already in progress and enough threads waiting");
        }
    }

    /**
     * This is called whenever core from {@link CorePullTracker} finish its async pull(successfully or unsuccessfully)
     * We use this to notify all waiting threads for a core that their wait has ended (if there are some waiting).
     */
    public static void finishedPull(String coreName, CoreSyncStatus status, BlobCoreMetadata blobMetadata, String message) {
        Exception pullException = null;
        final boolean isPullSuccessful = (status.isSuccess() ||
                // Following statuses are not considered success in strictest definition of pull but for BlobSyncer
                // they are not error either and we would let the normal query flow do its job (likely return missing core exception)
                status == CoreSyncStatus.BLOB_MISSING ||
                status == CoreSyncStatus.BLOB_DELETED_FOR_PULL ||
                // TODO:  likely changes needed here for W-5388477 Blob store corruption repair
                status == CoreSyncStatus.BLOB_CORRUPT);
        if (!isPullSuccessful) {
            pullException = new SolrException(SolrException.ErrorCode.SERVER_ERROR, message);
        }
        notifyEndOfPull(coreName, pullException);
    }

    private void throwPullInProgressException(String corename, String msgSuffix) throws PullInProgressException {
        String msg = SKIPPING_PULLING_CORE + " " + corename + " from blob " + msgSuffix;
        logger.info(msg);
        // Note that longer term, this is the place where we could decide that if the async
        // pull was enqueued too long ago and nothing happened, then there's an issue worth gacking/alerting for.
        throw new PullInProgressException(msg);

    }

    /**
     * Called by the puller thread once pull has completed (successfully or not), to notify all waiter threads of the issue
     * of the pull and to clean up this core in bookkeeping related to in-progress pulls({@link #pullEnded(String)}).
     * Also serves the purpose of being a memory barrier so that the waiting threads can check their SyncOnPullWait instances
     * for updates.
     */
    private static void notifyEndOfPull(String corename, Exception e) {
        final Collection<SyncOnPullWait> collectionOfWaiters = pullEnded(corename);
        if (collectionOfWaiters != null) {
            for (SyncOnPullWait w : collectionOfWaiters) {
                // Need to set the exception before counting down on the latch because of the memory barrier effect of countDown()
                w.exception = e;
                w.latch.countDown();
            }
        }
    }

    /**
     * Cleans up the core from bookkeeping related to in-progress pulls and returns the collection of waiters for that core.
     * Collection of returned waiters could be null as well.
     */
    private static Collection<SyncOnPullWait> pullEnded(String corename) {
        final Collection<SyncOnPullWait> collectionOfWaiters;
        synchronized (coreSyncsInFlight) {
            // Note that threads waiting for the pull to finish have references on their individual SyncOnPullWait instances,
            // so removing the entry from coreSyncsInFlight before the waiter threads are done waiting is ok.
            collectionOfWaiters = coreSyncsInFlight.remove(corename);
            if(collectionOfWaiters != null) {
                total_waiting_threads -= collectionOfWaiters.size();
            }
        }
        return collectionOfWaiters;
    }
}