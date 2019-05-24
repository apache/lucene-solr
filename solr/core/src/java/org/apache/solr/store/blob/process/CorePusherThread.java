package org.apache.solr.store.blob.process;

import searchserver.blobstore.util.DeduplicatingList;
import searchserver.logging.SearchLogger;

import java.util.logging.Level;

/**
 * A thread (there are a few of these created in {@link CorePusherFeeder#run}) that dequeues {@link CorePushTask} from
 * a {@link DeduplicatingList} and executes them forever or until interrupted (whichever comes first). The {@link DeduplicatingList}
 * is fed by {@link CorePusherFeeder} getting its data from {@link CoreUpdateTracker} via a different {@link DeduplicatingList}.
 *
 * @author iginzburg
 * @since 214/solr.6
 */
public class CorePusherThread implements Runnable {
    private static final SearchLogger logger = new SearchLogger(CorePusherThread.class);

    private final DeduplicatingList<String, CorePushTask> workQueue;
    private final CorePusherFeeder pusherFeeder;

    CorePusherThread(CorePusherFeeder pusherFeeder, DeduplicatingList<String, CorePushTask> workQueue) {
        this.workQueue = workQueue;
        this.pusherFeeder = pusherFeeder;
    }

    @Override
    public void run() {
            // Thread runs until interrupted (which is the right way to tell a thread to stop running)
            while (true) {
                CorePushTask task = null;
                try {
                    // This call blocks if work queue is empty
                    task = workQueue.removeFirst();

                    task.pushCoreToBlob();
                } catch (InterruptedException ie) {
                    logger.log(Level.INFO, null, "Pusher thread " + Thread.currentThread().getName()
                            + " got interrupted. Shutting down Blob CorePusherFeeder.");

                    // Stop the pusher feeder that will close the other threads and reinterrupt ourselves
                    pusherFeeder.close();
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    // Exceptions other than InterruptedException should not stop the business
                    String taskInfo = task == null ? "" : String.format("Attempt=%s to push core %s ", task.getAttempts(), task.getCoreName()) ;
                    logger.log(Level.WARNING, null, "CorePusherThread encountered a failure. " + taskInfo, e);
                }
            }
    }
}
