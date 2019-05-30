package org.apache.solr.store.blob.process;

import org.apache.solr.store.blob.util.DeduplicatingList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.lang.invoke.MethodHandles;

/**
 * A thread (there are a few of these created in {@link CorePullerFeeder#run}) that dequeues {@link CorePullTask} from a
 * {@link DeduplicatingList} and executes them forever or until interrupted (whichever comes first). The
 * {@link DeduplicatingList} is fed by {@link CorePullerFeeder} getting its data from {@link CorePullTracker} via a
 * different {@link DeduplicatingList}.
 *
 * @author msidavanahalli
 * @since 214/solr.6
 */
public class CorePullerThread implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final DeduplicatingList<String, CorePullTask> workQueue;
    private final CorePullerFeeder pullerFeeder;

    CorePullerThread(CorePullerFeeder pullerFeeder, DeduplicatingList<String, CorePullTask> workQueue) {
        this.workQueue = workQueue;
        this.pullerFeeder = pullerFeeder;
    }

    @Override
    public void run() {
            // Thread runs until interrupted (which is the right way to tell a thread to stop running)
            while (true) {
                CorePullTask task = null;
                try {
                    // This call blocks if work queue is empty
                    task = workQueue.removeFirst();
                    // TODO: we should timebox this request in case we are stuck for long time
                    task.pullCoreFromBlob();

                } catch (InterruptedException ie) {
                    logger.info("Puller thread " + Thread.currentThread().getName()
                            + " got interrupted. Shutting down Blob CorePullerFeeder.");

                    // Stop the puller feeder that will close the other threads and reinterrupt ourselves
                    pullerFeeder.close();
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    // Exceptions other than InterruptedException should not stop the business
                    String taskInfo = task == null ? "" : String.format("Attempt=%s to pull core %s ", task.getAttempts(), task.getPullCoreInfo().getSharedStoreName()) ;
                    logger.warn("CorePullerThread encountered a failure. " + taskInfo, e);
                }
        }
    }
}
