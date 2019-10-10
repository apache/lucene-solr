/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.store.blob.process;

import java.io.Closeable;
import java.lang.invoke.MethodHandles;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.exception.ExceptionUtils;
//import com.force.commons.util.concurrent.NamedThreadFactory; difference?
import org.apache.lucene.util.NamedThreadFactory;
import org.apache.solr.core.CoreContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link Runnable} that will start a set of threads {@link CorePullerThread} to process tasks
 * pulling local core.
 */
public abstract class CoreSyncFeeder implements Runnable, Closeable {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    protected final CoreContainer cores;

    /**
     * Maximum number of elements in the queue, NOT counting re-inserts after failures. Total queue size might therefore
     * exceed this value by the number of syncThreads which is around 5 or 10...
     * <p>
     * Note that this queue sits behind other tracking queues (see {@link CoreUpdateTracker} and
     * {@link CorePullTracker}). The other queue has to be large, this one does not.
     */
    protected static final int ALMOST_MAX_WORKER_QUEUE_SIZE = 2000;

    /**
     * When a transient error occurs, the number of attempts to sync with Blob before giving up. Attempts are spaced by
     * at least 10 seconds (see {@link CorePullTask#MIN_RETRY_DELAY_MS}) which means we'll retry for at least 90 seconds 
     * before giving up. This is something to adjust as the implementation of the delay between retries is cleaned up, 
     * see {@link CorePullTask}.
     */
    protected static final int MAX_ATTEMPTS = 10;

    private final int numSyncThreads;

    /**
     * Used to interrupt close()
     */
    private volatile Thread executionThread;
    private volatile boolean closed = false;

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
            NamedThreadFactory threadFactory = new NamedThreadFactory(getMonsterThreadName());
            // TODO set daemon?
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
                  log.error("CoreSyncFeeder thread encountered an error and is exiting "
                      + "while close() was not called. " + ExceptionUtils.getStackTrace(e));
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
      return !this.cores.isShutDown();
    }

    @Override
    public void close() {
      if (!closed) {
        closed = true;
        Thread thread = this.executionThread;
        if (thread != null) {
          this.executionThread = null; // race to set to null but ok to try to interrupt twice
          log.info(String.format("Closing CoreSyncFeeder; interrupting execution thread %s.", thread.getName()));
          thread.interrupt();
        } else {
          log.warn("Closing CoreSyncFeeder before any syncer thread was started. Weird.");
        }
      }
    }

    abstract String getMonsterThreadName();

    abstract void feedTheMonsters() throws InterruptedException;

    abstract Runnable getSyncer();

}
