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

import org.apache.solr.store.blob.util.DeduplicatingList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.lang.invoke.MethodHandles;

/**
 * A thread (there are a few of these created in {@link CorePullerFeeder#run}) that dequeues {@link CorePullTask} from a
 * {@link DeduplicatingList} and executes them forever or until interrupted (whichever comes first). The
 * {@link DeduplicatingList} is fed by {@link CorePullerFeeder} getting its data from {@link CorePullTracker} via a
 * different {@link DeduplicatingList}.
 */
public class CorePullerThread implements Runnable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

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
        log.info("Puller thread " + Thread.currentThread().getName()
            + " got interrupted. Shutting down Blob CorePullerFeeder if not already.");

        // Stop the puller feeder that will close the other threads and re-interrupt ourselves
        pullerFeeder.close();
        Thread.currentThread().interrupt();
        break;
      } catch (Exception e) {
        // Exceptions other than InterruptedException should not stop the business
        String taskInfo = task == null ? "" : String.format("Attempt=%s to pull core %s ", task.getAttempts(), task.getPullCoreInfo().getSharedStoreName()) ;
        log.warn("CorePullerThread encountered a failure. " + taskInfo, e);
      }
    }
  }
}
