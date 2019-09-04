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

package org.apache.lucene.index;

import java.io.IOException;
import java.util.Observable;
import java.util.Observer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.util.ThreadInterruptedException;

/**
 * A variant of CMS: If there are cheap merges, wait for a merge to complete before continuing. This
 * has the benefit of greatly increasing the odds that an {@link org.apache.lucene.search.IndexSearcher}
 * will see fewer segments. Normally, CMS does all merges concurrently, and so it won't be until the
 * next commit that the IndexSearcher benefits from fewer segments. The trade-off is less
 * concurrency, and there will be some delay on a segment flush for a cheap merge if present.
 *
 * @author dsmiley
 * @since solr.7
 */
public class SometimesConcurrentMergeScheduler extends ConcurrentMergeScheduler {

  private long cheapMergeThresholdBytes = 2 * 1024 * 1024; // 2MB
  private long cheapMaxMergeWaitTimeoutMs = TimeUnit.SECONDS.toMillis(2);

  public long getCheapMergeThresholdBytes() {
    return cheapMergeThresholdBytes;
  }

  /** Limit cheap merges to those less than this size in megabytes. */
  public SometimesConcurrentMergeScheduler setCheapMergeThresholdMB(double v) {
    v *= 1024 * 1024;
    cheapMergeThresholdBytes = (v > Long.MAX_VALUE) ? Long.MAX_VALUE : (long) v;
    return this;
  }

  /**
   * If a cheap merge is taking longer than this amount time to finish, give up waiting for it. If
   * this threshold is reached often, then the system should probably be tuned further.
   */
  public SometimesConcurrentMergeScheduler setCheapMaxMergeWaitTimeoutMs(long cheapMaxMergeWaitTimeoutMs) {
    this.cheapMaxMergeWaitTimeoutMs = cheapMaxMergeWaitTimeoutMs;
    return this;
  }

  /** Mechanism to notify when merges complete. */
  private final Observable observableMerge = new Observable() {
    @Override
    public void notifyObservers(Object arg) {
      super.setChanged(); // setChanged is protected, and "changed" must be set for notifyObservers to do anything
      super.notifyObservers(arg);
    }
  };

  @Override
  public void merge(IndexWriter writer, MergeTrigger trigger, boolean newMergesFound) throws IOException {
    // short-circuit
    if (trigger != MergeTrigger.FULL_FLUSH || !writer.hasPendingMerges()) {
      super.merge(writer, trigger, newMergesFound); // note: might actually do stuff; like change thread priority
      return;
    }

    // Unfortunately Lucene doesn't give us a way to "peek" without taking the next merge.  And we can't
    //  pass the merge we took onto CMS if we don't want to merge it eagerly.  So instead we awkwardly
    //  hook in on the call to doMerge with an Observable, and we examine super.mergeThreads to see if
    //  there is a cheap merge.
    CountDownLatch latch = new CountDownLatch(1);
    Observer observer = (o, arg) -> latch.countDown();
    observableMerge.addObserver(observer);
    try {

      super.merge(writer, trigger, newMergesFound);
      // at this point there should be some CMS MergeThreads

      if (hasCheapMerge()) {
        if (verbose()) message("Cheap merge; waiting synchronously");
        try {
          long startTimeNs = System.nanoTime();
          if (!latch.await(cheapMaxMergeWaitTimeoutMs, TimeUnit.MILLISECONDS)) {
            // don't guard with verbose(); this is kinda like a warning
            message("WARN: reached timeout for cheap merge to complete");
          } else {
            if (verbose()) {
              long ms = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimeNs);
              message("waited " + ms + "ms for cheap merge");
              // note: the completed merge that triggered the latch could be an expensive merge if CMS is configured
              //  with multiple merge threads and if it's timed right.  It doesn't matter; the point is
              //  we waited a little for fewer segments (yay).
            }
          }
        } catch (InterruptedException e) {
          message("interrupted waiting for cheap merge");
          // This is what IndexWriter javadocs says we should do; we don't set interrupt state.
          throw new ThreadInterruptedException(e);
        }
      }
    } finally {
      observableMerge.deleteObserver(observer);
    }
  }

  private boolean hasCheapMerge() throws IOException {
    synchronized (this) {
      for (MergeThread mergeThread : mergeThreads) {
        MergePolicy.OneMerge oneMerge = mergeThread.merge;
        // Unfortunately Lucene doesn't transfer the purpose of the merge so that we can further constrain
        //   cheap merges here to a commit flush.  But I think practically speaking, if we find a "cheap"
        //   merge, it is very likely generated by our custom MergePolicy which generates such cheap
        //   merges exclusively at a commit.
        // Note: maybe MP should use a custom OneMerge subclass that we see here? But it's nice to keep decoupled and not a big deal either.
        if (!oneMerge.isExternal // unlikely but maybe we're merging external indexes
            && !oneMerge.getMergeProgress().isPaused() // if paused, don't bother waiting for it!
            && oneMerge.totalBytesSize() < cheapMergeThresholdBytes) { // is it cheap enough?
          // note: infostream logs should already note size of merges happening
          return true;
        }
      }
      return false;
    }
  }

  // called by a CMS MergeThread to do the actual merging
  @Override
  protected void doMerge(IndexWriter writer, MergePolicy.OneMerge merge) throws IOException {
    super.doMerge(writer, merge);
    observableMerge.notifyObservers(merge);
  }


}
