package org.apache.lucene.index;

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

import org.apache.lucene.store.Directory;
import org.apache.lucene.util.ThreadInterruptedException;
import org.apache.lucene.util.CollectionUtil;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.Comparator;

/** A {@link MergeScheduler} that runs each merge using a
 *  separate thread.
 *
 *  <p>Specify the max number of threads that may run at
 *  once with {@link #setMaxThreadCount}.</p>
 *
 *  <p>Separately specify the maximum number of simultaneous
 *  merges with {@link #setMaxMergeCount}.  If the number of
 *  merges exceeds the max number of threads then the
 *  largest merges are paused until one of the smaller
 *  merges completes.</p>
 *
 *  <p>If more than {@link #getMaxMergeCount} merges are
 *  requested then this class will forcefully throttle the
 *  incoming threads by pausing until one more more merges
 *  complete.</p>
 */ 
public class ConcurrentMergeScheduler extends MergeScheduler {

  private int mergeThreadPriority = -1;

  /** List of currently active {@link MergeThread}s. */
  protected List<MergeThread> mergeThreads = new ArrayList<MergeThread>();

  // Max number of merge threads allowed to be running at
  // once.  When there are more merges then this, we
  // forcefully pause the larger ones, letting the smaller
  // ones run, up until maxMergeCount merges at which point
  // we forcefully pause incoming threads (that presumably
  // are the ones causing so much merging).  We default to 1
  // here: tests on spinning-magnet drives showed slower
  // indexing perf if more than one merge thread runs at
  // once (though on an SSD it was faster):
  private int maxThreadCount = 1;

  // Max number of merges we accept before forcefully
  // throttling the incoming threads
  private int maxMergeCount = 2;

  /** {@link Directory} that holds the index. */
  protected Directory dir;

  /** {@link IndexWriter} that owns this instance. */
  protected IndexWriter writer;

  /** How many {@link MergeThread}s have kicked off (this is use
   *  to name them). */
  protected int mergeThreadCount;

  /** Sole constructor, with all settings set to default
   *  values. */
  public ConcurrentMergeScheduler() {
  }

  /** Sets the max # simultaneous merge threads that should
   *  be running at once.  This must be <= {@link
   *  #setMaxMergeCount}. */
  public void setMaxThreadCount(int count) {
    if (count < 1) {
      throw new IllegalArgumentException("count should be at least 1");
    }
    if (count > maxMergeCount) {
      throw new IllegalArgumentException("count should be <= maxMergeCount (= " + maxMergeCount + ")");
    }
    maxThreadCount = count;
  }

  /** Returns {@code maxThreadCount}.
   *
   * @see #setMaxThreadCount(int) */
  public int getMaxThreadCount() {
    return maxThreadCount;
  }

  /** Sets the max # simultaneous merges that are allowed.
   *  If a merge is necessary yet we already have this many
   *  threads running, the incoming thread (that is calling
   *  add/updateDocument) will block until a merge thread
   *  has completed.  Note that we will only run the
   *  smallest {@link #setMaxThreadCount} merges at a time. */
  public void setMaxMergeCount(int count) {
    if (count < 1) {
      throw new IllegalArgumentException("count should be at least 1");
    }
    if (count < maxThreadCount) {
      throw new IllegalArgumentException("count should be >= maxThreadCount (= " + maxThreadCount + ")");
    }
    maxMergeCount = count;
  }

  /** See {@link #setMaxMergeCount}. */
  public int getMaxMergeCount() {
    return maxMergeCount;
  }

  /** Return the priority that merge threads run at.  By
   *  default the priority is 1 plus the priority of (ie,
   *  slightly higher priority than) the first thread that
   *  calls merge. */
  public synchronized int getMergeThreadPriority() {
    initMergeThreadPriority();
    return mergeThreadPriority;
  }

  /** Set the base priority that merge threads run at.
   *  Note that CMS may increase priority of some merge
   *  threads beyond this base priority.  It's best not to
   *  set this any higher than
   *  Thread.MAX_PRIORITY-maxThreadCount, so that CMS has
   *  room to set relative priority among threads.  */
  public synchronized void setMergeThreadPriority(int pri) {
    if (pri > Thread.MAX_PRIORITY || pri < Thread.MIN_PRIORITY)
      throw new IllegalArgumentException("priority must be in range " + Thread.MIN_PRIORITY + " .. " + Thread.MAX_PRIORITY + " inclusive");
    mergeThreadPriority = pri;
    updateMergeThreads();
  }

  /** Sorts {@link MergeThread}s; larger merges come first. */
  protected static final Comparator<MergeThread> compareByMergeDocCount = new Comparator<MergeThread>() {
    @Override
    public int compare(MergeThread t1, MergeThread t2) {
      final MergePolicy.OneMerge m1 = t1.getCurrentMerge();
      final MergePolicy.OneMerge m2 = t2.getCurrentMerge();
      
      final int c1 = m1 == null ? Integer.MAX_VALUE : m1.totalDocCount;
      final int c2 = m2 == null ? Integer.MAX_VALUE : m2.totalDocCount;

      return c2 - c1;
    }
  };

  /**
   * Called whenever the running merges have changed, to pause & unpause
   * threads. This method sorts the merge threads by their merge size in
   * descending order and then pauses/unpauses threads from first to last --
   * that way, smaller merges are guaranteed to run before larger ones.
   */
  protected synchronized void updateMergeThreads() {

    // Only look at threads that are alive & not in the
    // process of stopping (ie have an active merge):
    final List<MergeThread> activeMerges = new ArrayList<MergeThread>();

    int threadIdx = 0;
    while (threadIdx < mergeThreads.size()) {
      final MergeThread mergeThread = mergeThreads.get(threadIdx);
      if (!mergeThread.isAlive()) {
        // Prune any dead threads
        mergeThreads.remove(threadIdx);
        continue;
      }
      if (mergeThread.getCurrentMerge() != null) {
        activeMerges.add(mergeThread);
      }
      threadIdx++;
    }

    // Sort the merge threads in descending order.
    CollectionUtil.mergeSort(activeMerges, compareByMergeDocCount);
    
    int pri = mergeThreadPriority;
    final int activeMergeCount = activeMerges.size();
    for (threadIdx=0;threadIdx<activeMergeCount;threadIdx++) {
      final MergeThread mergeThread = activeMerges.get(threadIdx);
      final MergePolicy.OneMerge merge = mergeThread.getCurrentMerge();
      if (merge == null) { 
        continue;
      }

      // pause the thread if maxThreadCount is smaller than the number of merge threads.
      final boolean doPause = threadIdx < activeMergeCount - maxThreadCount;

      if (verbose()) {
        if (doPause != merge.getPause()) {
          if (doPause) {
            message("pause thread " + mergeThread.getName());
          } else {
            message("unpause thread " + mergeThread.getName());
          }
        }
      }
      if (doPause != merge.getPause()) {
        merge.setPause(doPause);
      }

      if (!doPause) {
        if (verbose()) {
          message("set priority of merge thread " + mergeThread.getName() + " to " + pri);
        }
        mergeThread.setThreadPriority(pri);
        pri = Math.min(Thread.MAX_PRIORITY, 1+pri);
      }
    }
  }

  /**
   * Returns true if verbosing is enabled. This method is usually used in
   * conjunction with {@link #message(String)}, like that:
   * 
   * <pre class="prettyprint">
   * if (verbose()) {
   *   message(&quot;your message&quot;);
   * }
   * </pre>
   */
  protected boolean verbose() {
    return writer != null && writer.infoStream.isEnabled("CMS");
  }
  
  /**
   * Outputs the given message - this method assumes {@link #verbose()} was
   * called and returned true.
   */
  protected void message(String message) {
    writer.infoStream.message("CMS", message);
  }

  private synchronized void initMergeThreadPriority() {
    if (mergeThreadPriority == -1) {
      // Default to slightly higher priority than our
      // calling thread
      mergeThreadPriority = 1+Thread.currentThread().getPriority();
      if (mergeThreadPriority > Thread.MAX_PRIORITY)
        mergeThreadPriority = Thread.MAX_PRIORITY;
    }
  }

  @Override
  public void close() {
    sync();
  }

  /** Wait for any running merge threads to finish. This call is not interruptible as used by {@link #close()}. */
  public void sync() {
    boolean interrupted = false;
    try {
      while (true) {
        MergeThread toSync = null;
        synchronized (this) {
          for (MergeThread t : mergeThreads) {
            if (t.isAlive()) {
              toSync = t;
              break;
            }
          }
        }
        if (toSync != null) {
          try {
            toSync.join();
          } catch (InterruptedException ie) {
            // ignore this Exception, we will retry until all threads are dead
            interrupted = true;
          }
        } else {
          break;
        }
      }
    } finally {
      // finally, restore interrupt status:
      if (interrupted) Thread.currentThread().interrupt();
    }
  }

  /**
   * Returns the number of merge threads that are alive. Note that this number
   * is &le; {@link #mergeThreads} size.
   */
  protected synchronized int mergeThreadCount() {
    int count = 0;
    for (MergeThread mt : mergeThreads) {
      if (mt.isAlive() && mt.getCurrentMerge() != null) {
        count++;
      }
    }
    return count;
  }

  @Override
  public synchronized void merge(IndexWriter writer) throws IOException {

    assert !Thread.holdsLock(writer);

    this.writer = writer;

    initMergeThreadPriority();

    dir = writer.getDirectory();

    // First, quickly run through the newly proposed merges
    // and add any orthogonal merges (ie a merge not
    // involving segments already pending to be merged) to
    // the queue.  If we are way behind on merging, many of
    // these newly proposed merges will likely already be
    // registered.

    if (verbose()) {
      message("now merge");
      message("  index: " + writer.segString());
    }
    
    // Iterate, pulling from the IndexWriter's queue of
    // pending merges, until it's empty:
    while (true) {

      long startStallTime = 0;
      while (writer.hasPendingMerges() && mergeThreadCount() >= maxMergeCount) {
        // This means merging has fallen too far behind: we
        // have already created maxMergeCount threads, and
        // now there's at least one more merge pending.
        // Note that only maxThreadCount of
        // those created merge threads will actually be
        // running; the rest will be paused (see
        // updateMergeThreads).  We stall this producer
        // thread to prevent creation of new segments,
        // until merging has caught up:
        startStallTime = System.currentTimeMillis();
        if (verbose()) {
          message("    too many merges; stalling...");
        }
        try {
          wait();
        } catch (InterruptedException ie) {
          throw new ThreadInterruptedException(ie);
        }
      }

      if (verbose()) {
        if (startStallTime != 0) {
          message("  stalled for " + (System.currentTimeMillis()-startStallTime) + " msec");
        }
      }

      MergePolicy.OneMerge merge = writer.getNextMerge();
      if (merge == null) {
        if (verbose()) {
          message("  no more merges pending; now return");
        }
        return;
      }

      boolean success = false;
      try {
        if (verbose()) {
          message("  consider merge " + writer.segString(merge.segments));
        }

        // OK to spawn a new merge thread to handle this
        // merge:
        final MergeThread merger = getMergeThread(writer, merge);
        mergeThreads.add(merger);
        if (verbose()) {
          message("    launch new thread [" + merger.getName() + "]");
        }

        merger.start();

        // Must call this after starting the thread else
        // the new thread is removed from mergeThreads
        // (since it's not alive yet):
        updateMergeThreads();

        success = true;
      } finally {
        if (!success) {
          writer.mergeFinish(merge);
        }
      }
    }
  }

  /** Does the actual merge, by calling {@link IndexWriter#merge} */
  protected void doMerge(MergePolicy.OneMerge merge) throws IOException {
    writer.merge(merge);
  }

  /** Create and return a new MergeThread */
  protected synchronized MergeThread getMergeThread(IndexWriter writer, MergePolicy.OneMerge merge) throws IOException {
    final MergeThread thread = new MergeThread(writer, merge);
    thread.setThreadPriority(mergeThreadPriority);
    thread.setDaemon(true);
    thread.setName("Lucene Merge Thread #" + mergeThreadCount++);
    return thread;
  }

  /** Runs a merge thread, which may run one or more merges
   *  in sequence. */
  protected class MergeThread extends Thread {

    IndexWriter tWriter;
    MergePolicy.OneMerge startMerge;
    MergePolicy.OneMerge runningMerge;
    private volatile boolean done;

    /** Sole constructor. */
    public MergeThread(IndexWriter writer, MergePolicy.OneMerge startMerge) {
      this.tWriter = writer;
      this.startMerge = startMerge;
    }

    /** Record the currently running merge. */
    public synchronized void setRunningMerge(MergePolicy.OneMerge merge) {
      runningMerge = merge;
    }

    /** Return the currently running merge. */
    public synchronized MergePolicy.OneMerge getRunningMerge() {
      return runningMerge;
    }

    /** Return the current merge, or null if this {@code
     *  MergeThread} is done. */
    public synchronized MergePolicy.OneMerge getCurrentMerge() {
      if (done) {
        return null;
      } else if (runningMerge != null) {
        return runningMerge;
      } else {
        return startMerge;
      }
    }

    /** Set the priority of this thread. */
    public void setThreadPriority(int pri) {
      try {
        setPriority(pri);
      } catch (NullPointerException npe) {
        // Strangely, Sun's JDK 1.5 on Linux sometimes
        // throws NPE out of here...
      } catch (SecurityException se) {
        // Ignore this because we will still run fine with
        // normal thread priority
      }
    }

    @Override
    public void run() {
      
      // First time through the while loop we do the merge
      // that we were started with:
      MergePolicy.OneMerge merge = this.startMerge;
      
      try {

        if (verbose()) {
          message("  merge thread: start");
        }

        while(true) {
          setRunningMerge(merge);
          doMerge(merge);

          // Subsequent times through the loop we do any new
          // merge that writer says is necessary:
          merge = tWriter.getNextMerge();

          // Notify here in case any threads were stalled;
          // they will notice that the pending merge has
          // been pulled and possibly resume:
          synchronized(ConcurrentMergeScheduler.this) {
            ConcurrentMergeScheduler.this.notifyAll();
          }

          if (merge != null) {
            updateMergeThreads();
            if (verbose()) {
              message("  merge thread: do another merge " + tWriter.segString(merge.segments));
            }
          } else {
            break;
          }
        }

        if (verbose()) {
          message("  merge thread: done");
        }

      } catch (Throwable exc) {

        // Ignore the exception if it was due to abort:
        if (!(exc instanceof MergePolicy.MergeAbortedException)) {
          //System.out.println(Thread.currentThread().getName() + ": CMS: exc");
          //exc.printStackTrace(System.out);
          if (!suppressExceptions) {
            // suppressExceptions is normally only set during
            // testing.
            handleMergeException(exc);
          }
        }
      } finally {
        done = true;
        synchronized(ConcurrentMergeScheduler.this) {
          updateMergeThreads();
          ConcurrentMergeScheduler.this.notifyAll();
        }
      }
    }
  }

  /** Called when an exception is hit in a background merge
   *  thread */
  protected void handleMergeException(Throwable exc) {
    try {
      // When an exception is hit during merge, IndexWriter
      // removes any partial files and then allows another
      // merge to run.  If whatever caused the error is not
      // transient then the exception will keep happening,
      // so, we sleep here to avoid saturating CPU in such
      // cases:
      Thread.sleep(250);
    } catch (InterruptedException ie) {
      throw new ThreadInterruptedException(ie);
    }
    throw new MergePolicy.MergeException(exc, dir);
  }

  private boolean suppressExceptions;

  /** Used for testing */
  void setSuppressExceptions() {
    suppressExceptions = true;
  }

  /** Used for testing */
  void clearSuppressExceptions() {
    suppressExceptions = false;
  }
  
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(getClass().getSimpleName() + ": ");
    sb.append("maxThreadCount=").append(maxThreadCount).append(", ");    
    sb.append("maxMergeCount=").append(maxMergeCount).append(", ");    
    sb.append("mergeThreadPriority=").append(mergeThreadPriority);
    return sb.toString();
  }

  @Override
  public MergeScheduler clone() {
    ConcurrentMergeScheduler clone = (ConcurrentMergeScheduler) super.clone();
    clone.writer = null;
    clone.dir = null;
    clone.mergeThreads = new ArrayList<MergeThread>();
    return clone;
  }
}
