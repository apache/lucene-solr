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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.lucene.store.Directory;
import org.apache.lucene.util.CollectionUtil;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.ThreadInterruptedException;

/** A {@link MergeScheduler} that runs each merge using a
 *  separate thread.
 *
 *  <p>Specify the max number of threads that may run at
 *  once, and the maximum number of simultaneous merges
 *  with {@link #setMaxMergesAndThreads}.</p>
 *
 *  <p>If the number of merges exceeds the max number of threads 
 *  then the largest merges are paused until one of the smaller
 *  merges completes.</p>
 *
 *  <p>If more than {@link #getMaxMergeCount} merges are
 *  requested then this class will forcefully throttle the
 *  incoming threads by pausing until one more more merges
 *  complete.</p>
 *
 *  <p>This class attempts to detect whether the index is
 *  on rotational storage (traditional hard drive) or not
 *  (e.g. solid-state disk) and changes the default max merge
 *  and thread count accordingly.  This detection is currently
 *  Linux-only, and relies on the OS to put the right value
 *  into /sys/block/&lt;dev&gt;/block/rotational.  For all
 *  other operating systems it currently assumes a rotational
 *  disk for backwards compatibility.  To enable default
 *  settings for spinning or solid state disks for such
 *  operating systems, use {@link #setDefaultMaxMergesAndThreads(boolean)}.
 */ 
public class ConcurrentMergeScheduler extends MergeScheduler {

  /** Dynamic default for {@code maxThreadCount} and {@code maxMergeCount},
   *  used to detect whether the index is backed by an SSD or rotational disk and
   *  set {@code maxThreadCount} accordingly.  If it's an SSD,
   *  {@code maxThreadCount} is set to {@code max(1, min(3, cpuCoreCount/2))},
   *  otherwise 1.  Note that detection only currently works on
   *  Linux; other platforms will assume the index is not on an SSD. */
  public static final int AUTO_DETECT_MERGES_AND_THREADS = -1;

  private int mergeThreadPriority = -1;

  /** List of currently active {@link MergeThread}s. */
  protected final List<MergeThread> mergeThreads = new ArrayList<>();
  
  // Max number of merge threads allowed to be running at
  // once.  When there are more merges then this, we
  // forcefully pause the larger ones, letting the smaller
  // ones run, up until maxMergeCount merges at which point
  // we forcefully pause incoming threads (that presumably
  // are the ones causing so much merging).
  private int maxThreadCount = AUTO_DETECT_MERGES_AND_THREADS;

  // Max number of merges we accept before forcefully
  // throttling the incoming threads
  private int maxMergeCount = AUTO_DETECT_MERGES_AND_THREADS;

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

  /**
   * Expert: directly set the maximum number of merge threads and
   * simultaneous merges allowed.
   * 
   * @param maxMergeCount the max # simultaneous merges that are allowed.
   *       If a merge is necessary yet we already have this many
   *       threads running, the incoming thread (that is calling
   *       add/updateDocument) will block until a merge thread
   *       has completed.  Note that we will only run the
   *       smallest <code>maxThreadCount</code> merges at a time.
   * @param maxThreadCount the max # simultaneous merge threads that should
   *       be running at once.  This must be &lt;= <code>maxMergeCount</code>
   */
  public synchronized void setMaxMergesAndThreads(int maxMergeCount, int maxThreadCount) {
    if (maxMergeCount == AUTO_DETECT_MERGES_AND_THREADS && maxThreadCount == AUTO_DETECT_MERGES_AND_THREADS) {
      // OK
      this.maxMergeCount = AUTO_DETECT_MERGES_AND_THREADS;
      this.maxThreadCount = AUTO_DETECT_MERGES_AND_THREADS;
    } else if (maxMergeCount == AUTO_DETECT_MERGES_AND_THREADS) {
      throw new IllegalArgumentException("both maxMergeCount and maxThreadCount must be AUTO_DETECT_MERGES_AND_THREADS");
    } else if (maxThreadCount == AUTO_DETECT_MERGES_AND_THREADS) {
      throw new IllegalArgumentException("both maxMergeCount and maxThreadCount must be AUTO_DETECT_MERGES_AND_THREADS");
    } else {
      if (maxThreadCount < 1) {
        throw new IllegalArgumentException("maxThreadCount should be at least 1");
      }
      if (maxMergeCount < 1) {
        throw new IllegalArgumentException("maxMergeCount should be at least 1");
      }
      if (maxThreadCount > maxMergeCount) {
        throw new IllegalArgumentException("maxThreadCount should be <= maxMergeCount (= " + maxMergeCount + ")");
      }
      this.maxThreadCount = maxThreadCount;
      this.maxMergeCount = maxMergeCount;
    }
  }

  /** Sets max merges and threads to proper defaults for rotational
   *  or non-rotational storage.
   *
   * @param spins true to set defaults best for traditional rotatational storage (spinning disks), 
   *        else false (e.g. for solid-state disks)
   */
  public synchronized void setDefaultMaxMergesAndThreads(boolean spins) {
    if (spins) {
      maxThreadCount = 1;
      maxMergeCount = 2;
    } else {
      maxThreadCount = Math.max(1, Math.min(3, Runtime.getRuntime().availableProcessors()/2));
      maxMergeCount = maxThreadCount+2;
    }
  }

  /** Returns {@code maxThreadCount}.
   *
   * @see #setMaxMergesAndThreads(int, int) */
  public synchronized int getMaxThreadCount() {
    return maxThreadCount;
  }

  /** See {@link #setMaxMergesAndThreads}. */
  public synchronized int getMaxMergeCount() {
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
   * Called whenever the running merges have changed, to pause and unpause
   * threads. This method sorts the merge threads by their merge size in
   * descending order and then pauses/unpauses threads from first to last --
   * that way, smaller merges are guaranteed to run before larger ones.
   */
  protected synchronized void updateMergeThreads() {

    // Only look at threads that are alive & not in the
    // process of stopping (ie have an active merge):
    final List<MergeThread> activeMerges = new ArrayList<>();

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
    CollectionUtil.timSort(activeMerges, compareByMergeDocCount);
    
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

  private synchronized void initMaxMergesAndThreads() throws IOException {
    if (maxThreadCount == AUTO_DETECT_MERGES_AND_THREADS) {
      assert writer != null;
      boolean spins = IOUtils.spins(writer.getDirectory());
      setDefaultMaxMergesAndThreads(spins);
      if (verbose()) {
        message("initMaxMergesAndThreads spins=" + spins + " maxThreadCount=" + maxThreadCount + " maxMergeCount=" + maxMergeCount);
      }
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
      if (mt.isAlive()) {
        MergePolicy.OneMerge merge = mt.getCurrentMerge();
        if (merge != null && merge.isAborted() == false) {
          count++;
        }
      }
    }
    return count;
  }

  @Override
  public synchronized void merge(IndexWriter writer, MergeTrigger trigger, boolean newMergesFound) throws IOException {

    assert !Thread.holdsLock(writer);

    this.writer = writer;

    initMergeThreadPriority();
    initMaxMergesAndThreads();

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

      maybeStall();

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

  /** This is invoked by {@link #merge} to possibly stall the incoming
   *  thread when there are too many merges running or pending.  The 
   *  default behavior is to force this thread, which is producing too
   *  many segments for merging to keep up, to wait until merges catch
   *  up. Applications that can take other less drastic measures, such
   *  as limiting how many threads are allowed to index, can do nothing
   *  here and throttle elsewhere. */

  protected synchronized void maybeStall() {
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
      if (verbose() && startStallTime == 0) {
        message("    too many merges; stalling...");
      }
      startStallTime = System.currentTimeMillis();
      try {
        // Only wait 0.25 seconds, so if all merges are aborted (by IW.rollback) we notice:
        wait(250);
      } catch (InterruptedException ie) {
        throw new ThreadInterruptedException(ie);
      }
    }

    if (verbose()) {
      if (startStallTime != 0) {
        message("  stalled for " + (System.currentTimeMillis()-startStallTime) + " msec");
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
}
