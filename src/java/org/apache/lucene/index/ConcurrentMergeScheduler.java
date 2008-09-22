package org.apache.lucene.index;

/**
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

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

/** A {@link MergeScheduler} that runs each merge using a
 *  separate thread, up until a maximum number of threads
 *  ({@link #setMaxThreadCount}) at which when a merge is
 *  needed, the thread(s) that are updating the index will
 *  pause until one or more merges completes.  This is a
 *  simple way to use concurrency in the indexing process
 *  without having to create and manage application level
 *  threads. */

public class ConcurrentMergeScheduler extends MergeScheduler {

  private int mergeThreadPriority = -1;

  protected List mergeThreads = new ArrayList();

  // Max number of threads allowed to be merging at once
  private int maxThreadCount = 3;

  private List exceptions = new ArrayList();
  protected Directory dir;

  private boolean closed;
  protected IndexWriter writer;
  protected int mergeThreadCount;

  public ConcurrentMergeScheduler() {
    if (allInstances != null) {
      // Only for testing
      addMyself();
    }
  }

  /** Sets the max # simultaneous threads that may be
   *  running.  If a merge is necessary yet we already have
   *  this many threads running, the incoming thread (that
   *  is calling add/updateDocument) will block until
   *  a merge thread has completed. */
  public void setMaxThreadCount(int count) {
    if (count < 1)
      throw new IllegalArgumentException("count should be at least 1");
    maxThreadCount = count;
  }

  /** Get the max # simultaneous threads that may be
   *  running. @see #setMaxThreadCount. */
  public int getMaxThreadCount() {
    return maxThreadCount;
  }

  /** Return the priority that merge threads run at.  By
   *  default the priority is 1 plus the priority of (ie,
   *  slightly higher priority than) the first thread that
   *  calls merge. */
  public synchronized int getMergeThreadPriority() {
    initMergeThreadPriority();
    return mergeThreadPriority;
  }

  /** Return the priority that merge threads run at. */
  public synchronized void setMergeThreadPriority(int pri) {
    if (pri > Thread.MAX_PRIORITY || pri < Thread.MIN_PRIORITY)
      throw new IllegalArgumentException("priority must be in range " + Thread.MIN_PRIORITY + " .. " + Thread.MAX_PRIORITY + " inclusive");
    mergeThreadPriority = pri;

    final int numThreads = mergeThreadCount();
    for(int i=0;i<numThreads;i++) {
      MergeThread merge = (MergeThread) mergeThreads.get(i);
      merge.setThreadPriority(pri);
    }
  }

  private void message(String message) {
    if (writer != null)
      writer.message("CMS: " + message);
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

  public void close() {
    closed = true;
  }

  public synchronized void sync() {
    while(mergeThreadCount() > 0) {
      message("now wait for threads; currently " + mergeThreads.size() + " still running");
      final int count = mergeThreads.size();
      for(int i=0;i<count;i++)
        message("    " + i + ": " + ((MergeThread) mergeThreads.get(i)));

      try {
        wait();
      } catch (InterruptedException e) {
      }
    }
  }
  private synchronized int mergeThreadCount() {
    int count = 0;
    final int numThreads = mergeThreads.size();
    for(int i=0;i<numThreads;i++)
      if (((MergeThread) mergeThreads.get(i)).isAlive())
        count++;
    return count;
  }

  public void merge(IndexWriter writer)
    throws CorruptIndexException, IOException {

    // TODO: enable this once we are on JRE 1.5
    // assert !Thread.holdsLock(writer);

    this.writer = writer;

    initMergeThreadPriority();

    dir = writer.getDirectory();

    // First, quickly run through the newly proposed merges
    // and add any orthogonal merges (ie a merge not
    // involving segments already pending to be merged) to
    // the queue.  If we are way behind on merging, many of
    // these newly proposed merges will likely already be
    // registered.

    message("now merge");
    message("  index: " + writer.segString());

    // Iterate, pulling from the IndexWriter's queue of
    // pending merges, until it's empty:
    while(true) {

      // TODO: we could be careful about which merges to do in
      // the BG (eg maybe the "biggest" ones) vs FG, which
      // merges to do first (the easiest ones?), etc.

      MergePolicy.OneMerge merge = writer.getNextMerge();
      if (merge == null) {
        message("  no more merges pending; now return");
        return;
      }

      // We do this w/ the primary thread to keep
      // deterministic assignment of segment names
      writer.mergeInit(merge);

      synchronized(this) {
        while (mergeThreadCount() >= maxThreadCount) {
          message("    too many merge threads running; stalling...");
          try {
            wait();
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
          }
        }

        message("  consider merge " + merge.segString(dir));
      
        assert mergeThreadCount() < maxThreadCount;

        // OK to spawn a new merge thread to handle this
        // merge:
        final MergeThread merger = getMergeThread(writer, merge);
        mergeThreads.add(merger);
        message("    launch new thread [" + merger.getName() + "]");
        merger.start();
      }
    }
  }

  /** Does the actual merge, by calling {@link IndexWriter#merge} */
  protected void doMerge(MergePolicy.OneMerge merge)
    throws IOException {
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

  protected class MergeThread extends Thread {

    IndexWriter writer;
    MergePolicy.OneMerge startMerge;
    MergePolicy.OneMerge runningMerge;

    public MergeThread(IndexWriter writer, MergePolicy.OneMerge startMerge) throws IOException {
      this.writer = writer;
      this.startMerge = startMerge;
    }

    public synchronized void setRunningMerge(MergePolicy.OneMerge merge) {
      runningMerge = merge;
    }

    public synchronized MergePolicy.OneMerge getRunningMerge() {
      return runningMerge;
    }

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

    public void run() {
      
      // First time through the while loop we do the merge
      // that we were started with:
      MergePolicy.OneMerge merge = this.startMerge;
      
      try {

        message("  merge thread: start");

        while(true) {
          setRunningMerge(merge);
          doMerge(merge);

          // Subsequent times through the loop we do any new
          // merge that writer says is necessary:
          merge = writer.getNextMerge();
          if (merge != null) {
            writer.mergeInit(merge);
            message("  merge thread: do another merge " + merge.segString(dir));
          } else
            break;
        }

        message("  merge thread: done");

      } catch (Throwable exc) {

        // Ignore the exception if it was due to abort:
        if (!(exc instanceof MergePolicy.MergeAbortedException)) {
          synchronized(ConcurrentMergeScheduler.this) {
            exceptions.add(exc);
          }
          
          if (!suppressExceptions) {
            // suppressExceptions is normally only set during
            // testing.
            anyExceptions = true;
            handleMergeException(exc);
          }
        }
      } finally {
        synchronized(ConcurrentMergeScheduler.this) {
          ConcurrentMergeScheduler.this.notifyAll();
          boolean removed = mergeThreads.remove(this);
          assert removed;
        }
      }
    }

    public String toString() {
      MergePolicy.OneMerge merge = getRunningMerge();
      if (merge == null)
        merge = startMerge;
      return "merge thread: " + merge.segString(dir);
    }
  }

  /** Called when an exception is hit in a background merge
   *  thread */
  protected void handleMergeException(Throwable exc) {
    throw new MergePolicy.MergeException(exc, dir);
  }

  static boolean anyExceptions = false;

  /** Used for testing */
  public static boolean anyUnhandledExceptions() {
    synchronized(allInstances) {
      final int count = allInstances.size();
      // Make sure all outstanding threads are done so we see
      // any exceptions they may produce:
      for(int i=0;i<count;i++)
        ((ConcurrentMergeScheduler) allInstances.get(i)).sync();
      boolean v = anyExceptions;
      anyExceptions = false;
      return v;
    }
  }

  public static void clearUnhandledExceptions() {
    synchronized(allInstances) {
      anyExceptions = false;
    }
  }

  /** Used for testing */
  private void addMyself() {
    synchronized(allInstances) {
      final int size=0;
      int upto = 0;
      for(int i=0;i<size;i++) {
        final ConcurrentMergeScheduler other = (ConcurrentMergeScheduler) allInstances.get(i);
        if (!(other.closed && 0 == other.mergeThreadCount()))
          // Keep this one for now: it still has threads or
          // may spawn new threads
          allInstances.set(upto++, other);
      }
      allInstances.subList(upto, allInstances.size()).clear();
      allInstances.add(this);
    }
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

  /** Used for testing */
  private static List allInstances;
  public static void setTestMode() {
    allInstances = new ArrayList();
  }
}
