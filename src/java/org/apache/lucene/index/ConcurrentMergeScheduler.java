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
 *  ({@link #setMaxThreadCount}) at which points merges are
 *  run in the foreground, serially.  This is a simple way
 *  to use concurrency in the indexing process without
 *  having to create and manage application level
 *  threads. */

public class ConcurrentMergeScheduler implements MergeScheduler {

  public static boolean VERBOSE = false;

  private int mergeThreadPriority = -1;

  private List mergeThreads = new ArrayList();
  private int maxThreadCount = 3;

  private List exceptions = new ArrayList();
  private Directory dir;

  /** Sets the max # simultaneous threads that may be
   *  running.  If a merge is necessary yet we already have
   *  this many threads running, the merge is returned back
   *  to IndexWriter so that it runs in the "foreground". */
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
    mergeThreadPriority = pri;

    final int numThreads = mergeThreads.size();
    for(int i=0;i<numThreads;i++) {
      MergeThread merge = (MergeThread) mergeThreads.get(i);
      try {
        merge.setPriority(pri);
      } catch (NullPointerException npe) {
        // Strangely, Sun's JDK 1.5 on Linux sometimes
        // throws NPE out of here...
      }
    }
  }

  /** Returns any exceptions that were caught in the merge
   *  threads. */
  public List getExceptions() {
    return exceptions;
  }

  private void message(String message) {
    System.out.println("CMS [" + Thread.currentThread().getName() + "]: " + message);
  }

  private synchronized void initMergeThreadPriority() {
    if (mergeThreadPriority == -1)
      // Default to slightly higher priority than our
      // calling thread
      mergeThreadPriority = 1+Thread.currentThread().getPriority();
  }

  public void close() {}

  private synchronized void finishThreads() {
    while(mergeThreads.size() > 0) {
      if (VERBOSE) {
        message("now wait for threads; currently " + mergeThreads.size() + " still running");
        for(int i=0;i<mergeThreads.size();i++) {
          final MergeThread mergeThread = ((MergeThread) mergeThreads.get(i));
          message("    " + i + ": " + mergeThread.merge.segString(dir));
        }
      }

      try {
        wait();
      } catch (InterruptedException e) {
      }
    }
  }

  public void sync() {
    finishThreads();
  }

  // Used for testing
  private boolean suppressExceptions;

  /** Used for testing */
  void setSuppressExceptions() {
    suppressExceptions = true;
  }
  void clearSuppressExceptions() {
    suppressExceptions = false;
  }

  public void merge(IndexWriter writer)
    throws CorruptIndexException, IOException {

    initMergeThreadPriority();

    dir = writer.getDirectory();

    // First, quickly run through the newly proposed merges
    // and add any orthogonal merges (ie a merge not
    // involving segments already pending to be merged) to
    // the queue.  If we are way behind on merging, many of
    // these newly proposed merges will likely already be
    // registered.

    if (VERBOSE) {
      message("now merge");
      message("  index: " + writer.segString());
    }

    // Iterate, pulling from the IndexWriter's queue of
    // pending merges, until its empty:
    while(true) {

      // TODO: we could be careful about which merges to do in
      // the BG (eg maybe the "biggest" ones) vs FG, which
      // merges to do first (the easiest ones?), etc.

      MergePolicy.OneMerge merge = writer.getNextMerge();
      if (merge == null) {
        if (VERBOSE)
          message("  no more merges pending; now return");
        return;
      }

      // We do this w/ the primary thread to keep
      // deterministic assignment of segment names
      writer.mergeInit(merge);

      if (VERBOSE)
        message("  consider merge " + merge.segString(dir));
      
      if (merge.isExternal) {
        if (VERBOSE)
          message("    merge involves segments from an external directory; now run in foreground");
      } else {
        synchronized(this) {
          if (mergeThreads.size() < maxThreadCount) {
            // OK to spawn a new merge thread to handle this
            // merge:
            MergeThread merger = new MergeThread(writer, merge);
            mergeThreads.add(merger);
            if (VERBOSE)
              message("    launch new thread [" + merger.getName() + "]");
            try {
              merger.setPriority(mergeThreadPriority);
            } catch (NullPointerException npe) {
              // Strangely, Sun's JDK 1.5 on Linux sometimes
              // throws NPE out of here...
            }
            merger.start();
            continue;
          } else if (VERBOSE)
            message("    too many merge threads running; run merge in foreground");
        }
      }

      // Too many merge threads already running, so we do
      // this in the foreground of the calling thread
      writer.merge(merge);
    }
  }

  private class MergeThread extends Thread {

    IndexWriter writer;
    MergePolicy.OneMerge merge;

    public MergeThread(IndexWriter writer, MergePolicy.OneMerge merge) throws IOException {
      this.writer = writer;
      this.merge = merge;
    }

    public void run() {
      try {

        if (VERBOSE)
          message("  merge thread: start");

        // First time through the while loop we do the merge
        // that we were started with:
        MergePolicy.OneMerge merge = this.merge;

        while(true) {
          writer.merge(merge);

          // Subsequent times through the loop we do any new
          // merge that writer says is necessary:
          merge = writer.getNextMerge();
          if (merge != null) {
            writer.mergeInit(merge);
            if (VERBOSE)
              message("  merge thread: do another merge " + merge.segString(dir));
          } else
            break;
        }

        if (VERBOSE)
          message("  merge thread: done");

      } catch (Throwable exc) {
        // When a merge was aborted & IndexWriter closed,
        // it's possible to get various IOExceptions,
        // NullPointerExceptions, AlreadyClosedExceptions:
        merge.setException(exc);
        writer.addMergeException(merge);

        if (!merge.isAborted()) {
          // If the merge was not aborted then the exception
          // is real
          exceptions.add(exc);
          
          if (!suppressExceptions)
            // suppressExceptions is normally only set during
            // testing.
            throw new MergePolicy.MergeException(exc);
        }
      } finally {
        synchronized(ConcurrentMergeScheduler.this) {
          mergeThreads.remove(this);
          ConcurrentMergeScheduler.this.notifyAll();
        }
      }
    }

    public String toString() {
      return "merge thread: " + merge.segString(dir);
    }
  }
}
