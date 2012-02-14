package org.apache.lucene.search;

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

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader; // javadocs
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher; // javadocs
import org.apache.lucene.search.SearcherFactory; // javadocs
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.ThreadInterruptedException;

/**
 * Utility class to manage sharing near-real-time searchers
 * across multiple searching thread.  The difference vs
 * SearcherManager is that this class enables individual
 * requests to wait until specific indexing changes are
 * visible.
 *
 * <p>You must create an IndexWriter, then create a {@link
 * NRTManager.TrackingIndexWriter} from it, and pass that to the
 * NRTManager.  You may want to create two NRTManagers, once
 * that always applies deletes on refresh and one that does
 * not.  In this case you should use a single {@link
 * NRTManager.TrackingIndexWriter} instance for both.
 *
 * <p>Then, use {@link #getSearcherManager} to obtain the
 * {@link SearcherManager} that you then use to
 * acquire/release searchers.  Don't call maybeReopen on
 * that SearcherManager!  Only call NRTManager's {@link
 * #maybeReopen}.
 *
 * <p>NOTE: to use this class, you must call {@link #maybeReopen()}
 * periodically.  The {@link NRTManagerReopenThread} is a
 * simple class to do this on a periodic basis, and reopens
 * more quickly if a request is waiting.  If you implement
 * your own reopener, be sure to call {@link
 * #addWaitingListener} so your reopener is notified when a
 * caller is waiting for a specific generation
 * searcher. </p>
 *
 * @see SearcherFactory
 * 
 * @lucene.experimental
 */

public class NRTManager implements Closeable {
  private static final long MAX_SEARCHER_GEN = Long.MAX_VALUE;
  private final TrackingIndexWriter writer;
  private final List<WaitingListener> waitingListeners = new CopyOnWriteArrayList<WaitingListener>();
  private final ReentrantLock reopenLock = new ReentrantLock();
  private final Condition newGeneration = reopenLock.newCondition();

  private final SearcherManager mgr;
  private volatile long searchingGen;

  /**
   * Create new NRTManager.
   * 
   * @param writer TrackingIndexWriter to open near-real-time
   *        readers
   * @param searcherFactory An optional {@link SearcherFactory}. Pass
   *        <code>null</code> if you don't require the searcher to be warmed
   *        before going live or other custom behavior.
   */
  public NRTManager(TrackingIndexWriter writer, SearcherFactory searcherFactory) throws IOException {
    this(writer, searcherFactory, true);
  }

  /**
   * Expert: just like {@link
   * #NRTManager(TrackingIndexWriter,SearcherFactory)},
   * but you can also specify whether each reopened searcher must
   * apply deletes.  This is useful for cases where certain
   * uses can tolerate seeing some deleted docs, since
   * reopen time is faster if deletes need not be applied. */
  public NRTManager(TrackingIndexWriter writer, SearcherFactory searcherFactory, boolean applyDeletes) throws IOException {
    this.writer = writer;
    mgr = new SearcherManager(writer.getIndexWriter(), applyDeletes, searcherFactory);
  }

  /**
   * Returns the {@link SearcherManager} you should use to
   * acquire/release searchers.
   *
   * <p><b>NOTE</b>: Never call maybeReopen on the returned
   * SearcherManager; only call this NRTManager's {@link
   * #maybeReopen}.  Otherwise threads waiting for a
   * generation may never return.
   */
  public SearcherManager getSearcherManager() {
    return mgr;
  }
  
  /** NRTManager invokes this interface to notify it when a
   *  caller is waiting for a specific generation searcher
   *  to be visible. */
  public static interface WaitingListener {
    public void waiting(long targetGen);
  }

  /** Adds a listener, to be notified when a caller is
   *  waiting for a specific generation searcher to be
   *  visible. */
  public void addWaitingListener(WaitingListener l) {
    waitingListeners.add(l);
  }

  /** Remove a listener added with {@link
   *  #addWaitingListener}. */
  public void removeWaitingListener(WaitingListener l) {
    waitingListeners.remove(l);
  }

  /** Class that tracks changes to a delegated
   * IndexWriter.  Create this class (passing your
   * IndexWriter), and then pass this class to NRTManager.
   * Be sure to make all changes via the
   * TrackingIndexWriter, otherwise NRTManager won't know
   * about the changes.
   *
   * @lucene.experimental */
  public static class TrackingIndexWriter {
    private final IndexWriter writer;
    private final AtomicLong indexingGen = new AtomicLong(1);

    public TrackingIndexWriter(IndexWriter writer) {
      this.writer = writer;
    }

    public long updateDocument(Term t, Iterable<? extends IndexableField> d, Analyzer a) throws IOException {
      writer.updateDocument(t, d, a);
      // Return gen as of when indexing finished:
      return indexingGen.get();
    }

    public long updateDocument(Term t, Iterable<? extends IndexableField> d) throws IOException {
      writer.updateDocument(t, d);
      // Return gen as of when indexing finished:
      return indexingGen.get();
    }

    public long updateDocuments(Term t, Iterable<? extends Iterable<? extends IndexableField>> docs, Analyzer a) throws IOException {
      writer.updateDocuments(t, docs, a);
      // Return gen as of when indexing finished:
      return indexingGen.get();
    }

    public long updateDocuments(Term t, Iterable<? extends Iterable<? extends IndexableField>> docs) throws IOException {
      writer.updateDocuments(t, docs);
      // Return gen as of when indexing finished:
      return indexingGen.get();
    }

    public long deleteDocuments(Term t) throws IOException {
      writer.deleteDocuments(t);
      // Return gen as of when indexing finished:
      return indexingGen.get();
    }

    public long deleteDocuments(Term... terms) throws IOException {
      writer.deleteDocuments(terms);
      // Return gen as of when indexing finished:
      return indexingGen.get();
    }

    public long deleteDocuments(Query q) throws IOException {
      writer.deleteDocuments(q);
      // Return gen as of when indexing finished:
      return indexingGen.get();
    }

    public long deleteDocuments(Query... queries) throws IOException {
      writer.deleteDocuments(queries);
      // Return gen as of when indexing finished:
      return indexingGen.get();
    }

    public long deleteAll() throws IOException {
      writer.deleteAll();
      // Return gen as of when indexing finished:
      return indexingGen.get();
    }

    public long addDocument(Iterable<? extends IndexableField> d, Analyzer a) throws IOException {
      writer.addDocument(d, a);
      // Return gen as of when indexing finished:
      return indexingGen.get();
    }

    public long addDocuments(Iterable<? extends Iterable<? extends IndexableField>> docs, Analyzer a) throws IOException {
      writer.addDocuments(docs, a);
      // Return gen as of when indexing finished:
      return indexingGen.get();
    }

    public long addDocument(Iterable<? extends IndexableField> d) throws IOException {
      writer.addDocument(d);
      // Return gen as of when indexing finished:
      return indexingGen.get();
    }

    public long addDocuments(Iterable<? extends Iterable<? extends IndexableField>> docs) throws IOException {
      writer.addDocuments(docs);
      // Return gen as of when indexing finished:
      return indexingGen.get();
    }

    public long addIndexes(Directory... dirs) throws CorruptIndexException, IOException {
      writer.addIndexes(dirs);
      // Return gen as of when indexing finished:
      return indexingGen.get();
    }

    public long addIndexes(IndexReader... readers) throws CorruptIndexException, IOException {
      writer.addIndexes(readers);
      // Return gen as of when indexing finished:
      return indexingGen.get();
    }

    public long getGeneration() {
      return indexingGen.get();
    }

    public IndexWriter getIndexWriter() {
      return writer;
    }

    long getAndIncrementGeneration() {
      return indexingGen.getAndIncrement();
    }
  }

  /**
   * Waits for the target generation to become visible in
   * the searcher.
   * If the current searcher is older than the
   * target generation, this method will block
   * until the searcher is reopened, by another via
   * {@link #maybeReopen} or until the {@link NRTManager} is closed.
   * 
   * @param targetGen the generation to wait for
   */
  public void waitForGeneration(long targetGen) {
    waitForGeneration(targetGen, -1, TimeUnit.NANOSECONDS);
  }

  /**
   * Waits for the target generation to become visible in
   * the searcher.  If the current searcher is older than
   * the target generation, this method will block until the
   * searcher has been reopened by another thread via
   * {@link #maybeReopen}, the given waiting time has elapsed, or until
   * the NRTManager is closed.
   * <p>
   * NOTE: if the waiting time elapses before the requested target generation is
   * available the current {@link SearcherManager} is returned instead.
   * 
   * @param targetGen
   *          the generation to wait for
   * @param time
   *          the time to wait for the target generation
   * @param unit
   *          the waiting time's time unit
   */
  public void waitForGeneration(long targetGen, long time, TimeUnit unit) {
    try {
      final long curGen = writer.getGeneration();
      if (targetGen > curGen) {
        throw new IllegalArgumentException("targetGen=" + targetGen + " was never returned by this NRTManager instance (current gen=" + curGen + ")");
      }
      reopenLock.lockInterruptibly();
      try {
        if (targetGen > searchingGen) {
          for (WaitingListener listener : waitingListeners) {
            listener.waiting(targetGen);
          }
          while (targetGen > searchingGen) {
            if (!waitOnGenCondition(time, unit)) {
              return;
            }
          }
        }
      } finally {
        reopenLock.unlock();
      }
    } catch (InterruptedException ie) {
      throw new ThreadInterruptedException(ie);
    }
  }
  
  private boolean waitOnGenCondition(long time, TimeUnit unit)
      throws InterruptedException {
    assert reopenLock.isHeldByCurrentThread();
    if (time < 0) {
      newGeneration.await();
      return true;
    } else {
      return newGeneration.await(time, unit);
    }
  }

  /** Returns generation of current searcher. */
  public long getCurrentSearchingGen() {
    return searchingGen;
  }

  public void maybeReopen() throws IOException {
    if (reopenLock.tryLock()) {
      try {
        // Mark gen as of when reopen started:
        final long newSearcherGen = writer.getAndIncrementGeneration();
        if (searchingGen == MAX_SEARCHER_GEN) {
          newGeneration.signalAll(); // wake up threads if we have a new generation
          return;
        }
        boolean setSearchGen;
        if (!mgr.isSearcherCurrent()) {
          setSearchGen = mgr.maybeRefresh();
        } else {
          setSearchGen = true;
        }
        if (setSearchGen) {
          searchingGen = newSearcherGen;// update searcher gen
          newGeneration.signalAll(); // wake up threads if we have a new generation
        }
      } finally {
        reopenLock.unlock();
      }
    }
  }

  /**
   * Close this NRTManager to future searching. Any searches still in process in
   * other threads won't be affected, and they should still call
   * {@link SearcherManager#release(IndexSearcher)} after they are done.
   * 
   * <p>
   * <b>NOTE</b>: caller must separately close the writer.
   */
  public void close() throws IOException {
    reopenLock.lock();
    try {
      try {
        // max it out to make sure nobody can wait on another gen
        searchingGen = MAX_SEARCHER_GEN; 
        mgr.close();
      } finally { // make sure we signal even if close throws an exception
        newGeneration.signalAll();
      }
    } finally {
      reopenLock.unlock();
    }
  }
}
