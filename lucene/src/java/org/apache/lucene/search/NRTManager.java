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
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader; // javadocs
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher; // javadocs
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.ThreadInterruptedException;

/**
 * Utility class to manage sharing near-real-time searchers
 * across multiple searching threads.
 *
 * <p>NOTE: to use this class, you must call {@link #maybeReopen(boolean)}
 * periodically.  The {@link NRTManagerReopenThread} is a
 * simple class to do this on a periodic basis.  If you
 * implement your own reopener, be sure to call {@link
 * #addWaitingListener} so your reopener is notified when a
 * caller is waiting for a specific generation searcher. </p>
 *
 * @lucene.experimental
 */

public class NRTManager implements Closeable {
  private static final long MAX_SEARCHER_GEN = Long.MAX_VALUE;
  private final IndexWriter writer;
  private final SearcherManagerRef withoutDeletes;
  private final SearcherManagerRef withDeletes;
  private final AtomicLong indexingGen;
  private final List<WaitingListener> waitingListeners = new CopyOnWriteArrayList<WaitingListener>();
  private final ReentrantLock reopenLock = new ReentrantLock();
  private final Condition newGeneration = reopenLock.newCondition();

  /**
   * Create new NRTManager.
   * 
   *  @param writer IndexWriter to open near-real-time
   *         readers
   *  @param warmer optional {@link SearcherWarmer}.  Pass
   *         null if you don't require the searcher to warmed
   *         before going live.  If this is non-null then a
   *         merged segment warmer is installed on the
   *         provided IndexWriter's config.
   *
   *  <p><b>NOTE</b>: the provided {@link SearcherWarmer} is
   *  not invoked for the initial searcher; you should
   *  warm it yourself if necessary.
   */
  public NRTManager(IndexWriter writer, SearcherWarmer warmer) throws IOException {
    this(writer, null, warmer, true);
  }

  /**
   * Create new NRTManager.
   * 
   *  @param writer IndexWriter to open near-real-time
   *         readers
   *  @param es optional ExecutorService so different segments can
   *         be searched concurrently (see {@link IndexSearcher#IndexSearcher(IndexReader, ExecutorService)}.
   *         Pass <code>null</code> to search segments sequentially.
   *  @param warmer optional {@link SearcherWarmer}.  Pass
   *         null if you don't require the searcher to warmed
   *         before going live.  If this is non-null then a
   *         merged segment warmer is installed on the
   *         provided IndexWriter's config.
   *
   *  <p><b>NOTE</b>: the provided {@link SearcherWarmer} is
   *  not invoked for the initial searcher; you should
   *  warm it yourself if necessary.
   */
  public NRTManager(IndexWriter writer, ExecutorService es,
      SearcherWarmer warmer) throws IOException {
    this(writer, es, warmer, true);
  }

  /**
   * Expert: just like {@link
   * #NRTManager(IndexWriter,ExecutorService,SearcherWarmer)},
   * but you can also specify whether every searcher must
   * apply deletes.  This is useful for cases where certain
   * uses can tolerate seeing some deleted docs, since
   * reopen time is faster if deletes need not be applied. */
  public NRTManager(IndexWriter writer, ExecutorService es,
      SearcherWarmer warmer, boolean alwaysApplyDeletes) throws IOException {
    this.writer = writer;
    if (alwaysApplyDeletes) {
      withoutDeletes = withDeletes = new SearcherManagerRef(true, 0,  new SearcherManager(writer, true, warmer, es));
    } else {
      withDeletes = new SearcherManagerRef(true, 0, new SearcherManager(writer, true, warmer, es));
      withoutDeletes = new SearcherManagerRef(false, 0, new SearcherManager(writer, false, warmer, es));
    }
    indexingGen = new AtomicLong(1);
  }
  
  /** NRTManager invokes this interface to notify it when a
   *  caller is waiting for a specific generation searcher
   *  to be visible. */
  public static interface WaitingListener {
    public void waiting(boolean requiresDeletes, long targetGen);
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

  public long updateDocument(Term t, Document d, Analyzer a) throws IOException {
    writer.updateDocument(t, d, a);
    // Return gen as of when indexing finished:
    return indexingGen.get();
  }

  public long updateDocument(Term t, Document d) throws IOException {
    writer.updateDocument(t, d);
    // Return gen as of when indexing finished:
    return indexingGen.get();
  }

  public long updateDocuments(Term t, Collection<Document> docs, Analyzer a) throws IOException {
    writer.updateDocuments(t, docs, a);
    // Return gen as of when indexing finished:
    return indexingGen.get();
  }

  public long updateDocuments(Term t, Collection<Document> docs) throws IOException {
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

  public long addDocument(Document d, Analyzer a) throws IOException {
    writer.addDocument(d, a);
    // Return gen as of when indexing finished:
    return indexingGen.get();
  }

  public long addDocuments(Collection<Document> docs, Analyzer a) throws IOException {
    writer.addDocuments(docs, a);
    // Return gen as of when indexing finished:
    return indexingGen.get();
  }

  public long addDocument(Document d) throws IOException {
    writer.addDocument(d);
    // Return gen as of when indexing finished:
    return indexingGen.get();
  }

  public long addDocuments(Collection<Document> docs) throws IOException {
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

  /**
   * Waits for a given {@link SearcherManager} target generation to be available
   * via {@link #getSearcherManager(boolean)}. If the current generation is less
   * than the given target generation this method will block until the
   * correspondent {@link SearcherManager} is reopened by another thread via
   * {@link #maybeReopen(boolean)} or until the {@link NRTManager} is closed.
   * 
   * @param targetGen the generation to wait for
   * @param requireDeletes <code>true</code> iff the generation requires deletes to be applied otherwise <code>false</code>
   * @return the {@link SearcherManager} with the given target generation
   */
  public SearcherManager waitForGeneration(long targetGen, boolean requireDeletes) {
    return waitForGeneration(targetGen, requireDeletes, -1,  TimeUnit.NANOSECONDS);
  }

  /**
   * Waits for a given {@link SearcherManager} target generation to be available
   * via {@link #getSearcherManager(boolean)}. If the current generation is less
   * than the given target generation this method will block until the
   * correspondent {@link SearcherManager} is reopened by another thread via
   * {@link #maybeReopen(boolean)}, the given waiting time has elapsed, or until
   * the {@link NRTManager} is closed.
   * <p>
   * NOTE: if the waiting time elapses before the requested target generation is
   * available the latest {@link SearcherManager} is returned instead.
   * 
   * @param targetGen
   *          the generation to wait for
   * @param requireDeletes
   *          <code>true</code> iff the generation requires deletes to be
   *          applied otherwise <code>false</code>
   * @param time
   *          the time to wait for the target generation
   * @param unit
   *          the waiting time's time unit
   * @return the {@link SearcherManager} with the given target generation or the
   *         latest {@link SearcherManager} if the waiting time elapsed before
   *         the requested generation is available.
   */
  public SearcherManager waitForGeneration(long targetGen, boolean requireDeletes, long time, TimeUnit unit) {
    try {
      final long curGen = indexingGen.get();
      if (targetGen > curGen) {
        throw new IllegalArgumentException("targetGen=" + targetGen + " was never returned by this NRTManager instance (current gen=" + curGen + ")");
      }
      reopenLock.lockInterruptibly();
      try {
        if (targetGen > getCurrentSearchingGen(requireDeletes)) {
          for (WaitingListener listener : waitingListeners) {
            listener.waiting(requireDeletes, targetGen);
          }
          while (targetGen > getCurrentSearchingGen(requireDeletes)) {
            if (!waitOnGenCondition(time, unit)) {
              return getSearcherManager(requireDeletes);
            }
          }
        }

      } finally {
        reopenLock.unlock();
      }
    } catch (InterruptedException ie) {
      throw new ThreadInterruptedException(ie);
    }
    return getSearcherManager(requireDeletes);
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
  public long getCurrentSearchingGen(boolean applyAllDeletes) {
    if (applyAllDeletes) {
      return withDeletes.generation;
    } else {
      return Math.max(withoutDeletes.generation, withDeletes.generation);
    }
  }

  public boolean maybeReopen(boolean applyAllDeletes) throws IOException {
    if (reopenLock.tryLock()) {
      try {
        final SearcherManagerRef reference = applyAllDeletes ? withDeletes : withoutDeletes;
        // Mark gen as of when reopen started:
        final long newSearcherGen = indexingGen.getAndIncrement();
        boolean setSearchGen = false;
        if (reference.generation == MAX_SEARCHER_GEN) {
          newGeneration.signalAll(); // wake up threads if we have a new generation
          return false;
        }
        if (!(setSearchGen = reference.manager.isSearcherCurrent())) {
          setSearchGen = reference.manager.maybeReopen();
        }
        if (setSearchGen) {
          reference.generation = newSearcherGen;// update searcher gen
          newGeneration.signalAll(); // wake up threads if we have a new generation
        }
        return setSearchGen;
      } finally {
        reopenLock.unlock();
      }
    }
    return false;
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
        IOUtils.close(withDeletes, withoutDeletes);
      } finally { // make sure we signal even if close throws an exception
        newGeneration.signalAll();
      }
    } finally {
      reopenLock.unlock();
      assert withDeletes.generation == MAX_SEARCHER_GEN && withoutDeletes.generation == MAX_SEARCHER_GEN;
    }
  }

  /**
   * Returns a {@link SearcherManager}. If <code>applyAllDeletes</code> is
   * <code>true</code> the returned manager is guaranteed to have all deletes
   * applied on the last reopen. Otherwise the latest manager with or without deletes
   * is returned.
   */
  public SearcherManager getSearcherManager(boolean applyAllDeletes) {
    if (applyAllDeletes) {
      return withDeletes.manager;
    } else {
      if (withDeletes.generation > withoutDeletes.generation) {
        return withDeletes.manager;
      } else {
        return withoutDeletes.manager;
      }
    }
  }
  
  static final class SearcherManagerRef implements Closeable {
    final boolean applyDeletes;
    volatile long generation;
    final SearcherManager manager;

    SearcherManagerRef(boolean applyDeletes, long generation, SearcherManager manager) {
      super();
      this.applyDeletes = applyDeletes;
      this.generation = generation;
      this.manager = manager;
    }
    
    public void close() throws IOException {
      generation = MAX_SEARCHER_GEN; // max it out to make sure nobody can wait on another gen
      manager.close();
    }
  }
}
