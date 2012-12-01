package org.apache.lucene.search;

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
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.DirectoryReader;
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
 * <p>Then, use {@link #acquire} to obtain the
 * {@link IndexSearcher}, and {@link #release} (ideally,
 * from within a <code>finally</code> clause) to release it.
 *
 * <p>NOTE: to use this class, you must call {@link #maybeRefresh()}
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

public final class NRTManager extends ReferenceManager<IndexSearcher> {
  private static final long MAX_SEARCHER_GEN = Long.MAX_VALUE;
  private final TrackingIndexWriter writer;
  private final List<WaitingListener> waitingListeners = new CopyOnWriteArrayList<WaitingListener>();
  private final ReentrantLock genLock = new ReentrantLock();;
  private final Condition newGeneration = genLock.newCondition();
  private final SearcherFactory searcherFactory;

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
  public NRTManager(TrackingIndexWriter writer, SearcherFactory searcherFactory, boolean applyAllDeletes) throws IOException {
    this.writer = writer;
    if (searcherFactory == null) {
      searcherFactory = new SearcherFactory();
    }
    this.searcherFactory = searcherFactory;
    current = SearcherManager.getSearcher(searcherFactory, DirectoryReader.open(writer.getIndexWriter(), applyAllDeletes));
  }

  @Override
  protected void decRef(IndexSearcher reference) throws IOException {
    reference.getIndexReader().decRef();
  }
  
  @Override
  protected boolean tryIncRef(IndexSearcher reference) {
    return reference.getIndexReader().tryIncRef();
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

    public long addIndexes(Directory... dirs) throws IOException {
      writer.addIndexes(dirs);
      // Return gen as of when indexing finished:
      return indexingGen.get();
    }

    public long addIndexes(IndexReader... readers) throws IOException {
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

    public long tryDeleteDocument(IndexReader reader, int docID) throws IOException {
      if (writer.tryDeleteDocument(reader, docID)) {
        return indexingGen.get();
      } else {
        return -1;
      }
    }
  }

  /**
   * Waits for the target generation to become visible in
   * the searcher.
   * If the current searcher is older than the
   * target generation, this method will block
   * until the searcher is reopened, by another via
   * {@link #maybeRefresh} or until the {@link NRTManager} is closed.
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
   * {@link #maybeRefresh}, the given waiting time has elapsed, or until
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
      genLock.lockInterruptibly();
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
        genLock.unlock();
      }
    } catch (InterruptedException ie) {
      throw new ThreadInterruptedException(ie);
    }
  }
  
  private boolean waitOnGenCondition(long time, TimeUnit unit)
      throws InterruptedException {
    assert genLock.isHeldByCurrentThread();
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

  private long lastRefreshGen;

  @Override
  protected IndexSearcher refreshIfNeeded(IndexSearcher referenceToRefresh) throws IOException {
    // Record gen as of when reopen started:
    lastRefreshGen = writer.getAndIncrementGeneration();
    final IndexReader r = referenceToRefresh.getIndexReader();
    assert r instanceof DirectoryReader: "searcher's IndexReader should be a DirectoryReader, but got " + r;
    final DirectoryReader dirReader = (DirectoryReader) r;
    IndexSearcher newSearcher = null;
    if (!dirReader.isCurrent()) {
      final IndexReader newReader = DirectoryReader.openIfChanged(dirReader);
      if (newReader != null) {
        newSearcher = SearcherManager.getSearcher(searcherFactory, newReader);
      }
    }

    return newSearcher;
  }

  @Override
  protected void afterMaybeRefresh() {
    genLock.lock();
    try {
      if (searchingGen != MAX_SEARCHER_GEN) {
        // update searchingGen:
        assert lastRefreshGen >= searchingGen;
        searchingGen = lastRefreshGen;
      }
      // wake up threads if we have a new generation:
      newGeneration.signalAll();
    } finally {
      genLock.unlock();
    }
  }

  @Override
  protected synchronized void afterClose() throws IOException {
    genLock.lock();
    try {
      // max it out to make sure nobody can wait on another gen
      searchingGen = MAX_SEARCHER_GEN; 
      newGeneration.signalAll();
    } finally {
      genLock.unlock();
    }
  }

  /**
   * Returns <code>true</code> if no changes have occured since this searcher
   * ie. reader was opened, otherwise <code>false</code>.
   * @see DirectoryReader#isCurrent() 
   */
  public boolean isSearcherCurrent() throws IOException {
    final IndexSearcher searcher = acquire();
    try {
      final IndexReader r = searcher.getIndexReader();
      assert r instanceof DirectoryReader: "searcher's IndexReader should be a DirectoryReader, but got " + r;
      return ((DirectoryReader) r).isCurrent();
    } finally {
      release(searcher);
    }
  }
}
