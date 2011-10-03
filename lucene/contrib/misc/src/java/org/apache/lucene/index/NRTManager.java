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

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexReader;       // javadocs
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SearcherWarmer;
import org.apache.lucene.util.ThreadInterruptedException;

// TODO
//   - we could make this work also w/ "normal" reopen/commit?

/**
 * Utility class to manage sharing near-real-time searchers
 * across multiple searching threads.
 *
 * <p>NOTE: to use this class, you must call reopen
 * periodically.  The {@link NRTManagerReopenThread} is a
 * simple class to do this on a periodic basis.  If you
 * implement your own reopener, be sure to call {@link
 * #addWaitingListener} so your reopener is notified when a
 * caller is waiting for a specific generation searcher. </p>
 *
 * @lucene.experimental
 */

public class NRTManager implements Closeable {
  private final IndexWriter writer;
  private final ExecutorService es;
  private final AtomicLong indexingGen;
  private final AtomicLong searchingGen;
  private final AtomicLong noDeletesSearchingGen;
  private final SearcherWarmer warmer;
  private final List<WaitingListener> waitingListeners = new CopyOnWriteArrayList<WaitingListener>();

  private volatile IndexSearcher currentSearcher;
  private volatile IndexSearcher noDeletesCurrentSearcher;

  /**
   * Create new NRTManager.
   * 
   *  @param writer IndexWriter to open near-real-time
   *         readers
   *  @param es optional ExecutorService so different segments can
   *         be searched concurrently (see {@link
   *         IndexSearcher#IndexSearcher(IndexReader,ExecutorService)}.  Pass null
   *         to search segments sequentially.
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
  public NRTManager(IndexWriter writer, ExecutorService es, SearcherWarmer warmer) throws IOException {

    this.writer = writer;
    this.es = es;
    this.warmer = warmer;
    indexingGen = new AtomicLong(1);
    searchingGen = new AtomicLong(-1);
    noDeletesSearchingGen = new AtomicLong(-1);

    // Create initial reader:
    swapSearcher(new IndexSearcher(IndexReader.open(writer, true), es), 0, true);

    if (this.warmer != null) {
      writer.getConfig().setMergedSegmentWarmer(
         new IndexWriter.IndexReaderWarmer() {
           @Override
           public void warm(IndexReader reader) throws IOException {
             NRTManager.this.warmer.warm(new IndexSearcher(reader, NRTManager.this.es));
           }
         });
    }
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

  public long deleteDocuments(Query q) throws IOException {
    writer.deleteDocuments(q);
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

  /** Returns the most current searcher.  If you require a
   *  certain indexing generation be visible in the returned
   *  searcher, call {@link #get(long)}
   *  instead.
   */
  public synchronized IndexSearcher get() {
    return get(true);
  }

  /** Just like {@link #get}, but by passing <code>false</code> for
   *  requireDeletes, you can get faster reopen time, but
   *  the returned reader is allowed to not reflect all
   *  deletions.  See {@link IndexReader#open(IndexWriter,boolean)}  */
  public synchronized IndexSearcher get(boolean requireDeletes) {
    final IndexSearcher s;
    if (requireDeletes) {
      s = currentSearcher;
    } else if (noDeletesSearchingGen.get() > searchingGen.get()) {
      s = noDeletesCurrentSearcher;
    } else {
      s = currentSearcher;
    }
    s.getIndexReader().incRef();
    return s;
  }

  /** Call this if you require a searcher reflecting all
   *  changes as of the target generation.
   *
   * @param targetGen Returned searcher must reflect changes
   * as of this generation
   */
  public synchronized IndexSearcher get(long targetGen) {
    return get(targetGen, true);
  }

  /** Call this if you require a searcher reflecting all
   *  changes as of the target generation, and you don't
   *  require deletions to be reflected.  Note that the
   *  returned searcher may still reflect some or all
   *  deletions.
   *
   * @param targetGen Returned searcher must reflect changes
   * as of this generation
   *
   * @param requireDeletes If true, the returned searcher must
   * reflect all deletions.  This can be substantially more
   * costly than not applying deletes.  Note that if you
   * pass false, it's still possible that some or all
   * deletes may have been applied.
   **/
  public synchronized IndexSearcher get(long targetGen, boolean requireDeletes) {

    assert noDeletesSearchingGen.get() >= searchingGen.get(): "noDeletesSearchingGen=" + noDeletesSearchingGen.get() + " searchingGen=" + searchingGen.get();

    if (targetGen > getCurrentSearchingGen(requireDeletes)) {
      // Must wait
      //final long t0 = System.nanoTime();
      for(WaitingListener listener : waitingListeners) {
        listener.waiting(requireDeletes, targetGen);
      }
      while (targetGen > getCurrentSearchingGen(requireDeletes)) {
        //System.out.println(Thread.currentThread().getName() + ": wait fresh searcher targetGen=" + targetGen + " vs searchingGen=" + getCurrentSearchingGen(requireDeletes) + " requireDeletes=" + requireDeletes);
        try {
          wait();
        } catch (InterruptedException ie) {
          throw new ThreadInterruptedException(ie);
        }
      }
      //final long waitNS = System.nanoTime()-t0;
      //System.out.println(Thread.currentThread().getName() + ": done wait fresh searcher targetGen=" + targetGen + " vs searchingGen=" + getCurrentSearchingGen(requireDeletes) + " requireDeletes=" + requireDeletes + " WAIT msec=" + (waitNS/1000000.0));
    }

    return get(requireDeletes);
  }

  /** Returns generation of current searcher. */
  public long getCurrentSearchingGen(boolean requiresDeletes) {
    return requiresDeletes ? searchingGen.get() : noDeletesSearchingGen.get();
  }

  /** Release the searcher obtained from {@link
   *  #get()} or {@link #get(long)}.
   *
   *  <p><b>NOTE</b>: it's safe to call this after {@link
   *  #close}. */
  public void release(IndexSearcher s) throws IOException {
    s.getIndexReader().decRef();
  }

  /** Call this when you need the NRT reader to reopen.
   *
   * @param applyDeletes If true, the newly opened reader
   *        will reflect all deletes
   */
  public boolean reopen(boolean applyDeletes) throws IOException {

    // Mark gen as of when reopen started:
    final long newSearcherGen = indexingGen.getAndIncrement();

    if (applyDeletes && currentSearcher.getIndexReader().isCurrent()) {
      //System.out.println("reopen: skip: isCurrent both force gen=" + newSearcherGen + " vs current gen=" + searchingGen);
      searchingGen.set(newSearcherGen);
      noDeletesSearchingGen.set(newSearcherGen);
      synchronized(this) {
        notifyAll();
      }
      //System.out.println("reopen: skip: return");
      return false;
    } else if (!applyDeletes && noDeletesCurrentSearcher.getIndexReader().isCurrent()) {
      //System.out.println("reopen: skip: isCurrent force gen=" + newSearcherGen + " vs current gen=" + noDeletesSearchingGen);
      noDeletesSearchingGen.set(newSearcherGen);
      synchronized(this) {
        notifyAll();
      }
      //System.out.println("reopen: skip: return");
      return false;
    }

    //System.out.println("indexingGen now " + indexingGen);

    // .reopen() returns a new reference:

    // Start from whichever searcher is most current:
    final IndexSearcher startSearcher = noDeletesSearchingGen.get() > searchingGen.get() ? noDeletesCurrentSearcher : currentSearcher;
    IndexReader nextReader = IndexReader.openIfChanged(startSearcher.getIndexReader(), writer, applyDeletes);
    if (nextReader == null) {
      // NOTE: doesn't happen currently in Lucene (reopen on
      // NRT reader always returns new reader), but could in
      // the future:
      nextReader = startSearcher.getIndexReader();
      nextReader.incRef();
    }

    if (nextReader != startSearcher.getIndexReader()) {
      final IndexSearcher nextSearcher = new IndexSearcher(nextReader, es);
      if (warmer != null) {
        boolean success = false;
        try {
          warmer.warm(nextSearcher);
          success = true;
        } finally {
          if (!success) {
            nextReader.decRef();
          }
        }
      }

      // Transfer reference to swapSearcher:
      swapSearcher(nextSearcher,
                   newSearcherGen,
                   applyDeletes);
      return true;
    } else {
      return false;
    }
  }

  // Steals a reference from newSearcher:
  private synchronized void swapSearcher(IndexSearcher newSearcher, long newSearchingGen, boolean applyDeletes) throws IOException {
    //System.out.println(Thread.currentThread().getName() + ": swap searcher gen=" + newSearchingGen + " applyDeletes=" + applyDeletes);
    
    // Always replace noDeletesCurrentSearcher:
    if (noDeletesCurrentSearcher != null) {
      noDeletesCurrentSearcher.getIndexReader().decRef();
    }
    noDeletesCurrentSearcher = newSearcher;
    assert newSearchingGen > noDeletesSearchingGen.get(): "newSearchingGen=" + newSearchingGen + " noDeletesSearchingGen=" + noDeletesSearchingGen;
    noDeletesSearchingGen.set(newSearchingGen);

    if (applyDeletes) {
      // Deletes were applied, so we also update currentSearcher:
      if (currentSearcher != null) {
        currentSearcher.getIndexReader().decRef();
      }
      currentSearcher = newSearcher;
      if (newSearcher != null) {
        newSearcher.getIndexReader().incRef();
      }
      assert newSearchingGen > searchingGen.get(): "newSearchingGen=" + newSearchingGen + " searchingGen=" + searchingGen;
      searchingGen.set(newSearchingGen);
    }

    notifyAll();
    //System.out.println(Thread.currentThread().getName() + ": done");
  }

  /** Close this NRTManager to future searching.  Any
   *  searches still in process in other threads won't be
   *  affected, and they should still call {@link #release}
   *  after they are done.
   *
   * <p><b>NOTE</b>: caller must separately close the writer. */
  @Override
  public void close() throws IOException {
    swapSearcher(null, indexingGen.getAndIncrement(), true);
  }
}
