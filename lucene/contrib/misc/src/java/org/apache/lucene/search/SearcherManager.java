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

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.NRTManager; // javadocs
import org.apache.lucene.search.IndexSearcher; // javadocs
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;

/**
 * Utility class to safely share {@link IndexSearcher} instances across multiple
 * threads, while periodically reopening. This class ensures each searcher is
 * closed only once all threads have finished using it.
 * 
 * <p>
 * Use {@link #acquire} to obtain the current searcher, and {@link #release} to
 * release it, like this:
 * 
 * <pre class="prettyprint">
 * IndexSearcher s = manager.acquire();
 * try {
 *   // Do searching, doc retrieval, etc. with s
 * } finally {
 *   manager.release(s);
 * }
 * // Do not use s after this!
 * s = null;
 * </pre>
 * 
 * <p>
 * In addition you should periodically call {@link #maybeReopen}. While it's
 * possible to call this just before running each query, this is discouraged
 * since it penalizes the unlucky queries that do the reopen. It's better to use
 * a separate background thread, that periodically calls maybeReopen. Finally,
 * be sure to call {@link #close} once you are done.
 * 
 * <p>
 * <b>NOTE</b>: if you have an {@link IndexWriter}, it's better to use
 * {@link NRTManager} since that class pulls near-real-time readers from the
 * IndexWriter.
 * 
 * @lucene.experimental
 */

public final class SearcherManager {

  private volatile IndexSearcher currentSearcher;
  private final ExecutorService es;
  private final SearcherWarmer warmer;
  private final Semaphore reopenLock = new Semaphore(1);
  
  /**
   * Creates and returns a new SearcherManager from the given {@link IndexWriter}. 
   * @param writer the IndexWriter to open the IndexReader from.
   * @param applyAllDeletes If <code>true</code>, all buffered deletes will
   *        be applied (made visible) in the {@link IndexSearcher} / {@link IndexReader}.
   *        If <code>false</code>, the deletes may or may not be applied, but remain buffered 
   *        (in IndexWriter) so that they will be applied in the future.
   *        Applying deletes can be costly, so if your app can tolerate deleted documents
   *        being returned you might gain some performance by passing <code>false</code>.
   *        See {@link IndexReader#openIfChanged(IndexReader, IndexWriter, boolean)}.
   * @param warmer An optional {@link SearcherWarmer}. Pass
   *        <code>null</code> if you don't require the searcher to warmed
   *        before going live.  If this is  <code>non-null</code> then a
   *        merged segment warmer is installed on the
   *        provided IndexWriter's config.
   * @param es An optional {@link ExecutorService} so different segments can
   *        be searched concurrently (see {@link
   *        IndexSearcher#IndexSearcher(IndexReader,ExecutorService)}.  Pass <code>null</code>
   *        to search segments sequentially.
   *        
   * @throws IOException
   */
  public SearcherManager(IndexWriter writer, boolean applyAllDeletes,
      final SearcherWarmer warmer, final ExecutorService es) throws IOException {
    this.es = es;
    this.warmer = warmer;
    currentSearcher = new IndexSearcher(IndexReader.open(writer, applyAllDeletes));
    if (warmer != null) {
      writer.getConfig().setMergedSegmentWarmer(
          new IndexWriter.IndexReaderWarmer() {
            @Override
            public void warm(IndexReader reader) throws IOException {
              warmer.warm(new IndexSearcher(reader, es));
            }
          });
    }
  }

  /**
   * Creates and returns a new SearcherManager from the given {@link Directory}. 
   * @param dir the directory to open the IndexReader on.
   * @param warmer An optional {@link SearcherWarmer}.  Pass
   *        <code>null</code> if you don't require the searcher to warmed
   *        before going live.  If this is  <code>non-null</code> then a
   *        merged segment warmer is installed on the
   *        provided IndexWriter's config.
   * @param es And optional {@link ExecutorService} so different segments can
   *        be searched concurrently (see {@link
   *        IndexSearcher#IndexSearcher(IndexReader,ExecutorService)}.  Pass <code>null</code>
   *        to search segments sequentially.
   *        
   * @throws IOException
   */
  public SearcherManager(Directory dir, SearcherWarmer warmer,
      ExecutorService es) throws IOException {
    this.es = es;
    this.warmer = warmer;
    currentSearcher = new IndexSearcher(IndexReader.open(dir, true), es);
  }

  /**
   * You must call this, periodically, to perform a reopen. This calls
   * {@link IndexReader#openIfChanged(IndexReader)} with the underlying reader, and if that returns a
   * new reader, it's warmed (if you provided a {@link SearcherWarmer} and then
   * swapped into production.
   * 
   * <p>
   * <b>Threads</b>: it's fine for more than one thread to call this at once.
   * Only the first thread will attempt the reopen; subsequent threads will see
   * that another thread is already handling reopen and will return immediately.
   * Note that this means if another thread is already reopening then subsequent
   * threads will return right away without waiting for the reader reopen to
   * complete.
   * </p>
   * 
   * <p>
   * This method returns true if a new reader was in fact opened or 
   * if the current searcher has no pending changes.
   * </p>
   */
  public boolean maybeReopen() throws IOException {
    ensureOpen();
    // Ensure only 1 thread does reopen at once; other
    // threads just return immediately:
    if (reopenLock.tryAcquire()) {
      try {
        // IR.openIfChanged preserves NRT and applyDeletes
        // in the newly returned reader:
        final IndexReader newReader = IndexReader.openIfChanged(currentSearcher.getIndexReader());
        if (newReader != null) {
          final IndexSearcher newSearcher = new IndexSearcher(newReader, es);
          boolean success = false;
          try {
            if (warmer != null) {
              warmer.warm(newSearcher);
            }
            swapSearcher(newSearcher);
            success = true;
          } finally {
            if (!success) {
              release(newSearcher);
            }
          }
        }
        return true;
      } finally {
        reopenLock.release();
      }
    } else {
      return false;
    }
  }
  
  /**
   * Returns <code>true</code> if no changes have occured since this searcher
   * ie. reader was opened, otherwise <code>false</code>.
   * @see IndexReader#isCurrent() 
   */
  public boolean isSearcherCurrent() throws CorruptIndexException,
      IOException {
    final IndexSearcher searcher = acquire();
    try {
      return searcher.getIndexReader().isCurrent();
    } finally {
      release(searcher);
    }
  }

  /**
   * Release the searcher previously obtained with {@link #acquire}.
   * 
   * <p>
   * <b>NOTE</b>: it's safe to call this after {@link #close}.
   */
  public void release(IndexSearcher searcher) throws IOException {
    assert searcher != null;
    searcher.getIndexReader().decRef();
  }

  /**
   * Close this SearcherManager to future searching. Any searches still in
   * process in other threads won't be affected, and they should still call
   * {@link #release} after they are done.
   */
  public synchronized void close() throws IOException {
    if (currentSearcher != null) {
      // make sure we can call this more than once
      // closeable javadoc says:
      // if this is already closed then invoking this method has no effect.
      swapSearcher(null);
    }
  }

  /**
   * Obtain the current IndexSearcher. You must match every call to acquire with
   * one call to {@link #release}; it's best to do so in a finally clause.
   */
  public IndexSearcher acquire() {
    IndexSearcher searcher;
    do {
      if ((searcher = currentSearcher) == null) {
        throw new AlreadyClosedException("this SearcherManager is closed");
      }
    } while (!searcher.getIndexReader().tryIncRef());
    return searcher;
  }

  private void ensureOpen() {
    if (currentSearcher == null) {
      throw new AlreadyClosedException("this SearcherManager is closed");
    }
  }

  private synchronized void swapSearcher(IndexSearcher newSearcher) throws IOException {
    ensureOpen();
    final IndexSearcher oldSearcher = currentSearcher;
    currentSearcher = newSearcher;
    release(oldSearcher);
  }
 
}
