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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.NRTManager; // javadocs
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;

/** Utility class to safely share {@link IndexSearcher} instances
 *  across multiple threads, while periodically reopening.
 *  This class ensures each IndexSearcher instance is not
 *  closed until it is no longer needed.
 *
 *  <p>Use {@link #get} to obtain the current searcher, and
 *  {@link #release} to release it, like this:
 *
 *  <pre>
 *    IndexSearcher s = manager.get();
 *    try {
 *      // Do searching, doc retrieval, etc. with s
 *    } finally {
 *      manager.release(s);
 *    }
 *    // Do not use s after this!
 *    s = null;
 *  </pre>
 *
 *  <p>In addition you should periodically call {@link
 *  #maybeReopen}.  While it's possible to call this just
 *  before running each query, this is discouraged since it
 *  penalizes the unlucky queries that do the reopen.  It's
 *  better to  use a separate background thread, that
 *  periodically calls maybeReopen.  Finally, be sure to
 *  call {@link #close} once you are done.
 *
 *  <p><b>NOTE</b>: if you have an {@link IndexWriter}, it's
 *  better to use {@link NRTManager} since that class pulls
 *  near-real-time readers from the IndexWriter.
 *
 *  @lucene.experimental
 */

public class SearcherManager implements Closeable {

  // Current searcher
  private volatile IndexSearcher currentSearcher;
  private final SearcherWarmer warmer;
  private final AtomicBoolean reopening = new AtomicBoolean();
  private final ExecutorService es;

  /** Opens an initial searcher from the Directory.
   *
   * @param dir Directory to open the searcher from
   *
   * @param warmer optional {@link SearcherWarmer}.  Pass
   *        null if you don't require the searcher to warmed
   *        before going live.
   *
   *  <p><b>NOTE</b>: the provided {@link SearcherWarmer} is
   *  not invoked for the initial searcher; you should
   *  warm it yourself if necessary.
   */
  public SearcherManager(Directory dir, SearcherWarmer warmer) throws IOException {
    this(dir, warmer, null);
  }

  /** Opens an initial searcher from the Directory.
   *
   * @param dir Directory to open the searcher from
   *
   * @param warmer optional {@link SearcherWarmer}.  Pass
   *        null if you don't require the searcher to warmed
   *        before going live.
   *
   * @param es optional ExecutorService so different segments can
   *        be searched concurrently (see {@link
   *        IndexSearcher#IndexSearcher(IndexReader,ExecutorService)}.  Pass null
   *        to search segments sequentially.
   *
   *  <p><b>NOTE</b>: the provided {@link SearcherWarmer} is
   *  not invoked for the initial searcher; you should
   *  warm it yourself if necessary.
   */
  public SearcherManager(Directory dir, SearcherWarmer warmer, ExecutorService es) throws IOException {
    this.es = es;
    currentSearcher = new IndexSearcher(IndexReader.open(dir), this.es);
    this.warmer = warmer;
  }

  /** You must call this, periodically, to perform a
   *  reopen.  This calls {@link IndexReader#reopen} on the
   *  underlying reader, and if that returns a new reader,
   *  it's warmed (if you provided a {@link SearcherWarmer}
   *  and then swapped into production.
   *
   *  <p><b>Threads</b>: it's fine for more than one thread to
   *  call this at once.  Only the first thread will attempt
   *  the reopen; subsequent threads will see that another
   *  thread is already handling reopen and will return
   *  immediately.  Note that this means if another thread
   *  is already reopening then subsequent threads will
   *  return right away without waiting for the reader
   *  reopen to complete.</p>
   *
   *  <p>This method returns true if a new reader was in
   *  fact opened.</p>
   */
  public boolean maybeReopen()
    throws  IOException {

    if (currentSearcher == null) {
      throw new AlreadyClosedException("this SearcherManager is closed");
    }

    // Ensure only 1 thread does reopen at once; other
    // threads just return immediately:
    if (!reopening.getAndSet(true)) {
      try {
        IndexReader newReader = currentSearcher.getIndexReader().reopen();
        if (newReader != currentSearcher.getIndexReader()) {
          IndexSearcher newSearcher = new IndexSearcher(newReader, es);
          if (warmer != null) {
            warmer.warm(newSearcher);
          }
          swapSearcher(newSearcher);
          return true;
        } else {
          return false;
        }
      } finally {
        reopening.set(false);
      }
    } else {
      return false;
    }
  }

  /** Obtain the current IndexSearcher.  You must match
   *  every call to get with one call to {@link #release};
   *  it's best to do so in a finally clause. */
  public synchronized IndexSearcher get() {
    if (currentSearcher == null) {
      throw new AlreadyClosedException("this SearcherManager is closed");
    }
    currentSearcher.getIndexReader().incRef();
    return currentSearcher;
  }    

  /** Release the searcher previously obtained with {@link
   *  #get}.
   *
   *  <p><b>NOTE</b>: it's safe to call this after {@link
   *  #close}. */
  public void release(IndexSearcher searcher)
    throws IOException {
    searcher.getIndexReader().decRef();
  }

  // Replaces old searcher with new one
  private synchronized void swapSearcher(IndexSearcher newSearcher)
    throws IOException {
    IndexSearcher oldSearcher = currentSearcher;
    if (oldSearcher == null) {
      throw new AlreadyClosedException("this SearcherManager is closed");
    }
    currentSearcher = newSearcher;
    release(oldSearcher);
  }

  /** Close this SearcherManager to future searching.  Any
   *  searches still in process in other threads won't be
   *  affected, and they should still call {@link #release}
   *  after they are done. */
  @Override
  public void close() throws IOException {
    swapSearcher(null);
  }
}
