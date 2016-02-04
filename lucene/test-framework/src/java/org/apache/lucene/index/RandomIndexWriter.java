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
package org.apache.lucene.index;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.Random;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.NullInfoStream;
import org.apache.lucene.util.TestUtil;

/** Silly class that randomizes the indexing experience.  EG
 *  it may swap in a different merge policy/scheduler; may
 *  commit periodically; may or may not forceMerge in the end,
 *  may flush by doc count instead of RAM, etc. 
 */

public class RandomIndexWriter implements Closeable {

  public IndexWriter w;
  private final Random r;
  int docCount;
  int flushAt;
  private double flushAtFactor = 1.0;
  private boolean getReaderCalled;
  private final Analyzer analyzer; // only if WE created it (then we close it)

  /** Returns an indexwriter that randomly mixes up thread scheduling (by yielding at test points) */
  public static IndexWriter mockIndexWriter(Directory dir, IndexWriterConfig conf, Random r) throws IOException {
    // Randomly calls Thread.yield so we mixup thread scheduling
    final Random random = new Random(r.nextLong());
    return mockIndexWriter(r, dir, conf, new TestPoint() {
      @Override
      public void apply(String message) {
        if (random.nextInt(4) == 2)
          Thread.yield();
      }
    });
  }
  
  /** Returns an indexwriter that enables the specified test point */
  public static IndexWriter mockIndexWriter(Random r, Directory dir, IndexWriterConfig conf, TestPoint testPoint) throws IOException {
    conf.setInfoStream(new TestPointInfoStream(conf.getInfoStream(), testPoint));
    DirectoryReader reader = null;
    if (r.nextBoolean() && DirectoryReader.indexExists(dir) && conf.getOpenMode() != IndexWriterConfig.OpenMode.CREATE) {
      if (LuceneTestCase.VERBOSE) {
        System.out.println("RIW: open writer from reader");
      }
      reader = DirectoryReader.open(dir);
      conf.setIndexCommit(reader.getIndexCommit());
    }

    IndexWriter iw;
    boolean success = false;
    try {
      iw = new IndexWriter(dir, conf);
      success = true;
    } finally {
      if (reader != null) {
        if (success) {
          IOUtils.close(reader);
        } else {
          IOUtils.closeWhileHandlingException(reader);
        }
      }
    }
    iw.enableTestPoints = true;
    return iw;
  }

  /** create a RandomIndexWriter with a random config: Uses MockAnalyzer */
  public RandomIndexWriter(Random r, Directory dir) throws IOException {
    this(r, dir, LuceneTestCase.newIndexWriterConfig(r, new MockAnalyzer(r)), true);
  }
  
  /** create a RandomIndexWriter with a random config */
  public RandomIndexWriter(Random r, Directory dir, Analyzer a) throws IOException {
    this(r, dir, LuceneTestCase.newIndexWriterConfig(r, a));
  }
  
  /** create a RandomIndexWriter with the provided config */
  public RandomIndexWriter(Random r, Directory dir, IndexWriterConfig c) throws IOException {
    this(r, dir, c, false);
  }
      
  private RandomIndexWriter(Random r, Directory dir, IndexWriterConfig c, boolean closeAnalyzer) throws IOException {
    // TODO: this should be solved in a different way; Random should not be shared (!).
    this.r = new Random(r.nextLong());
    w = mockIndexWriter(dir, c, r);
    flushAt = TestUtil.nextInt(r, 10, 1000);
    if (closeAnalyzer) {
      analyzer = w.getAnalyzer();
    } else {
      analyzer = null;
    }
    if (LuceneTestCase.VERBOSE) {
      System.out.println("RIW dir=" + dir);
    }

    // Make sure we sometimes test indices that don't get
    // any forced merges:
    doRandomForceMerge = !(c.getMergePolicy() instanceof NoMergePolicy) && r.nextBoolean();
  } 
  
  /**
   * Adds a Document.
   * @see IndexWriter#addDocument(Iterable)
   */
  public <T extends IndexableField> void addDocument(final Iterable<T> doc) throws IOException {
    LuceneTestCase.maybeChangeLiveIndexWriterConfig(r, w.getConfig());
    if (r.nextInt(5) == 3) {
      // TODO: maybe, we should simply buffer up added docs
      // (but we need to clone them), and only when
      // getReader, commit, etc. are called, we do an
      // addDocuments?  Would be better testing.
      w.addDocuments(new Iterable<Iterable<T>>() {

        @Override
        public Iterator<Iterable<T>> iterator() {
          return new Iterator<Iterable<T>>() {
            boolean done;
            
            @Override
            public boolean hasNext() {
              return !done;
            }

            @Override
            public void remove() {
              throw new UnsupportedOperationException();
            }

            @Override
            public Iterable<T> next() {
              if (done) {
                throw new IllegalStateException();
              }
              done = true;
              return doc;
            }
          };
        }
        });
    } else {
      w.addDocument(doc);
    }
    
    maybeFlushOrCommit();
  }

  private void maybeFlushOrCommit() throws IOException {
    LuceneTestCase.maybeChangeLiveIndexWriterConfig(r, w.getConfig());
    if (docCount++ == flushAt) {
      if (r.nextBoolean()) {
        if (LuceneTestCase.VERBOSE) {
          System.out.println("RIW.add/updateDocument: now doing a flush at docCount=" + docCount);
        }
        w.flush();
      } else {
        if (LuceneTestCase.VERBOSE) {
          System.out.println("RIW.add/updateDocument: now doing a commit at docCount=" + docCount);
        }
        w.commit();
      }
      flushAt += TestUtil.nextInt(r, (int) (flushAtFactor * 10), (int) (flushAtFactor * 1000));
      if (flushAtFactor < 2e6) {
        // gradually but exponentially increase time b/w flushes
        flushAtFactor *= 1.05;
      }
    }
  }
  
  public void addDocuments(Iterable<? extends Iterable<? extends IndexableField>> docs) throws IOException {
    LuceneTestCase.maybeChangeLiveIndexWriterConfig(r, w.getConfig());
    w.addDocuments(docs);
    maybeFlushOrCommit();
  }

  public void updateDocuments(Term delTerm, Iterable<? extends Iterable<? extends IndexableField>> docs) throws IOException {
    LuceneTestCase.maybeChangeLiveIndexWriterConfig(r, w.getConfig());
    w.updateDocuments(delTerm, docs);
    maybeFlushOrCommit();
  }

  /**
   * Updates a document.
   * @see IndexWriter#updateDocument(Term, Iterable)
   */
  public <T extends IndexableField> void updateDocument(Term t, final Iterable<T> doc) throws IOException {
    LuceneTestCase.maybeChangeLiveIndexWriterConfig(r, w.getConfig());
    if (r.nextInt(5) == 3) {
      w.updateDocuments(t, new Iterable<Iterable<T>>() {

        @Override
        public Iterator<Iterable<T>> iterator() {
          return new Iterator<Iterable<T>>() {
            boolean done;
            
            @Override
            public boolean hasNext() {
              return !done;
            }

            @Override
            public void remove() {
              throw new UnsupportedOperationException();
            }

            @Override
            public Iterable<T> next() {
              if (done) {
                throw new IllegalStateException();
              }
              done = true;
              return doc;
            }
          };
        }
        });
    } else {
      w.updateDocument(t, doc);
    }
    maybeFlushOrCommit();
  }
  
  public void addIndexes(Directory... dirs) throws IOException {
    LuceneTestCase.maybeChangeLiveIndexWriterConfig(r, w.getConfig());
    w.addIndexes(dirs);
  }

  public void addIndexes(CodecReader... readers) throws IOException {
    LuceneTestCase.maybeChangeLiveIndexWriterConfig(r, w.getConfig());
    w.addIndexes(readers);
  }
  
  public void updateNumericDocValue(Term term, String field, Long value) throws IOException {
    LuceneTestCase.maybeChangeLiveIndexWriterConfig(r, w.getConfig());
    w.updateNumericDocValue(term, field, value);
  }
  
  public void updateBinaryDocValue(Term term, String field, BytesRef value) throws IOException {
    LuceneTestCase.maybeChangeLiveIndexWriterConfig(r, w.getConfig());
    w.updateBinaryDocValue(term, field, value);
  }
  
  public void updateDocValues(Term term, Field... updates) throws IOException {
    LuceneTestCase.maybeChangeLiveIndexWriterConfig(r, w.getConfig());
    w.updateDocValues(term, updates);
  }
  
  public void deleteDocuments(Term term) throws IOException {
    LuceneTestCase.maybeChangeLiveIndexWriterConfig(r, w.getConfig());
    w.deleteDocuments(term);
  }

  public void deleteDocuments(Query q) throws IOException {
    LuceneTestCase.maybeChangeLiveIndexWriterConfig(r, w.getConfig());
    w.deleteDocuments(q);
  }
  
  public void commit() throws IOException {
    LuceneTestCase.maybeChangeLiveIndexWriterConfig(r, w.getConfig());
    w.commit();
  }
  
  public int numDocs() {
    return w.numDocs();
  }

  public int maxDoc() {
    return w.maxDoc();
  }

  public void deleteAll() throws IOException {
    w.deleteAll();
  }

  public DirectoryReader getReader() throws IOException {
    LuceneTestCase.maybeChangeLiveIndexWriterConfig(r, w.getConfig());
    return getReader(true);
  }

  private boolean doRandomForceMerge = true;
  private boolean doRandomForceMergeAssert = true;

  public void forceMergeDeletes(boolean doWait) throws IOException {
    LuceneTestCase.maybeChangeLiveIndexWriterConfig(r, w.getConfig());
    w.forceMergeDeletes(doWait);
  }

  public void forceMergeDeletes() throws IOException {
    LuceneTestCase.maybeChangeLiveIndexWriterConfig(r, w.getConfig());
    w.forceMergeDeletes();
  }

  public void setDoRandomForceMerge(boolean v) {
    doRandomForceMerge = v;
  }

  public void setDoRandomForceMergeAssert(boolean v) {
    doRandomForceMergeAssert = v;
  }

  private void doRandomForceMerge() throws IOException {
    if (doRandomForceMerge) {
      final int segCount = w.getSegmentCount();
      if (r.nextBoolean() || segCount == 0) {
        // full forceMerge
        if (LuceneTestCase.VERBOSE) {
          System.out.println("RIW: doRandomForceMerge(1)");
        }
        w.forceMerge(1);
      } else if (r.nextBoolean()) {
        // partial forceMerge
        final int limit = TestUtil.nextInt(r, 1, segCount);
        if (LuceneTestCase.VERBOSE) {
          System.out.println("RIW: doRandomForceMerge(" + limit + ")");
        }
        w.forceMerge(limit);
        assert !doRandomForceMergeAssert || w.getSegmentCount() <= limit: "limit=" + limit + " actual=" + w.getSegmentCount();
      } else {
        if (LuceneTestCase.VERBOSE) {
          System.out.println("RIW: do random forceMergeDeletes()");
        }
        w.forceMergeDeletes();
      }
    }
  }

  public DirectoryReader getReader(boolean applyDeletions) throws IOException {
    LuceneTestCase.maybeChangeLiveIndexWriterConfig(r, w.getConfig());
    getReaderCalled = true;
    if (r.nextInt(20) == 2) {
      doRandomForceMerge();
    }
    if (!applyDeletions || r.nextBoolean()) {
      if (LuceneTestCase.VERBOSE) {
        System.out.println("RIW.getReader: use NRT reader");
      }
      if (r.nextInt(5) == 1) {
        w.commit();
      }
      return w.getReader(applyDeletions);
    } else {
      if (LuceneTestCase.VERBOSE) {
        System.out.println("RIW.getReader: open new reader");
      }
      w.commit();
      if (r.nextBoolean()) {
        return DirectoryReader.open(w.getDirectory());
      } else {
        return w.getReader(applyDeletions);
      }
    }
  }

  /**
   * Close this writer.
   * @see IndexWriter#close()
   */
  @Override
  public void close() throws IOException {
    if (w.isClosed() == false) {
      LuceneTestCase.maybeChangeLiveIndexWriterConfig(r, w.getConfig());
    }
    // if someone isn't using getReader() API, we want to be sure to
    // forceMerge since presumably they might open a reader on the dir.
    if (getReaderCalled == false && r.nextInt(8) == 2 && w.isClosed() == false) {
      doRandomForceMerge();
      if (w.getConfig().getCommitOnClose() == false) {
        // index may have changed, must commit the changes, or otherwise they are discarded by the call to close()
        w.commit();
      }
    }
    IOUtils.close(w, analyzer);
  }

  /**
   * Forces a forceMerge.
   * <p>
   * NOTE: this should be avoided in tests unless absolutely necessary,
   * as it will result in less test coverage.
   * @see IndexWriter#forceMerge(int)
   */
  public void forceMerge(int maxSegmentCount) throws IOException {
    LuceneTestCase.maybeChangeLiveIndexWriterConfig(r, w.getConfig());
    w.forceMerge(maxSegmentCount);
  }
  
  static final class TestPointInfoStream extends InfoStream {
    private final InfoStream delegate;
    private final TestPoint testPoint;
    
    public TestPointInfoStream(InfoStream delegate, TestPoint testPoint) {
      this.delegate = delegate == null ? new NullInfoStream(): delegate;
      this.testPoint = testPoint;
    }

    @Override
    public void close() throws IOException {
      delegate.close();
    }

    @Override
    public void message(String component, String message) {
      if ("TP".equals(component)) {
        testPoint.apply(message);
      }
      if (delegate.isEnabled(component)) {
        delegate.message(component, message);
      }
    }
    
    @Override
    public boolean isEnabled(String component) {
      return "TP".equals(component) || delegate.isEnabled(component);
    }
  }
  
  /** Writes all in-memory segments to the {@link Directory}. */
  public final void flush() throws IOException {
    w.flush();
  }

  /**
   * Simple interface that is executed for each <tt>TP</tt> {@link InfoStream} component
   * message. See also {@link RandomIndexWriter#mockIndexWriter(Random, Directory, IndexWriterConfig, TestPoint)}
   */
  public static interface TestPoint {
    public abstract void apply(String message);
  }
}
