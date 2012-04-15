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
import java.util.Iterator;
import java.util.Random;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.DocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter; // javadoc
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.Version;
import org.apache.lucene.util._TestUtil;

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
  private final int fixedBytesLength;
  private final long docValuesFieldPrefix;
  private volatile boolean doDocValues;
  private final Codec codec; // sugar

  // Randomly calls Thread.yield so we mixup thread scheduling
  private static final class MockIndexWriter extends IndexWriter {

    private final Random r;

    public MockIndexWriter(Random r, Directory dir, IndexWriterConfig conf) throws IOException {
      super(dir, conf);
      // TODO: this should be solved in a different way; Random should not be shared (!).
      this.r = new Random(r.nextLong());
    }

    @Override
    boolean testPoint(String name) {
      if (r.nextInt(4) == 2)
        Thread.yield();
      return true;
    }
  }

  /** create a RandomIndexWriter with a random config: Uses TEST_VERSION_CURRENT and MockAnalyzer */
  public RandomIndexWriter(Random r, Directory dir) throws IOException {
    this(r, dir, LuceneTestCase.newIndexWriterConfig(r, LuceneTestCase.TEST_VERSION_CURRENT, new MockAnalyzer(r)));
  }
  
  /** create a RandomIndexWriter with a random config: Uses TEST_VERSION_CURRENT */
  public RandomIndexWriter(Random r, Directory dir, Analyzer a) throws IOException {
    this(r, dir, LuceneTestCase.newIndexWriterConfig(r, LuceneTestCase.TEST_VERSION_CURRENT, a));
  }
  
  /** create a RandomIndexWriter with a random config */
  public RandomIndexWriter(Random r, Directory dir, Version v, Analyzer a) throws IOException {
    this(r, dir, LuceneTestCase.newIndexWriterConfig(r, v, a));
  }
  
  /** create a RandomIndexWriter with the provided config */
  public RandomIndexWriter(Random r, Directory dir, IndexWriterConfig c) throws IOException {
    // TODO: this should be solved in a different way; Random should not be shared (!).
    this.r = new Random(r.nextLong());
    w = new MockIndexWriter(r, dir, c);
    flushAt = _TestUtil.nextInt(r, 10, 1000);
    codec = w.getConfig().getCodec();
    if (LuceneTestCase.VERBOSE) {
      System.out.println("RIW config=" + w.getConfig());
      System.out.println("codec default=" + codec.getName());
    }
    /* TODO: find some way to make this random...
     * This must be fixed across all fixed bytes 
     * fields in one index. so if you open another writer
     * this might change if I use r.nextInt(x)
     * maybe we can peek at the existing files here? 
     */
    fixedBytesLength = 17; 

    // NOTE: this means up to 13 * 5 unique fields (we have
    // 13 different DV types):
    docValuesFieldPrefix = r.nextInt(5);
    switchDoDocValues();

    // Make sure we sometimes test indices that don't get
    // any forced merges:
    doRandomForceMerge = r.nextBoolean();
  } 

  private void switchDoDocValues() {
    // randomly enable / disable docValues 
    doDocValues = LuceneTestCase.rarely(r);
    if (LuceneTestCase.VERBOSE) {
      if (doDocValues) {
        System.out.println("NOTE: RIW: turning on random DocValues fields");
      }
    }
  }
  
  /**
   * Adds a Document.
   * @see IndexWriter#addDocument(Iterable)
   */
  public <T extends IndexableField> void addDocument(final Iterable<T> doc) throws IOException {
    addDocument(doc, w.getAnalyzer());
  }

  public <T extends IndexableField> void addDocument(final Iterable<T> doc, Analyzer a) throws IOException {
    if (doDocValues && doc instanceof Document) {
      randomPerDocFieldValues(r, (Document) doc);
    }
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
        }, a);
    } else {
      w.addDocument(doc, a);
    }
    
    maybeCommit();
  }
  
  private void randomPerDocFieldValues(Random random, Document doc) {
    
    DocValues.Type[] values = DocValues.Type.values();
    DocValues.Type type = values[random.nextInt(values.length)];
    String name = "random_" + type.name() + "" + docValuesFieldPrefix;
    if ("Lucene3x".equals(codec.getName()) || doc.getField(name) != null) {
      return;
    }
    final Field f;
    switch (type) {
    case BYTES_FIXED_DEREF:
    case BYTES_FIXED_STRAIGHT:
    case BYTES_FIXED_SORTED:
      //make sure we use a valid unicode string with a fixed size byte length
      final String randomUnicodeString = _TestUtil.randomFixedByteLengthUnicodeString(random, fixedBytesLength);
      BytesRef fixedRef = new BytesRef(randomUnicodeString);
      if (fixedRef.length > fixedBytesLength) {
        fixedRef = new BytesRef(fixedRef.bytes, 0, fixedBytesLength);
      } else {
        fixedRef.grow(fixedBytesLength);
        fixedRef.length = fixedBytesLength;
      }
      f = new DocValuesField(name, fixedRef, type);
      break;
    case BYTES_VAR_DEREF:
    case BYTES_VAR_STRAIGHT:
    case BYTES_VAR_SORTED:
      f = new DocValuesField(name, new BytesRef(_TestUtil.randomUnicodeString(random, 20)), type);
      break;
    case FLOAT_32:
      f = new DocValuesField(name, random.nextFloat(), type);
      break;
    case FLOAT_64:
      f = new DocValuesField(name, random.nextDouble(), type);
      break;
    case VAR_INTS:
      f = new DocValuesField(name, random.nextLong(), type);
      break;
    case FIXED_INTS_16:
      f = new DocValuesField(name, random.nextInt(Short.MAX_VALUE), type);
      break;
    case FIXED_INTS_32:
      f = new DocValuesField(name, random.nextInt(), type);
      break;
    case FIXED_INTS_64:
      f = new DocValuesField(name, random.nextLong(), type);
      break;
    case FIXED_INTS_8:
      f = new DocValuesField(name, random.nextInt(128), type);
      break;
    default:
      throw new IllegalArgumentException("no such type: " + type);
    }
    doc.add(f);
  }

  private void maybeCommit() throws IOException {
    if (docCount++ == flushAt) {
      if (LuceneTestCase.VERBOSE) {
        System.out.println("RIW.add/updateDocument: now doing a commit at docCount=" + docCount);
      }
      w.commit();
      flushAt += _TestUtil.nextInt(r, (int) (flushAtFactor * 10), (int) (flushAtFactor * 1000));
      if (flushAtFactor < 2e6) {
        // gradually but exponentially increase time b/w flushes
        flushAtFactor *= 1.05;
      }
      switchDoDocValues();
    }
  }
  
  public void addDocuments(Iterable<? extends Iterable<? extends IndexableField>> docs) throws IOException {
    w.addDocuments(docs);
    maybeCommit();
  }

  public void updateDocuments(Term delTerm, Iterable<? extends Iterable<? extends IndexableField>> docs) throws IOException {
    w.updateDocuments(delTerm, docs);
    maybeCommit();
  }

  /**
   * Updates a document.
   * @see IndexWriter#updateDocument(Term, Iterable)
   */
  public <T extends IndexableField> void updateDocument(Term t, final Iterable<T> doc) throws IOException {
    if (doDocValues) {
      randomPerDocFieldValues(r, (Document) doc);
    }
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
    maybeCommit();
  }
  
  public void addIndexes(Directory... dirs) throws CorruptIndexException, IOException {
    w.addIndexes(dirs);
  }

  public void addIndexes(IndexReader... readers) throws CorruptIndexException, IOException {
    w.addIndexes(readers);
  }
  
  public void deleteDocuments(Term term) throws CorruptIndexException, IOException {
    w.deleteDocuments(term);
  }

  public void deleteDocuments(Query q) throws CorruptIndexException, IOException {
    w.deleteDocuments(q);
  }
  
  public void commit() throws CorruptIndexException, IOException {
    w.commit();
    switchDoDocValues();
  }
  
  public int numDocs() throws IOException {
    return w.numDocs();
  }

  public int maxDoc() {
    return w.maxDoc();
  }

  public void deleteAll() throws IOException {
    w.deleteAll();
  }

  public DirectoryReader getReader() throws IOException {
    return getReader(true);
  }

  private boolean doRandomForceMerge = true;
  private boolean doRandomForceMergeAssert = true;

  public void forceMergeDeletes(boolean doWait) throws IOException {
    w.forceMergeDeletes(doWait);
  }

  public void forceMergeDeletes() throws IOException {
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
        w.forceMerge(1);
      } else {
        // partial forceMerge
        final int limit = _TestUtil.nextInt(r, 1, segCount);
        w.forceMerge(limit);
        assert !doRandomForceMergeAssert || w.getSegmentCount() <= limit: "limit=" + limit + " actual=" + w.getSegmentCount();
      }
    }
    switchDoDocValues();
  }

  public DirectoryReader getReader(boolean applyDeletions) throws IOException {
    getReaderCalled = true;
    if (r.nextInt(20) == 2) {
      doRandomForceMerge();
    }
    // If we are writing with PreFlexRW, force a full
    // IndexReader.open so terms are sorted in codepoint
    // order during searching:
    if (!applyDeletions || !codec.getName().equals("Lucene3x") && r.nextBoolean()) {
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
      switchDoDocValues();
      if (r.nextBoolean()) {
        return IndexReader.open(w.getDirectory(), _TestUtil.nextInt(r, 1, 10));
      } else {
        return w.getReader(applyDeletions);
      }
    }
  }

  /**
   * Close this writer.
   * @see IndexWriter#close()
   */
  public void close() throws IOException {
    // if someone isn't using getReader() API, we want to be sure to
    // forceMerge since presumably they might open a reader on the dir.
    if (getReaderCalled == false && r.nextInt(8) == 2) {
      doRandomForceMerge();
    }
    w.close();
  }

  /**
   * Forces a forceMerge.
   * <p>
   * NOTE: this should be avoided in tests unless absolutely necessary,
   * as it will result in less test coverage.
   * @see IndexWriter#forceMerge(int)
   */
  public void forceMerge(int maxSegmentCount) throws IOException {
    w.forceMerge(maxSegmentCount);
  }
}
