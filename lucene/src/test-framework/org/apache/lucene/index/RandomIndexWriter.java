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
import java.util.Random;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.DocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter; // javadoc
import org.apache.lucene.index.codecs.CodecProvider;
import org.apache.lucene.index.values.Type;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.Version;
import org.apache.lucene.util._TestUtil;

/** Silly class that randomizes the indexing experience.  EG
 *  it may swap in a different merge policy/scheduler; may
 *  commit periodically; may or may not optimize in the end,
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
  private CodecProvider codecProvider;

  // Randomly calls Thread.yield so we mixup thread scheduling
  private static final class MockIndexWriter extends IndexWriter {

    private final Random r;

    public MockIndexWriter(Random r,Directory dir, IndexWriterConfig conf) throws IOException {
      super(dir, conf);
      // must make a private random since our methods are
      // called from different threads; else test failures may
      // not be reproducible from the original seed
      this.r = new Random(r.nextInt());
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
    this.r = r;
    w = new MockIndexWriter(r, dir, c);
    flushAt = _TestUtil.nextInt(r, 10, 1000);
    if (LuceneTestCase.VERBOSE) {
      System.out.println("RIW config=" + w.getConfig());
      System.out.println("codec default=" + w.getConfig().getCodecProvider().getDefaultFieldCodec());
      w.setInfoStream(System.out);
    }
    /* TODO: find some what to make that random...
     * This must be fixed across all fixed bytes 
     * fields in one index. so if you open another writer
     * this might change if I use r.nextInt(x)
     * maybe we can peek at the existing files here? 
     */
    fixedBytesLength = 37; 
    docValuesFieldPrefix = r.nextLong();
    codecProvider =  w.getConfig().getCodecProvider();
    switchDoDocValues();
  } 

  private void switchDoDocValues() {
    // randomly enable / disable docValues 
    doDocValues = r.nextInt(10) != 0;
  }
  
  /**
   * Adds a Document.
   * @see IndexWriter#addDocument(Document)
   */
  public void addDocument(Document doc) throws IOException {
    if (doDocValues) {
      randomPerDocFieldValues(r, doc);
    }
    w.addDocument(doc);
    
    maybeCommit();
  }
  
  private void randomPerDocFieldValues(Random random, Document doc) {
    
    Type[] values = Type.values();
    Type type = values[random.nextInt(values.length)];
    String name = "random_" + type.name() + "" + docValuesFieldPrefix;
    if ("PreFlex".equals(codecProvider.getFieldCodec(name)) || doc.getFieldable(name) != null)
        return;
    DocValuesField docValuesField = new DocValuesField(name);
    switch (type) {
    case BYTES_FIXED_DEREF:
    case BYTES_FIXED_SORTED:
    case BYTES_FIXED_STRAIGHT:
      final String randomUnicodeString = _TestUtil.randomUnicodeString(random, fixedBytesLength);
      BytesRef fixedRef = new BytesRef(randomUnicodeString);
      if (fixedRef.length > fixedBytesLength) {
        fixedRef = new BytesRef(fixedRef.bytes, 0, fixedBytesLength);
      } else {
        fixedRef.grow(fixedBytesLength);
        fixedRef.length = fixedBytesLength;
      }
      docValuesField.setBytes(fixedRef, type);
      break;
    case BYTES_VAR_DEREF:
    case BYTES_VAR_SORTED:
    case BYTES_VAR_STRAIGHT:
      BytesRef ref = new BytesRef(_TestUtil.randomUnicodeString(random, 200));
      docValuesField.setBytes(ref, type);
      break;
    case FLOAT_32:
      docValuesField.setFloat(random.nextFloat());
      break;
    case FLOAT_64:
      docValuesField.setFloat(random.nextDouble());
      break;
    case INTS:
      docValuesField.setInt(random.nextInt());
      break;
    default:
      throw new IllegalArgumentException("no such type: " + type);
    }

    doc.add(docValuesField);
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
  
  /**
   * Updates a document.
   * @see IndexWriter#updateDocument(Term, Document)
   */
  public void updateDocument(Term t, Document doc) throws IOException {
    if (doDocValues) {
      randomPerDocFieldValues(r, doc);
    }
    w.updateDocument(t, doc);
    maybeCommit();
  }
  
  public void addIndexes(Directory... dirs) throws CorruptIndexException, IOException {
    w.addIndexes(dirs);
  }
  
  public void deleteDocuments(Term term) throws CorruptIndexException, IOException {
    w.deleteDocuments(term);
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

  public IndexReader getReader() throws IOException {
    return getReader(true);
  }

  private void doRandomOptimize() throws IOException {
    final int segCount = w.getSegmentCount();
    if (r.nextBoolean() || segCount == 0) {
      // full optimize
      w.optimize();
    } else {
      // partial optimize
      final int limit = _TestUtil.nextInt(r, 1, segCount);
      w.optimize(limit);
      assert w.getSegmentCount() <= limit: "limit=" + limit + " actual=" + w.getSegmentCount();
    }
    switchDoDocValues();
  }

  public IndexReader getReader(boolean applyDeletions) throws IOException {
    getReaderCalled = true;
    if (r.nextInt(4) == 2) {
      doRandomOptimize();
    }
    // If we are writing with PreFlexRW, force a full
    // IndexReader.open so terms are sorted in codepoint
    // order during searching:
    if (!applyDeletions || !w.codecs.getDefaultFieldCodec().equals("PreFlex") && r.nextBoolean()) {
      if (LuceneTestCase.VERBOSE) {
        System.out.println("RIW.getReader: use NRT reader");
      }
      return w.getReader(applyDeletions);
    } else {
      if (LuceneTestCase.VERBOSE) {
        System.out.println("RIW.getReader: open new reader");
      }
      w.commit();
      switchDoDocValues();
      return IndexReader.open(w.getDirectory(), new KeepOnlyLastCommitDeletionPolicy(), r.nextBoolean(), _TestUtil.nextInt(r, 1, 10), w.getConfig().getCodecProvider());
    }
  }

  /**
   * Close this writer.
   * @see IndexWriter#close()
   */
  public void close() throws IOException {
    // if someone isn't using getReader() API, we want to be sure to
    // maybeOptimize since presumably they might open a reader on the dir.
    if (getReaderCalled == false && r.nextInt(4) == 2) {
      doRandomOptimize();
    }
    w.close();
  }

  /**
   * Forces an optimize.
   * <p>
   * NOTE: this should be avoided in tests unless absolutely necessary,
   * as it will result in less test coverage.
   * @see IndexWriter#optimize()
   */
  public void optimize() throws IOException {
    w.optimize();
  }
}
