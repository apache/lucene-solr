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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.function.LongSupplier;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.TestUtil;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 * Abstract class to do basic tests for a norms format.
 * NOTE: This test focuses on the norms impl, nothing else.
 * The [stretch] goal is for this test to be
 * so thorough in testing a new NormsFormat that if this
 * test passes, then all Lucene/Solr tests should also pass.  Ie,
 * if there is some bug in a given NormsFormat that this
 * test fails to catch then this test needs to be improved! */
public abstract class BaseNormsFormatTestCase extends BaseIndexFileFormatTestCase {

  /** Whether the codec supports sparse values. */
  protected boolean codecSupportsSparsity() {
    return true;
  }

  public void testByteRange() throws Exception {
    int iterations = atLeast(1);
    final Random r = random();
    for (int i = 0; i < iterations; i++) {
      doTestNormsVersusDocValues(1, new LongSupplier() {
        @Override
        public long getAsLong() {
          return TestUtil.nextLong(r, Byte.MIN_VALUE, Byte.MAX_VALUE);
        }
      });
    }
  }

  public void testSparseByteRange() throws Exception {
    assumeTrue("Requires sparse norms support", codecSupportsSparsity());
    int iterations = atLeast(1);
    final Random r = random();
    for (int i = 0; i < iterations; i++) {
      doTestNormsVersusDocValues(random().nextDouble(), new LongSupplier() {
        @Override
        public long getAsLong() {
          return TestUtil.nextLong(r, Byte.MIN_VALUE, Byte.MAX_VALUE);
        }
      });
    }
  }

  public void testShortRange() throws Exception {
    int iterations = atLeast(1);
    final Random r = random();
    for (int i = 0; i < iterations; i++) {
      doTestNormsVersusDocValues(1, new LongSupplier() {
        @Override
        public long getAsLong() {
          return TestUtil.nextLong(r, Short.MIN_VALUE, Short.MAX_VALUE);
        }
      });
    }
  }

  public void testSparseShortRange() throws Exception {
    assumeTrue("Requires sparse norms support", codecSupportsSparsity());
    int iterations = atLeast(1);
    final Random r = random();
    for (int i = 0; i < iterations; i++) {
      doTestNormsVersusDocValues(random().nextDouble(), new LongSupplier() {
        @Override
        public long getAsLong() {
          return TestUtil.nextLong(r, Short.MIN_VALUE, Short.MAX_VALUE);
        }
      });
    }
  }

  public void testLongRange() throws Exception {
    int iterations = atLeast(1);
    final Random r = random();
    for (int i = 0; i < iterations; i++) {
      doTestNormsVersusDocValues(1, new LongSupplier() {
        @Override
        public long getAsLong() {
          return TestUtil.nextLong(r, Long.MIN_VALUE, Long.MAX_VALUE);
        }
      });
    }
  }

  public void testSparseLongRange() throws Exception {
    assumeTrue("Requires sparse norms support", codecSupportsSparsity());
    int iterations = atLeast(1);
    final Random r = random();
    for (int i = 0; i < iterations; i++) {
      doTestNormsVersusDocValues(random().nextDouble(), new LongSupplier() {
        @Override
        public long getAsLong() {
          return TestUtil.nextLong(r, Long.MIN_VALUE, Long.MAX_VALUE);
        }
      });
    }
  }

  public void testFullLongRange() throws Exception {
    int iterations = atLeast(1);
    final Random r = random();
    for (int i = 0; i < iterations; i++) {
      doTestNormsVersusDocValues(1, new LongSupplier() {
        @Override
        public long getAsLong() {
          int thingToDo = r.nextInt(3);
          switch (thingToDo) {
            case 0: return Long.MIN_VALUE;
            case 1: return Long.MAX_VALUE;
            default:  return TestUtil.nextLong(r, Long.MIN_VALUE, Long.MAX_VALUE);
          }
        }
      });
    }
  }

  public void testSparseFullLongRange() throws Exception {
    assumeTrue("Requires sparse norms support", codecSupportsSparsity());
    int iterations = atLeast(1);
    final Random r = random();
    for (int i = 0; i < iterations; i++) {
      doTestNormsVersusDocValues(random().nextDouble(), new LongSupplier() {
        @Override
        public long getAsLong() {
          int thingToDo = r.nextInt(3);
          switch (thingToDo) {
            case 0: return Long.MIN_VALUE;
            case 1: return Long.MAX_VALUE;
            default:  return TestUtil.nextLong(r, Long.MIN_VALUE, Long.MAX_VALUE);
          }
        }
      });
    }
  }

  public void testFewValues() throws Exception {
    int iterations = atLeast(1);
    final Random r = random();
    for (int i = 0; i < iterations; i++) {
      doTestNormsVersusDocValues(1, new LongSupplier() {
        @Override
        public long getAsLong() {
          return r.nextBoolean() ? 20 : 3;
        }
      });
    }
  }

  public void testFewSparseValues() throws Exception {
    assumeTrue("Requires sparse norms support", codecSupportsSparsity());
    int iterations = atLeast(1);
    final Random r = random();
    for (int i = 0; i < iterations; i++) {
      doTestNormsVersusDocValues(random().nextDouble(), new LongSupplier() {
        @Override
        public long getAsLong() {
          return r.nextBoolean() ? 20 : 3;
        }
      });
    }
  }

  public void testFewLargeValues() throws Exception {
    int iterations = atLeast(1);
    final Random r = random();
    for (int i = 0; i < iterations; i++) {
      doTestNormsVersusDocValues(1, new LongSupplier() {
        @Override
        public long getAsLong() {
          return r.nextBoolean() ? 1000000L : -5000;
        }
      });
    }
  }

  public void testFewSparseLargeValues() throws Exception {
    assumeTrue("Requires sparse norms support", codecSupportsSparsity());
    int iterations = atLeast(1);
    final Random r = random();
    for (int i = 0; i < iterations; i++) {
      doTestNormsVersusDocValues(random().nextDouble(), new LongSupplier() {
        @Override
        public long getAsLong() {
          return r.nextBoolean() ? 1000000L : -5000;
        }
      });
    }
  }

  public void testAllZeros() throws Exception {
    int iterations = atLeast(1);
    for (int i = 0; i < iterations; i++) {
      doTestNormsVersusDocValues(1, new LongSupplier() {
        @Override
        public long getAsLong() {
          return 0;
        }
      });
    }
  }

  public void testSparseAllZeros() throws Exception {
    assumeTrue("Requires sparse norms support", codecSupportsSparsity());
    int iterations = atLeast(1);
    for (int i = 0; i < iterations; i++) {
      doTestNormsVersusDocValues(random().nextDouble(), new LongSupplier() {
        @Override
        public long getAsLong() {
          return 0;
        }
      });
    }
  }

  public void testMostZeros() throws Exception {
    int iterations = atLeast(1);
    final Random r = random();
    for (int i = 0; i < iterations; i++) {
      doTestNormsVersusDocValues(1, new LongSupplier() {
        @Override
        public long getAsLong() {
          return r.nextInt(100) == 0 ? TestUtil.nextLong(r, Byte.MIN_VALUE, Byte.MAX_VALUE) : 0;
        }
      });
    }
  }
  
  public void testOutliers() throws Exception {
    int iterations = atLeast(1);
    final Random r = random();
    for (int i = 0; i < iterations; i++) {
      final long commonValue = TestUtil.nextLong(r, Byte.MIN_VALUE, Byte.MAX_VALUE);
      doTestNormsVersusDocValues(1, new LongSupplier() {
        @Override
        public long getAsLong() {
          return r.nextInt(100) == 0 ? TestUtil.nextLong(r, Byte.MIN_VALUE, Byte.MAX_VALUE) : commonValue;
        }
      });
    }
  }

  public void testSparseOutliers() throws Exception {
    assumeTrue("Requires sparse norms support", codecSupportsSparsity());
    int iterations = atLeast(1);
    final Random r = random();
    for (int i = 0; i < iterations; i++) {
      final long commonValue = TestUtil.nextLong(r, Byte.MIN_VALUE, Byte.MAX_VALUE);
      doTestNormsVersusDocValues(random().nextDouble(), new LongSupplier() {
        @Override
        public long getAsLong() {
          return r.nextInt(100) == 0 ? TestUtil.nextLong(r, Byte.MIN_VALUE, Byte.MAX_VALUE) : commonValue;
        }
      });
    }
  }

  public void testOutliers2() throws Exception {
    int iterations = atLeast(1);
    final Random r = random();
    for (int i = 0; i < iterations; i++) {
      final long commonValue = TestUtil.nextLong(r, Byte.MIN_VALUE, Byte.MAX_VALUE);
      final long uncommonValue = TestUtil.nextLong(r, Byte.MIN_VALUE, Byte.MAX_VALUE);
      doTestNormsVersusDocValues(1, new LongSupplier() {
        @Override
        public long getAsLong() {
          return r.nextInt(100) == 0 ? uncommonValue : commonValue;
        }
      });
    }
  }

  public void testSparseOutliers2() throws Exception {
    assumeTrue("Requires sparse norms support", codecSupportsSparsity());
    int iterations = atLeast(1);
    final Random r = random();
    for (int i = 0; i < iterations; i++) {
      final long commonValue = TestUtil.nextLong(r, Byte.MIN_VALUE, Byte.MAX_VALUE);
      final long uncommonValue = TestUtil.nextLong(r, Byte.MIN_VALUE, Byte.MAX_VALUE);
      doTestNormsVersusDocValues(random().nextDouble(), new LongSupplier() {
        @Override
        public long getAsLong() {
          return r.nextInt(100) == 0 ? uncommonValue : commonValue;
        }
      });
    }
  }

  public void testNCommon() throws Exception {
    final Random r = random();
    final int N = TestUtil.nextInt(r, 2, 15);
    final long[] commonValues = new long[N];
    for (int j = 0; j < N; ++j) {
      commonValues[j] = TestUtil.nextLong(r, Byte.MIN_VALUE, Byte.MAX_VALUE);
    }
    final int numOtherValues = TestUtil.nextInt(r, 2, 256 - N);
    final long[] otherValues = new long[numOtherValues];
    for (int j = 0; j < numOtherValues; ++j) {
      otherValues[j] = TestUtil.nextLong(r, Byte.MIN_VALUE, Byte.MAX_VALUE);
    }
    doTestNormsVersusDocValues(1, new LongSupplier() {
      @Override
      public long getAsLong() {
        return r.nextInt(100) == 0 ? otherValues[r.nextInt(numOtherValues - 1)] : commonValues[r.nextInt(N - 1)];
      }
    });
  }

  public void testSparseNCommon() throws Exception {
    assumeTrue("Requires sparse norms support", codecSupportsSparsity());
    final Random r = random();
    final int N = TestUtil.nextInt(r, 2, 15);
    final long[] commonValues = new long[N];
    for (int j = 0; j < N; ++j) {
      commonValues[j] = TestUtil.nextLong(r, Byte.MIN_VALUE, Byte.MAX_VALUE);
    }
    final int numOtherValues = TestUtil.nextInt(r, 2, 256 - N);
    final long[] otherValues = new long[numOtherValues];
    for (int j = 0; j < numOtherValues; ++j) {
      otherValues[j] = TestUtil.nextLong(r, Byte.MIN_VALUE, Byte.MAX_VALUE);
    }
    doTestNormsVersusDocValues(random().nextDouble(), new LongSupplier() {
      @Override
      public long getAsLong() {
        return r.nextInt(100) == 0 ? otherValues[r.nextInt(numOtherValues - 1)] : commonValues[r.nextInt(N - 1)];
      }
    });
  }

  /**
   * a more thorough n-common that tests all low bpv
   */
  @Nightly
  public void testNCommonBig() throws Exception {
    final int iterations = atLeast(1);
    final Random r = random();
    for (int i = 0; i < iterations; ++i) {
      // 16 is 4 bpv, the max before we jump to 8bpv
      for (int n = 2; n < 16; ++n) {
        final int N = n;
        final long[] commonValues = new long[N];
        for (int j = 0; j < N; ++j) {
          commonValues[j] = TestUtil.nextLong(r, Byte.MIN_VALUE, Byte.MAX_VALUE);
        }
        final int numOtherValues = TestUtil.nextInt(r, 2, 256 - N);
        final long[] otherValues = new long[numOtherValues];
        for (int j = 0; j < numOtherValues; ++j) {
          otherValues[j] = TestUtil.nextLong(r, Byte.MIN_VALUE, Byte.MAX_VALUE);
        }
        doTestNormsVersusDocValues(1, new LongSupplier() {
          @Override
          public long getAsLong() {
            return r.nextInt(100) == 0 ? otherValues[r.nextInt(numOtherValues - 1)] : commonValues[r.nextInt(N - 1)];
          }
        });
      }
    }
  }

  /**
   * a more thorough n-common that tests all low bpv and sparse docs
   */
  @Nightly
  public void testSparseNCommonBig() throws Exception {
    assumeTrue("Requires sparse norms support", codecSupportsSparsity());
    final int iterations = atLeast(1);
    final Random r = random();
    for (int i = 0; i < iterations; ++i) {
      // 16 is 4 bpv, the max before we jump to 8bpv
      for (int n = 2; n < 16; ++n) {
        final int N = n;
        final long[] commonValues = new long[N];
        for (int j = 0; j < N; ++j) {
          commonValues[j] = TestUtil.nextLong(r, Byte.MIN_VALUE, Byte.MAX_VALUE);
        }
        final int numOtherValues = TestUtil.nextInt(r, 2, 256 - N);
        final long[] otherValues = new long[numOtherValues];
        for (int j = 0; j < numOtherValues; ++j) {
          otherValues[j] = TestUtil.nextLong(r, Byte.MIN_VALUE, Byte.MAX_VALUE);
        }
        doTestNormsVersusDocValues(random().nextDouble(), new LongSupplier() {
          @Override
          public long getAsLong() {
            return r.nextInt(100) == 0 ? otherValues[r.nextInt(numOtherValues - 1)] : commonValues[r.nextInt(N - 1)];
          }
        });
      }
    }
  }

  private void doTestNormsVersusDocValues(double density, LongSupplier longs) throws Exception {
    int numDocs = atLeast(500);
    final FixedBitSet docsWithField = new FixedBitSet(numDocs);
    final int numDocsWithField = Math.max(1, (int) (density * numDocs));
    if (numDocsWithField == numDocs) {
      docsWithField.set(0, numDocs);
    } else {
      int i = 0;
      while (i < numDocsWithField) {
        int doc = random().nextInt(numDocs);
        if (docsWithField.get(doc) == false) {
          docsWithField.set(doc);
          ++i;
        }
      }
    }
    long norms[] = new long[numDocsWithField];
    for (int i = 0; i < numDocsWithField; i++) {
      norms[i] = longs.getAsLong();
    }
    
    Directory dir = newDirectory();
    Analyzer analyzer = new MockAnalyzer(random(), MockTokenizer.KEYWORD, false);
    IndexWriterConfig conf = newIndexWriterConfig(analyzer);conf.setMergePolicy(NoMergePolicy.INSTANCE);
    conf.setSimilarity(new CannedNormSimilarity(norms));
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, conf);
    Document doc = new Document();
    Field idField = new StringField("id", "", Field.Store.NO);
    Field indexedField = new TextField("indexed", "", Field.Store.NO);
    Field dvField = new NumericDocValuesField("dv", 0);
    doc.add(idField);
    doc.add(indexedField);
    doc.add(dvField);
    
    for (int i = 0, j = 0; i < numDocs; i++) {
      idField.setStringValue(Integer.toString(i));
      if (docsWithField.get(i) == false) {
        Document doc2 = new Document();
        doc2.add(idField);
        writer.addDocument(doc2);
      } else {
        long value = norms[j++];
        dvField.setLongValue(value);
        indexedField.setStringValue(Long.toString(value));
        writer.addDocument(doc);
      }
      if (random().nextInt(31) == 0) {
        writer.commit();
      }
    }
    
    // delete some docs
    int numDeletions = random().nextInt(numDocs/20);
    for (int i = 0; i < numDeletions; i++) {
      int id = random().nextInt(numDocs);
      writer.deleteDocuments(new Term("id", Integer.toString(id)));
    }
    
    writer.commit();
    
    // compare
    DirectoryReader ir = DirectoryReader.open(dir);
    checkNormsVsDocValues(ir);
    ir.close();
    
    writer.forceMerge(1);
    
    // compare again
    ir = DirectoryReader.open(dir);
    checkNormsVsDocValues(ir);
    
    writer.close();
    ir.close();
    dir.close();
  }

  private void checkNormsVsDocValues(IndexReader ir) throws IOException {
    for (LeafReaderContext context : ir.leaves()) {
      LeafReader r = context.reader();
      NumericDocValues expected = r.getNumericDocValues("dv");
      NumericDocValues actual = r.getNormValues("indexed");
      assertEquals(expected == null, actual == null);
      if (expected != null) {
        for (int d = expected.nextDoc(); d != DocIdSetIterator.NO_MORE_DOCS; d = expected.nextDoc()) {
          assertEquals(d, actual.nextDoc());
          assertEquals("doc " + d, expected.longValue(), actual.longValue());
        }
        assertEquals(NO_MORE_DOCS, actual.nextDoc());
      }
    }
  }
  
  static class CannedNormSimilarity extends Similarity {
    final long norms[];
    int index = 0;
    
    CannedNormSimilarity(long norms[]) {
      this.norms = norms;
    }

    @Override
    public long computeNorm(FieldInvertState state) {
      return norms[index++];
    }

    @Override
    public SimWeight computeWeight(float boost, CollectionStatistics collectionStats, TermStatistics... termStats) {
      throw new UnsupportedOperationException();
    }

    @Override
    public SimScorer simScorer(SimWeight weight, LeafReaderContext context) throws IOException {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  protected void addRandomFields(Document doc) {
    // TODO: improve
    doc.add(new TextField("foobar", TestUtil.randomSimpleString(random()), Field.Store.NO));
    
  }

  @Override
  public void testMergeStability() throws Exception {
    // TODO: can we improve this base test to just have subclasses declare the extensions to check,
    // rather than a blacklist to exclude? we need to index stuff to get norms, but we dont care about testing
    // the PFs actually doing that...
    assumeTrue("The MockRandom PF randomizes content on the fly, so we can't check it", false);
  }
  
  // TODO: test thread safety (e.g. across different fields) explicitly here

  /*
   * LUCENE-6006: Tests undead norms.
   *                                 .....            
   *                             C C  /            
   *                            /<   /             
   *             ___ __________/_#__=o             
   *            /(- /(\_\________   \              
   *            \ ) \ )_      \o     \             
   *            /|\ /|\       |'     |             
   *                          |     _|             
   *                          /o   __\             
   *                         / '     |             
   *                        / /      |             
   *                       /_/\______|             
   *                      (   _(    <              
   *                       \    \    \             
   *                        \    \    |            
   *                         \____\____\           
   *                         ____\_\__\_\          
   *                       /`   /`     o\          
   *                       |___ |_______|
   *
   */
  public void testUndeadNorms() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    int numDocs = atLeast(500);
    List<Integer> toDelete = new ArrayList<>();
    for(int i=0;i<numDocs;i++) {
      Document doc = new Document();
      doc.add(new StringField("id", ""+i, Field.Store.NO));
      if (random().nextInt(5) == 1) {
        toDelete.add(i);
        doc.add(new TextField("content", "some content", Field.Store.NO));
      }
      w.addDocument(doc);
    }
    for(Integer id : toDelete) {
      w.deleteDocuments(new Term("id", ""+id));
    }
    w.forceMerge(1);
    IndexReader r = w.getReader();
    assertFalse(r.hasDeletions());

    // Confusingly, norms should exist, and should all be 0, even though we deleted all docs that had the field "content".  They should not
    // be undead:
    NumericDocValues norms = MultiDocValues.getNormValues(r, "content");
    assertNotNull(norms);
    if (codecSupportsSparsity()) {
      assertEquals(DocIdSetIterator.NO_MORE_DOCS, norms.nextDoc());
    } else {
      for(int i=0;i<r.maxDoc();i++) {
        assertEquals(i, norms.nextDoc());
        assertEquals(0, norms.longValue());
      }
    }

    r.close();
    w.close();
    dir.close();
  }

  public void testThreads() throws Exception {
    float density = codecSupportsSparsity() == false || random().nextBoolean() ? 1f : random().nextFloat();
    int numDocs = atLeast(500);
    final FixedBitSet docsWithField = new FixedBitSet(numDocs);
    final int numDocsWithField = Math.max(1, (int) (density * numDocs));
    if (numDocsWithField == numDocs) {
      docsWithField.set(0, numDocs);
    } else {
      int i = 0;
      while (i < numDocsWithField) {
        int doc = random().nextInt(numDocs);
        if (docsWithField.get(doc) == false) {
          docsWithField.set(doc);
          ++i;
        }
      }
    }

    long norms[] = new long[numDocsWithField];
    for (int i = 0; i < numDocsWithField; i++) {
      norms[i] = random().nextLong();
    }

    Directory dir = newDirectory();
    Analyzer analyzer = new MockAnalyzer(random(), MockTokenizer.KEYWORD, false);
    IndexWriterConfig conf = newIndexWriterConfig(analyzer);conf.setMergePolicy(NoMergePolicy.INSTANCE);
    conf.setSimilarity(new CannedNormSimilarity(norms));
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, conf);
    Document doc = new Document();
    Field idField = new StringField("id", "", Field.Store.NO);
    Field indexedField = new TextField("indexed", "", Field.Store.NO);
    Field dvField = new NumericDocValuesField("dv", 0);
    doc.add(idField);
    doc.add(indexedField);
    doc.add(dvField);
    
    for (int i = 0, j = 0; i < numDocs; i++) {
      idField.setStringValue(Integer.toString(i));
      if (docsWithField.get(i) == false) {
        Document doc2 = new Document();
        doc2.add(idField);
        writer.addDocument(doc2);
      } else {
        long value = norms[j++];
        dvField.setLongValue(value);
        indexedField.setStringValue(Long.toString(value));
        writer.addDocument(doc);
      }
      if (random().nextInt(31) == 0) {
        writer.commit();
      }
    }

    DirectoryReader reader = writer.getReader();
    writer.close();

    final int numThreads = TestUtil.nextInt(random(), 3, 30);
    Thread[] threads = new Thread[numThreads];
    final CountDownLatch latch = new CountDownLatch(1);
    for (int i = 0; i < numThreads; ++i) {
      threads[i] = new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            latch.await();
            checkNormsVsDocValues(reader);
            TestUtil.checkReader(reader);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
      });
    }

    for (Thread thread : threads) {
      thread.start();
    }
    latch.countDown();
    for (Thread thread : threads) {
      thread.join();
    }

    reader.close();
    dir.close();
  }
}
