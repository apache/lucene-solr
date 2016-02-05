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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.TestUtil;

/**
 * Abstract class to do basic tests for a norms format.
 * NOTE: This test focuses on the norms impl, nothing else.
 * The [stretch] goal is for this test to be
 * so thorough in testing a new NormsFormat that if this
 * test passes, then all Lucene/Solr tests should also pass.  Ie,
 * if there is some bug in a given NormsFormat that this
 * test fails to catch then this test needs to be improved! */
public abstract class BaseNormsFormatTestCase extends BaseIndexFileFormatTestCase {
  
  public void testByteRange() throws Exception {
    int iterations = atLeast(1);
    final Random r = random();
    for (int i = 0; i < iterations; i++) {
      doTestNormsVersusDocValues(new LongProducer() {
        @Override
        long next() {
          return TestUtil.nextLong(r, Byte.MIN_VALUE, Byte.MAX_VALUE);
        }
      });
    }
  }
  
  public void testShortRange() throws Exception {
    int iterations = atLeast(1);
    final Random r = random();
    for (int i = 0; i < iterations; i++) {
      doTestNormsVersusDocValues(new LongProducer() {
        @Override
        long next() {
          return TestUtil.nextLong(r, Short.MIN_VALUE, Short.MAX_VALUE);
        }
      });
    }
  }
  
  public void testLongRange() throws Exception {
    int iterations = atLeast(1);
    final Random r = random();
    for (int i = 0; i < iterations; i++) {
      doTestNormsVersusDocValues(new LongProducer() {
        @Override
        long next() {
          return TestUtil.nextLong(r, Long.MIN_VALUE, Long.MAX_VALUE);
        }
      });
    }
  }
  
  public void testFullLongRange() throws Exception {
    int iterations = atLeast(1);
    final Random r = random();
    for (int i = 0; i < iterations; i++) {
      doTestNormsVersusDocValues(new LongProducer() {
        @Override
        long next() {
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
      doTestNormsVersusDocValues(new LongProducer() {
        @Override
        long next() {
          return r.nextBoolean() ? 20 : 3;
        }
      });
    }
  }
  
  public void testFewLargeValues() throws Exception {
    int iterations = atLeast(1);
    final Random r = random();
    for (int i = 0; i < iterations; i++) {
      doTestNormsVersusDocValues(new LongProducer() {
        @Override
        long next() {
          return r.nextBoolean() ? 1000000L : -5000;
        }
      });
    }
  }
  
  public void testAllZeros() throws Exception {
    int iterations = atLeast(1);
    for (int i = 0; i < iterations; i++) {
      doTestNormsVersusDocValues(new LongProducer() {
        @Override
        long next() {
          return 0;
        }
      });
    }
  }
  
  public void testSparse() throws Exception {
    int iterations = atLeast(1);
    final Random r = random();
    for (int i = 0; i < iterations; i++) {
      doTestNormsVersusDocValues(new LongProducer() {
        @Override
        long next() {
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
      doTestNormsVersusDocValues(new LongProducer() {
        @Override
        long next() {
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
      doTestNormsVersusDocValues(new LongProducer() {
        @Override
        long next() {
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
    doTestNormsVersusDocValues(new LongProducer() {
      @Override
      long next() {
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
        doTestNormsVersusDocValues(new LongProducer() {
          @Override
          long next() {
            return r.nextInt(100) == 0 ? otherValues[r.nextInt(numOtherValues - 1)] : commonValues[r.nextInt(N - 1)];
          }
        });
      }
    }
  }
  
  private void doTestNormsVersusDocValues(LongProducer longs) throws Exception {
    int numDocs = atLeast(500);
    long norms[] = new long[numDocs];
    for (int i = 0; i < numDocs; i++) {
      norms[i] = longs.next();
    }
    
    Directory dir = newDirectory();
    Analyzer analyzer = new MockAnalyzer(random(), MockTokenizer.KEYWORD, false);
    IndexWriterConfig conf = newIndexWriterConfig(analyzer);
    conf.setSimilarity(new CannedNormSimilarity(norms));
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, conf);
    Document doc = new Document();
    Field idField = new StringField("id", "", Field.Store.NO);
    Field indexedField = new TextField("indexed", "", Field.Store.NO);
    Field dvField = new NumericDocValuesField("dv", 0);
    doc.add(idField);
    doc.add(indexedField);
    doc.add(dvField);
    
    for (int i = 0; i < numDocs; i++) {
      idField.setStringValue(Integer.toString(i));
      long value = norms[i];
      dvField.setLongValue(value);
      indexedField.setStringValue(Long.toString(value));
      writer.addDocument(doc);
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
    for (LeafReaderContext context : ir.leaves()) {
      LeafReader r = context.reader();
      NumericDocValues expected = r.getNumericDocValues("dv");
      NumericDocValues actual = r.getNormValues("indexed");
      for (int i = 0; i < r.maxDoc(); i++) {
        assertEquals("doc " + i, expected.get(i), actual.get(i));
      }
    }
    ir.close();
    
    writer.forceMerge(1);
    
    // compare again
    ir = DirectoryReader.open(dir);
    for (LeafReaderContext context : ir.leaves()) {
      LeafReader r = context.reader();
      NumericDocValues expected = r.getNumericDocValues("dv");
      NumericDocValues actual = r.getNormValues("indexed");
      for (int i = 0; i < r.maxDoc(); i++) {
        assertEquals("doc " + i, expected.get(i), actual.get(i));
      }
    }
    
    writer.close();
    ir.close();
    dir.close();
  }
  
  
  static abstract class LongProducer {
    abstract long next();
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
    public SimWeight computeWeight(CollectionStatistics collectionStats, TermStatistics... termStats) {
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
    for(int i=0;i<r.maxDoc();i++) {
      assertEquals(0, norms.get(i));
    }

    r.close();
    w.close();
    dir.close();
  }
}
