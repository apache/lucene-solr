package org.apache.lucene.index;

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
import java.util.Random;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
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
      doTestNormsVersusStoredFields(new LongProducer() {
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
      doTestNormsVersusStoredFields(new LongProducer() {
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
      doTestNormsVersusStoredFields(new LongProducer() {
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
      doTestNormsVersusStoredFields(new LongProducer() {
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
      doTestNormsVersusStoredFields(new LongProducer() {
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
      doTestNormsVersusStoredFields(new LongProducer() {
        @Override
        long next() {
          return r.nextBoolean() ? 1000000L : -5000;
        }
      });
    }
  }
  
  public void testAllZeros() throws Exception {
    int iterations = atLeast(1);
    final Random r = random();
    for (int i = 0; i < iterations; i++) {
      doTestNormsVersusStoredFields(new LongProducer() {
        @Override
        long next() {
          return 0;
        }
      });
    }
  }
  
  private void doTestNormsVersusStoredFields(LongProducer longs) throws Exception {
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
    Field storedField = newTextField("stored", "", Field.Store.YES);
    doc.add(idField);
    doc.add(storedField);
    
    for (int i = 0; i < numDocs; i++) {
      idField.setStringValue(Integer.toString(i));
      long value = norms[i];
      storedField.setStringValue(Long.toString(value));
      writer.addDocument(doc);
      if (random().nextInt(31) == 0) {
        writer.commit();
      }
    }
    
    // delete some docs
    int numDeletions = random().nextInt(numDocs/10);
    for (int i = 0; i < numDeletions; i++) {
      int id = random().nextInt(numDocs);
      writer.deleteDocuments(new Term("id", Integer.toString(id)));
    }
    
    writer.commit();
    
    // compare
    DirectoryReader ir = DirectoryReader.open(dir);
    for (AtomicReaderContext context : ir.leaves()) {
      AtomicReader r = context.reader();
      NumericDocValues docValues = r.getNormValues("stored");
      for (int i = 0; i < r.maxDoc(); i++) {
        long storedValue = Long.parseLong(r.document(i).get("stored"));
        assertEquals(storedValue, docValues.get(i));
      }
    }
    ir.close();
    
    writer.forceMerge(1);
    
    // compare again
    ir = DirectoryReader.open(dir);
    for (AtomicReaderContext context : ir.leaves()) {
      AtomicReader r = context.reader();
      NumericDocValues docValues = r.getNormValues("stored");
      for (int i = 0; i < r.maxDoc(); i++) {
        long storedValue = Long.parseLong(r.document(i).get("stored"));
        assertEquals(storedValue, docValues.get(i));
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
    public SimWeight computeWeight(float queryBoost, CollectionStatistics collectionStats, TermStatistics... termStats) {
      throw new UnsupportedOperationException();
    }

    @Override
    public SimScorer simScorer(SimWeight weight, AtomicReaderContext context) throws IOException {
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
}
