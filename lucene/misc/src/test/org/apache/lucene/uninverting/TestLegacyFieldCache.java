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
package org.apache.lucene.uninverting;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.LegacyDoubleField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.LegacyFloatField;
import org.apache.lucene.document.LegacyIntField;
import org.apache.lucene.document.LegacyLongField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.SlowCompositeReaderWrapper;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LegacyNumericUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/** random assortment of tests against legacy numerics */
public class TestLegacyFieldCache extends LuceneTestCase {
  private static LeafReader reader;
  private static int NUM_DOCS;
  private static Directory directory;

  @BeforeClass
  public static void beforeClass() throws Exception {
    NUM_DOCS = atLeast(500);
    directory = newDirectory();
    RandomIndexWriter writer= new RandomIndexWriter(random(), directory, newIndexWriterConfig(new MockAnalyzer(random())).setMergePolicy(newLogMergePolicy()));
    long theLong = Long.MAX_VALUE;
    double theDouble = Double.MAX_VALUE;
    int theInt = Integer.MAX_VALUE;
    float theFloat = Float.MAX_VALUE;
    if (VERBOSE) {
      System.out.println("TEST: setUp");
    }
    for (int i = 0; i < NUM_DOCS; i++){
      Document doc = new Document();
      doc.add(new LegacyLongField("theLong", theLong--, Field.Store.NO));
      doc.add(new LegacyDoubleField("theDouble", theDouble--, Field.Store.NO));
      doc.add(new LegacyIntField("theInt", theInt--, Field.Store.NO));
      doc.add(new LegacyFloatField("theFloat", theFloat--, Field.Store.NO));
      if (i%2 == 0) {
        doc.add(new LegacyIntField("sparse", i, Field.Store.NO));
      }

      if (i%2 == 0) {
        doc.add(new LegacyIntField("numInt", i, Field.Store.NO));
      }
      writer.addDocument(doc);
    }
    IndexReader r = writer.getReader();
    reader = SlowCompositeReaderWrapper.wrap(r);
    TestUtil.checkReader(reader);
    writer.close();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    reader.close();
    reader = null;
    directory.close();
    directory = null;
  }
  
  public void testInfoStream() throws Exception {
    try {
      FieldCache cache = FieldCache.DEFAULT;
      ByteArrayOutputStream bos = new ByteArrayOutputStream(1024);
      cache.setInfoStream(new PrintStream(bos, false, IOUtils.UTF_8));
      cache.getNumerics(reader, "theDouble", FieldCache.LEGACY_DOUBLE_PARSER, false);
      cache.getNumerics(reader, "theDouble", new FieldCache.Parser() {
        @Override
        public TermsEnum termsEnum(Terms terms) throws IOException {
          return LegacyNumericUtils.filterPrefixCodedLongs(terms.iterator());
        }
        @Override
        public long parseValue(BytesRef term) {
          int val = (int) LegacyNumericUtils.prefixCodedToLong(term);
          if (val<0) val ^= 0x7fffffff;
          return val;
        }
      }, false);
      assertTrue(bos.toString(IOUtils.UTF_8).indexOf("WARNING") != -1);
    } finally {
      FieldCache.DEFAULT.setInfoStream(null);
      FieldCache.DEFAULT.purgeAllCaches();
    }
  }

  public void test() throws IOException {
    FieldCache cache = FieldCache.DEFAULT;
    NumericDocValues doubles = cache.getNumerics(reader, "theDouble", FieldCache.LEGACY_DOUBLE_PARSER, random().nextBoolean());
    assertSame("Second request to cache return same array", doubles, cache.getNumerics(reader, "theDouble", FieldCache.LEGACY_DOUBLE_PARSER, random().nextBoolean()));
    for (int i = 0; i < NUM_DOCS; i++) {
      assertEquals(Double.doubleToLongBits(Double.MAX_VALUE - i), doubles.get(i));
    }
    
    NumericDocValues longs = cache.getNumerics(reader, "theLong", FieldCache.LEGACY_LONG_PARSER, random().nextBoolean());
    assertSame("Second request to cache return same array", longs, cache.getNumerics(reader, "theLong", FieldCache.LEGACY_LONG_PARSER, random().nextBoolean()));
    for (int i = 0; i < NUM_DOCS; i++) {
      assertEquals(Long.MAX_VALUE - i, longs.get(i));
    }

    NumericDocValues ints = cache.getNumerics(reader, "theInt", FieldCache.LEGACY_INT_PARSER, random().nextBoolean());
    assertSame("Second request to cache return same array", ints, cache.getNumerics(reader, "theInt", FieldCache.LEGACY_INT_PARSER, random().nextBoolean()));
    for (int i = 0; i < NUM_DOCS; i++) {
      assertEquals(Integer.MAX_VALUE - i, ints.get(i));
    }
    
    NumericDocValues floats = cache.getNumerics(reader, "theFloat", FieldCache.LEGACY_FLOAT_PARSER, random().nextBoolean());
    assertSame("Second request to cache return same array", floats, cache.getNumerics(reader, "theFloat", FieldCache.LEGACY_FLOAT_PARSER, random().nextBoolean()));
    for (int i = 0; i < NUM_DOCS; i++) {
      assertEquals(Float.floatToIntBits(Float.MAX_VALUE - i), floats.get(i));
    }

    Bits docsWithField = cache.getDocsWithField(reader, "theLong", null);
    assertSame("Second request to cache return same array", docsWithField, cache.getDocsWithField(reader, "theLong", null));
    assertTrue("docsWithField(theLong) must be class Bits.MatchAllBits", docsWithField instanceof Bits.MatchAllBits);
    assertTrue("docsWithField(theLong) Size: " + docsWithField.length() + " is not: " + NUM_DOCS, docsWithField.length() == NUM_DOCS);
    for (int i = 0; i < docsWithField.length(); i++) {
      assertTrue(docsWithField.get(i));
    }
    
    docsWithField = cache.getDocsWithField(reader, "sparse", null);
    assertSame("Second request to cache return same array", docsWithField, cache.getDocsWithField(reader, "sparse", null));
    assertFalse("docsWithField(sparse) must not be class Bits.MatchAllBits", docsWithField instanceof Bits.MatchAllBits);
    assertTrue("docsWithField(sparse) Size: " + docsWithField.length() + " is not: " + NUM_DOCS, docsWithField.length() == NUM_DOCS);
    for (int i = 0; i < docsWithField.length(); i++) {
      assertEquals(i%2 == 0, docsWithField.get(i));
    }

    FieldCache.DEFAULT.purgeByCacheKey(reader.getCoreCacheKey());
  }

  public void testEmptyIndex() throws Exception {
    Directory dir = newDirectory();
    IndexWriter writer= new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())).setMaxBufferedDocs(500));
    writer.close();
    IndexReader r = DirectoryReader.open(dir);
    LeafReader reader = SlowCompositeReaderWrapper.wrap(r);
    TestUtil.checkReader(reader);
    FieldCache.DEFAULT.getTerms(reader, "foobar", true);
    FieldCache.DEFAULT.getTermsIndex(reader, "foobar");
    FieldCache.DEFAULT.purgeByCacheKey(reader.getCoreCacheKey());
    r.close();
    dir.close();
  }

  public void testDocsWithField() throws Exception {
    FieldCache cache = FieldCache.DEFAULT;
    cache.purgeAllCaches();
    assertEquals(0, cache.getCacheEntries().length);
    cache.getNumerics(reader, "theDouble", FieldCache.LEGACY_DOUBLE_PARSER, true);

    // The double[] takes one slots, and docsWithField should also
    // have been populated:
    assertEquals(2, cache.getCacheEntries().length);
    Bits bits = cache.getDocsWithField(reader, "theDouble", FieldCache.LEGACY_DOUBLE_PARSER);

    // No new entries should appear:
    assertEquals(2, cache.getCacheEntries().length);
    assertTrue(bits instanceof Bits.MatchAllBits);

    NumericDocValues ints = cache.getNumerics(reader, "sparse", FieldCache.LEGACY_INT_PARSER, true);
    assertEquals(4, cache.getCacheEntries().length);
    Bits docsWithField = cache.getDocsWithField(reader, "sparse", FieldCache.LEGACY_INT_PARSER);
    assertEquals(4, cache.getCacheEntries().length);
    for (int i = 0; i < docsWithField.length(); i++) {
      if (i%2 == 0) {
        assertTrue(docsWithField.get(i));
        assertEquals(i, ints.get(i));
      } else {
        assertFalse(docsWithField.get(i));
      }
    }

    NumericDocValues numInts = cache.getNumerics(reader, "numInt", FieldCache.LEGACY_INT_PARSER, random().nextBoolean());
    docsWithField = cache.getDocsWithField(reader, "numInt", FieldCache.LEGACY_INT_PARSER);
    for (int i = 0; i < docsWithField.length(); i++) {
      if (i%2 == 0) {
        assertTrue(docsWithField.get(i));
        assertEquals(i, numInts.get(i));
      } else {
        assertFalse(docsWithField.get(i));
      }
    }
  }
  
  public void testGetDocsWithFieldThreadSafety() throws Exception {
    final FieldCache cache = FieldCache.DEFAULT;
    cache.purgeAllCaches();

    int NUM_THREADS = 3;
    Thread[] threads = new Thread[NUM_THREADS];
    final AtomicBoolean failed = new AtomicBoolean();
    final AtomicInteger iters = new AtomicInteger();
    final int NUM_ITER = 200 * RANDOM_MULTIPLIER;
    final CyclicBarrier restart = new CyclicBarrier(NUM_THREADS,
                                                    new Runnable() {
                                                      @Override
                                                      public void run() {
                                                        cache.purgeAllCaches();
                                                        iters.incrementAndGet();
                                                      }
                                                    });
    for(int threadIDX=0;threadIDX<NUM_THREADS;threadIDX++) {
      threads[threadIDX] = new Thread() {
          @Override
          public void run() {

            try {
              while(!failed.get()) {
                final int op = random().nextInt(3);
                if (op == 0) {
                  // Purge all caches & resume, once all
                  // threads get here:
                  restart.await();
                  if (iters.get() >= NUM_ITER) {
                    break;
                  }
                } else if (op == 1) {
                  Bits docsWithField = cache.getDocsWithField(reader, "sparse", null);
                  for (int i = 0; i < docsWithField.length(); i++) {
                    assertEquals(i%2 == 0, docsWithField.get(i));
                  }
                } else {
                  NumericDocValues ints = cache.getNumerics(reader, "sparse", FieldCache.LEGACY_INT_PARSER, true);
                  Bits docsWithField = cache.getDocsWithField(reader, "sparse", null);
                  for (int i = 0; i < docsWithField.length(); i++) {
                    if (i%2 == 0) {
                      assertTrue(docsWithField.get(i));
                      assertEquals(i, ints.get(i));
                    } else {
                      assertFalse(docsWithField.get(i));
                    }
                  }
                }
              }
            } catch (Throwable t) {
              failed.set(true);
              restart.reset();
              throw new RuntimeException(t);
            }
          }
        };
      threads[threadIDX].start();
    }

    for(int threadIDX=0;threadIDX<NUM_THREADS;threadIDX++) {
      threads[threadIDX].join();
    }
    assertFalse(failed.get());
  }
  
  public void testDocValuesIntegration() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(null);
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);
    Document doc = new Document();
    doc.add(new BinaryDocValuesField("binary", new BytesRef("binary value")));
    doc.add(new SortedDocValuesField("sorted", new BytesRef("sorted value")));
    doc.add(new NumericDocValuesField("numeric", 42));
    doc.add(new SortedSetDocValuesField("sortedset", new BytesRef("sortedset value1")));
    doc.add(new SortedSetDocValuesField("sortedset", new BytesRef("sortedset value2")));
    iw.addDocument(doc);
    DirectoryReader ir = iw.getReader();
    iw.close();
    LeafReader ar = getOnlyLeafReader(ir);
    
    // Binary type: can be retrieved via getTerms()
    expectThrows(IllegalStateException.class, () -> {
      FieldCache.DEFAULT.getNumerics(ar, "binary", FieldCache.LEGACY_INT_PARSER, false);
    });
    
    // Sorted type: can be retrieved via getTerms(), getTermsIndex(), getDocTermOrds()
    expectThrows(IllegalStateException.class, () -> {
      FieldCache.DEFAULT.getNumerics(ar, "sorted", FieldCache.LEGACY_INT_PARSER, false);
    });
    
    // Numeric type: can be retrieved via getInts() and so on
    NumericDocValues numeric = FieldCache.DEFAULT.getNumerics(ar, "numeric", FieldCache.LEGACY_INT_PARSER, false);
    assertEquals(42, numeric.get(0));
       
    // SortedSet type: can be retrieved via getDocTermOrds() 
    expectThrows(IllegalStateException.class, () -> {
      FieldCache.DEFAULT.getNumerics(ar, "sortedset", FieldCache.LEGACY_INT_PARSER, false);
    });
    
    ir.close();
    dir.close();
  }
  
  public void testNonexistantFields() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    iw.addDocument(doc);
    DirectoryReader ir = iw.getReader();
    iw.close();
    
    LeafReader ar = getOnlyLeafReader(ir);
    
    final FieldCache cache = FieldCache.DEFAULT;
    cache.purgeAllCaches();
    assertEquals(0, cache.getCacheEntries().length);
    
    NumericDocValues ints = cache.getNumerics(ar, "bogusints", FieldCache.LEGACY_INT_PARSER, true);
    assertEquals(0, ints.get(0));
    
    NumericDocValues longs = cache.getNumerics(ar, "boguslongs", FieldCache.LEGACY_LONG_PARSER, true);
    assertEquals(0, longs.get(0));
    
    NumericDocValues floats = cache.getNumerics(ar, "bogusfloats", FieldCache.LEGACY_FLOAT_PARSER, true);
    assertEquals(0, floats.get(0));
    
    NumericDocValues doubles = cache.getNumerics(ar, "bogusdoubles", FieldCache.LEGACY_DOUBLE_PARSER, true);
    assertEquals(0, doubles.get(0));
    
    // check that we cached nothing
    assertEquals(0, cache.getCacheEntries().length);
    ir.close();
    dir.close();
  }
  
  public void testNonIndexedFields() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(new StoredField("bogusbytes", "bogus"));
    doc.add(new StoredField("bogusshorts", "bogus"));
    doc.add(new StoredField("bogusints", "bogus"));
    doc.add(new StoredField("boguslongs", "bogus"));
    doc.add(new StoredField("bogusfloats", "bogus"));
    doc.add(new StoredField("bogusdoubles", "bogus"));
    doc.add(new StoredField("bogusbits", "bogus"));
    iw.addDocument(doc);
    DirectoryReader ir = iw.getReader();
    iw.close();
    
    LeafReader ar = getOnlyLeafReader(ir);
    
    final FieldCache cache = FieldCache.DEFAULT;
    cache.purgeAllCaches();
    assertEquals(0, cache.getCacheEntries().length);
    
    NumericDocValues ints = cache.getNumerics(ar, "bogusints", FieldCache.LEGACY_INT_PARSER, true);
    assertEquals(0, ints.get(0));
    
    NumericDocValues longs = cache.getNumerics(ar, "boguslongs", FieldCache.LEGACY_LONG_PARSER, true);
    assertEquals(0, longs.get(0));
    
    NumericDocValues floats = cache.getNumerics(ar, "bogusfloats", FieldCache.LEGACY_FLOAT_PARSER, true);
    assertEquals(0, floats.get(0));
    
    NumericDocValues doubles = cache.getNumerics(ar, "bogusdoubles", FieldCache.LEGACY_DOUBLE_PARSER, true);
    assertEquals(0, doubles.get(0));
    
    // check that we cached nothing
    assertEquals(0, cache.getCacheEntries().length);
    ir.close();
    dir.close();
  }

  // Make sure that the use of GrowableWriter doesn't prevent from using the full long range
  public void testLongFieldCache() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig cfg = newIndexWriterConfig(new MockAnalyzer(random()));
    cfg.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, cfg);
    Document doc = new Document();
    LegacyLongField field = new LegacyLongField("f", 0L, Store.YES);
    doc.add(field);
    final long[] values = new long[TestUtil.nextInt(random(), 1, 10)];
    for (int i = 0; i < values.length; ++i) {
      final long v;
      switch (random().nextInt(10)) {
        case 0:
          v = Long.MIN_VALUE;
          break;
        case 1:
          v = 0;
          break;
        case 2:
          v = Long.MAX_VALUE;
          break;
        default:
          v = TestUtil.nextLong(random(), -10, 10);
          break;
      }
      values[i] = v;
      if (v == 0 && random().nextBoolean()) {
        // missing
        iw.addDocument(new Document());
      } else {
        field.setLongValue(v);
        iw.addDocument(doc);
      }
    }
    iw.forceMerge(1);
    final DirectoryReader reader = iw.getReader();
    final NumericDocValues longs = FieldCache.DEFAULT.getNumerics(getOnlyLeafReader(reader), "f", FieldCache.LEGACY_LONG_PARSER, false);
    for (int i = 0; i < values.length; ++i) {
      assertEquals(values[i], longs.get(i));
    }
    reader.close();
    iw.close();
    dir.close();
  }

  // Make sure that the use of GrowableWriter doesn't prevent from using the full int range
  public void testIntFieldCache() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig cfg = newIndexWriterConfig(new MockAnalyzer(random()));
    cfg.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, cfg);
    Document doc = new Document();
    LegacyIntField field = new LegacyIntField("f", 0, Store.YES);
    doc.add(field);
    final int[] values = new int[TestUtil.nextInt(random(), 1, 10)];
    for (int i = 0; i < values.length; ++i) {
      final int v;
      switch (random().nextInt(10)) {
        case 0:
          v = Integer.MIN_VALUE;
          break;
        case 1:
          v = 0;
          break;
        case 2:
          v = Integer.MAX_VALUE;
          break;
        default:
          v = TestUtil.nextInt(random(), -10, 10);
          break;
      }
      values[i] = v;
      if (v == 0 && random().nextBoolean()) {
        // missing
        iw.addDocument(new Document());
      } else {
        field.setIntValue(v);
        iw.addDocument(doc);
      }
    }
    iw.forceMerge(1);
    final DirectoryReader reader = iw.getReader();
    final NumericDocValues ints = FieldCache.DEFAULT.getNumerics(getOnlyLeafReader(reader), "f", FieldCache.LEGACY_INT_PARSER, false);
    for (int i = 0; i < values.length; ++i) {
      assertEquals(values[i], ints.get(i));
    }
    reader.close();
    iw.close();
    dir.close();
  }

}
