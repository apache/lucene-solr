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
package org.apache.solr.uninverting;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.solr.legacy.LegacyDoubleField;
import org.apache.solr.legacy.LegacyFloatField;
import org.apache.solr.legacy.LegacyIntField;
import org.apache.solr.legacy.LegacyLongField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.SolrTestCase;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.index.SlowCompositeReaderWrapper;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/** random assortment of tests against legacy numerics */
public class TestLegacyFieldCache extends SolrTestCase {
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
    if (null != reader) {
      reader.close();
      reader = null;
    }
    if (null != directory) {
      directory.close();
      directory = null;
    }
  }

  public void test() throws IOException {
    FieldCache cache = FieldCache.DEFAULT;
    NumericDocValues doubles = cache.getNumerics(reader, "theDouble", FieldCache.LEGACY_DOUBLE_PARSER);
    for (int i = 0; i < NUM_DOCS; i++) {
      assertEquals(i, doubles.nextDoc());
      assertEquals(Double.doubleToLongBits(Double.MAX_VALUE - i), doubles.longValue());
    }
    
    NumericDocValues longs = cache.getNumerics(reader, "theLong", FieldCache.LEGACY_LONG_PARSER);
    for (int i = 0; i < NUM_DOCS; i++) {
      assertEquals(i, longs.nextDoc());
      assertEquals(Long.MAX_VALUE - i, longs.longValue());
    }

    NumericDocValues ints = cache.getNumerics(reader, "theInt", FieldCache.LEGACY_INT_PARSER);
    for (int i = 0; i < NUM_DOCS; i++) {
      assertEquals(i, ints.nextDoc());
      assertEquals(Integer.MAX_VALUE - i, ints.longValue());
    }
    
    NumericDocValues floats = cache.getNumerics(reader, "theFloat", FieldCache.LEGACY_FLOAT_PARSER);
    for (int i = 0; i < NUM_DOCS; i++) {
      assertEquals(i, floats.nextDoc());
      assertEquals(Float.floatToIntBits(Float.MAX_VALUE - i), floats.longValue());
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

    FieldCache.DEFAULT.purgeByCacheKey(reader.getCoreCacheHelper().getKey());
  }

  public void testEmptyIndex() throws Exception {
    Directory dir = newDirectory();
    IndexWriter writer= new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())).setMaxBufferedDocs(500));
    writer.close();
    IndexReader r = DirectoryReader.open(dir);
    LeafReader reader = SlowCompositeReaderWrapper.wrap(r);
    TestUtil.checkReader(reader);
    FieldCache.DEFAULT.getTerms(reader, "foobar");
    FieldCache.DEFAULT.getTermsIndex(reader, "foobar");
    FieldCache.DEFAULT.purgeByCacheKey(reader.getCoreCacheHelper().getKey());
    r.close();
    dir.close();
  }

  public void testDocsWithField() throws Exception {
    FieldCache cache = FieldCache.DEFAULT;
    cache.purgeAllCaches();
    assertEquals(0, cache.getCacheEntries().length);
    cache.getNumerics(reader, "theDouble", FieldCache.LEGACY_DOUBLE_PARSER);

    // The double[] takes one slots, and docsWithField should also
    // have been populated:
    assertEquals(2, cache.getCacheEntries().length);
    Bits bits = cache.getDocsWithField(reader, "theDouble", FieldCache.LEGACY_DOUBLE_PARSER);

    // No new entries should appear:
    assertEquals(2, cache.getCacheEntries().length);
    assertTrue(bits instanceof Bits.MatchAllBits);

    NumericDocValues ints = cache.getNumerics(reader, "sparse", FieldCache.LEGACY_INT_PARSER);
    assertEquals(4, cache.getCacheEntries().length);
    for (int i = 0; i < reader.maxDoc(); i++) {
      if (i%2 == 0) {
        assertEquals(i, ints.nextDoc());
        assertEquals(i, ints.longValue());
      }
    }

    NumericDocValues numInts = cache.getNumerics(reader, "numInt", FieldCache.LEGACY_INT_PARSER);
    for (int i = 0; i < reader.maxDoc(); i++) {
      if (i%2 == 0) {
        assertEquals(i, numInts.nextDoc());
        assertEquals(i, numInts.longValue());
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
                  NumericDocValues ints = cache.getNumerics(reader, "sparse", FieldCache.LEGACY_INT_PARSER);
                  for (int i = 0; i < reader.maxDoc(); i++) {
                    if (i%2 == 0) {
                      assertEquals(i, ints.nextDoc());
                      assertEquals(i, ints.longValue());
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
      FieldCache.DEFAULT.getNumerics(ar, "binary", FieldCache.LEGACY_INT_PARSER);
    });
    
    // Sorted type: can be retrieved via getTerms(), getTermsIndex(), getDocTermOrds()
    expectThrows(IllegalStateException.class, () -> {
      FieldCache.DEFAULT.getNumerics(ar, "sorted", FieldCache.LEGACY_INT_PARSER);
    });
    
    // Numeric type: can be retrieved via getInts() and so on
    NumericDocValues numeric = FieldCache.DEFAULT.getNumerics(ar, "numeric", FieldCache.LEGACY_INT_PARSER);
    assertEquals(0, numeric.nextDoc());
    assertEquals(42, numeric.longValue());
       
    // SortedSet type: can be retrieved via getDocTermOrds() 
    expectThrows(IllegalStateException.class, () -> {
      FieldCache.DEFAULT.getNumerics(ar, "sortedset", FieldCache.LEGACY_INT_PARSER);
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
    
    NumericDocValues ints = cache.getNumerics(ar, "bogusints", FieldCache.LEGACY_INT_PARSER);
    assertEquals(NO_MORE_DOCS, ints.nextDoc());
    
    NumericDocValues longs = cache.getNumerics(ar, "boguslongs", FieldCache.LEGACY_LONG_PARSER);
    assertEquals(NO_MORE_DOCS, longs.nextDoc());
    
    NumericDocValues floats = cache.getNumerics(ar, "bogusfloats", FieldCache.LEGACY_FLOAT_PARSER);
    assertEquals(NO_MORE_DOCS, floats.nextDoc());
    
    NumericDocValues doubles = cache.getNumerics(ar, "bogusdoubles", FieldCache.LEGACY_DOUBLE_PARSER);
    assertEquals(NO_MORE_DOCS, doubles.nextDoc());
    
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
    
    NumericDocValues ints = cache.getNumerics(ar, "bogusints", FieldCache.LEGACY_INT_PARSER);
    assertEquals(NO_MORE_DOCS, ints.nextDoc());
    
    NumericDocValues longs = cache.getNumerics(ar, "boguslongs", FieldCache.LEGACY_LONG_PARSER);
    assertEquals(NO_MORE_DOCS, longs.nextDoc());
    
    NumericDocValues floats = cache.getNumerics(ar, "bogusfloats", FieldCache.LEGACY_FLOAT_PARSER);
    assertEquals(NO_MORE_DOCS, floats.nextDoc());
    
    NumericDocValues doubles = cache.getNumerics(ar, "bogusdoubles", FieldCache.LEGACY_DOUBLE_PARSER);
    assertEquals(NO_MORE_DOCS, doubles.nextDoc());
    
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
    Set<Integer> missing = new HashSet<>();
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
        missing.add(i);
      } else {
        field.setLongValue(v);
        iw.addDocument(doc);
      }
    }
    iw.forceMerge(1);
    final DirectoryReader reader = iw.getReader();
    final NumericDocValues longs = FieldCache.DEFAULT.getNumerics(getOnlyLeafReader(reader), "f", FieldCache.LEGACY_LONG_PARSER);
    for (int i = 0; i < values.length; ++i) {
      if (missing.contains(i) == false) {
        assertEquals(i, longs.nextDoc());
        assertEquals(values[i], longs.longValue());
      }
    }
    assertEquals(NO_MORE_DOCS, longs.nextDoc());
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
    Set<Integer> missing = new HashSet<>();
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
        missing.add(i);
      } else {
        field.setIntValue(v);
        iw.addDocument(doc);
      }
    }
    iw.forceMerge(1);
    final DirectoryReader reader = iw.getReader();
    final NumericDocValues ints = FieldCache.DEFAULT.getNumerics(getOnlyLeafReader(reader), "f", FieldCache.LEGACY_INT_PARSER);
    for (int i = 0; i < values.length; ++i) {
      if (missing.contains(i) == false) {
        assertEquals(i, ints.nextDoc());
        assertEquals(values[i], ints.longValue());
      }
    }
    assertEquals(NO_MORE_DOCS, ints.nextDoc());
    reader.close();
    iw.close();
    dir.close();
  }

}
