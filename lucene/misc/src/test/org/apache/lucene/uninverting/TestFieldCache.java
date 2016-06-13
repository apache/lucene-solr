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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.SlowCompositeReaderWrapper;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class TestFieldCache extends LuceneTestCase {
  private static LeafReader reader;
  private static int NUM_DOCS;
  private static int NUM_ORDS;
  private static String[] unicodeStrings;
  private static BytesRef[][] multiValued;
  private static Directory directory;

  @BeforeClass
  public static void beforeClass() throws Exception {
    NUM_DOCS = atLeast(500);
    NUM_ORDS = atLeast(2);
    directory = newDirectory();
    IndexWriter writer= new IndexWriter(directory, new IndexWriterConfig(new MockAnalyzer(random())).setMergePolicy(new LogDocMergePolicy()));
    long theLong = Long.MAX_VALUE;
    double theDouble = Double.MAX_VALUE;
    int theInt = Integer.MAX_VALUE;
    float theFloat = Float.MAX_VALUE;
    unicodeStrings = new String[NUM_DOCS];
    multiValued = new BytesRef[NUM_DOCS][NUM_ORDS];
    if (VERBOSE) {
      System.out.println("TEST: setUp");
    }
    for (int i = 0; i < NUM_DOCS; i++){
      Document doc = new Document();
      doc.add(new LongPoint("theLong", theLong--));
      doc.add(new DoublePoint("theDouble", theDouble--));
      doc.add(new IntPoint("theInt", theInt--));
      doc.add(new FloatPoint("theFloat", theFloat--));
      if (i%2 == 0) {
        doc.add(new IntPoint("sparse", i));
      }

      if (i%2 == 0) {
        doc.add(new IntPoint("numInt", i));
      }

      // sometimes skip the field:
      if (random().nextInt(40) != 17) {
        unicodeStrings[i] = generateString(i);
        doc.add(newStringField("theRandomUnicodeString", unicodeStrings[i], Field.Store.YES));
      }

      // sometimes skip the field:
      if (random().nextInt(10) != 8) {
        for (int j = 0; j < NUM_ORDS; j++) {
          String newValue = generateString(i);
          multiValued[i][j] = new BytesRef(newValue);
          doc.add(newStringField("theRandomUnicodeMultiValuedField", newValue, Field.Store.YES));
        }
        Arrays.sort(multiValued[i]);
      }
      writer.addDocument(doc);
    }
    writer.forceMerge(1); // this test relies on one segment and docid order
    IndexReader r = DirectoryReader.open(writer);
    assertEquals(1, r.leaves().size());
    reader = r.leaves().get(0).reader();
    TestUtil.checkReader(reader);
    writer.close();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    reader.close();
    reader = null;
    directory.close();
    directory = null;
    unicodeStrings = null;
    multiValued = null;
  }
  
  public void test() throws IOException {
    FieldCache cache = FieldCache.DEFAULT;
    NumericDocValues doubles = cache.getNumerics(reader, "theDouble", FieldCache.DOUBLE_POINT_PARSER, random().nextBoolean());
    assertSame("Second request to cache return same array", doubles, cache.getNumerics(reader, "theDouble", FieldCache.DOUBLE_POINT_PARSER, random().nextBoolean()));
    for (int i = 0; i < NUM_DOCS; i++) {
      assertEquals(Double.doubleToLongBits(Double.MAX_VALUE - i), doubles.get(i));
    }
    
    NumericDocValues longs = cache.getNumerics(reader, "theLong", FieldCache.LONG_POINT_PARSER, random().nextBoolean());
    assertSame("Second request to cache return same array", longs, cache.getNumerics(reader, "theLong", FieldCache.LONG_POINT_PARSER, random().nextBoolean()));
    for (int i = 0; i < NUM_DOCS; i++) {
      assertEquals(Long.MAX_VALUE - i, longs.get(i));
    }

    NumericDocValues ints = cache.getNumerics(reader, "theInt", FieldCache.INT_POINT_PARSER, random().nextBoolean());
    assertSame("Second request to cache return same array", ints, cache.getNumerics(reader, "theInt", FieldCache.INT_POINT_PARSER, random().nextBoolean()));
    for (int i = 0; i < NUM_DOCS; i++) {
      assertEquals(Integer.MAX_VALUE - i, ints.get(i));
    }
    
    NumericDocValues floats = cache.getNumerics(reader, "theFloat", FieldCache.FLOAT_POINT_PARSER, random().nextBoolean());
    assertSame("Second request to cache return same array", floats, cache.getNumerics(reader, "theFloat", FieldCache.FLOAT_POINT_PARSER, random().nextBoolean()));
    for (int i = 0; i < NUM_DOCS; i++) {
      assertEquals(Float.floatToIntBits(Float.MAX_VALUE - i), floats.get(i));
    }

    Bits docsWithField = cache.getDocsWithField(reader, "theLong", FieldCache.LONG_POINT_PARSER);
    assertSame("Second request to cache return same array", docsWithField, cache.getDocsWithField(reader, "theLong", FieldCache.LONG_POINT_PARSER));
    assertTrue("docsWithField(theLong) must be class Bits.MatchAllBits", docsWithField instanceof Bits.MatchAllBits);
    assertTrue("docsWithField(theLong) Size: " + docsWithField.length() + " is not: " + NUM_DOCS, docsWithField.length() == NUM_DOCS);
    for (int i = 0; i < docsWithField.length(); i++) {
      assertTrue(docsWithField.get(i));
    }
    
    docsWithField = cache.getDocsWithField(reader, "sparse", FieldCache.INT_POINT_PARSER);
    assertSame("Second request to cache return same array", docsWithField, cache.getDocsWithField(reader, "sparse", FieldCache.INT_POINT_PARSER));
    assertFalse("docsWithField(sparse) must not be class Bits.MatchAllBits", docsWithField instanceof Bits.MatchAllBits);
    assertTrue("docsWithField(sparse) Size: " + docsWithField.length() + " is not: " + NUM_DOCS, docsWithField.length() == NUM_DOCS);
    for (int i = 0; i < docsWithField.length(); i++) {
      assertEquals(i%2 == 0, docsWithField.get(i));
    }

    // getTermsIndex
    SortedDocValues termsIndex = cache.getTermsIndex(reader, "theRandomUnicodeString");
    for (int i = 0; i < NUM_DOCS; i++) {
      final String s;
      final int ord = termsIndex.getOrd(i);
      if (ord == -1) {
        s = null;
      } else {
        s = termsIndex.lookupOrd(ord).utf8ToString();
      }
      assertTrue("for doc " + i + ": " + s + " does not equal: " + unicodeStrings[i], unicodeStrings[i] == null || unicodeStrings[i].equals(s));
    }

    int nTerms = termsIndex.getValueCount();

    TermsEnum tenum = termsIndex.termsEnum();
    for (int i=0; i<nTerms; i++) {
      BytesRef val1 = BytesRef.deepCopyOf(tenum.next());
      final BytesRef val = termsIndex.lookupOrd(i);
      // System.out.println("i="+i);
      assertEquals(val, val1);
    }

    // seek the enum around (note this isn't a great test here)
    int num = atLeast(100);
    for (int i = 0; i < num; i++) {
      int k = random().nextInt(nTerms);
      final BytesRef val = BytesRef.deepCopyOf(termsIndex.lookupOrd(k));
      assertEquals(TermsEnum.SeekStatus.FOUND, tenum.seekCeil(val));
      assertEquals(val, tenum.term());
    }

    for(int i=0;i<nTerms;i++) {
      final BytesRef val = BytesRef.deepCopyOf(termsIndex.lookupOrd(i));
      assertEquals(TermsEnum.SeekStatus.FOUND, tenum.seekCeil(val));
      assertEquals(val, tenum.term());
    }

    // test bad field
    termsIndex = cache.getTermsIndex(reader, "bogusfield");

    // getTerms
    BinaryDocValues terms = cache.getTerms(reader, "theRandomUnicodeString", true);
    Bits bits = cache.getDocsWithField(reader, "theRandomUnicodeString", null);
    for (int i = 0; i < NUM_DOCS; i++) {
      final String s;
      if (!bits.get(i)) {
        s = null;
      } else {
        s = terms.get(i).utf8ToString();
      }
      assertTrue("for doc " + i + ": " + s + " does not equal: " + unicodeStrings[i], unicodeStrings[i] == null || unicodeStrings[i].equals(s));
    }

    // test bad field
    terms = cache.getTerms(reader, "bogusfield", false);

    // getDocTermOrds
    SortedSetDocValues termOrds = cache.getDocTermOrds(reader, "theRandomUnicodeMultiValuedField", null);
    int numEntries = cache.getCacheEntries().length;
    // ask for it again, and check that we didnt create any additional entries:
    termOrds = cache.getDocTermOrds(reader, "theRandomUnicodeMultiValuedField", null);
    assertEquals(numEntries, cache.getCacheEntries().length);

    for (int i = 0; i < NUM_DOCS; i++) {
      termOrds.setDocument(i);
      // This will remove identical terms. A DocTermOrds doesn't return duplicate ords for a docId
      List<BytesRef> values = new ArrayList<>(new LinkedHashSet<>(Arrays.asList(multiValued[i])));
      for (BytesRef v : values) {
        if (v == null) {
          // why does this test use null values... instead of an empty list: confusing
          break;
        }
        long ord = termOrds.nextOrd();
        assert ord != SortedSetDocValues.NO_MORE_ORDS;
        BytesRef scratch = termOrds.lookupOrd(ord);
        assertEquals(v, scratch);
      }
      assertEquals(SortedSetDocValues.NO_MORE_ORDS, termOrds.nextOrd());
    }

    // test bad field
    termOrds = cache.getDocTermOrds(reader, "bogusfield", null);
    assertTrue(termOrds.getValueCount() == 0);

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

  private static String generateString(int i) {
    String s = null;
    if (i > 0 && random().nextInt(3) == 1) {
      // reuse past string -- try to find one that's not null
      for(int iter = 0; iter < 10 && s == null;iter++) {
        s = unicodeStrings[random().nextInt(i)];
      }
      if (s == null) {
        s = TestUtil.randomUnicodeString(random());
      }
    } else {
      s = TestUtil.randomUnicodeString(random());
    }
    return s;
  }

  public void testDocsWithField() throws Exception {
    FieldCache cache = FieldCache.DEFAULT;
    cache.purgeAllCaches();
    assertEquals(0, cache.getCacheEntries().length);
    cache.getNumerics(reader, "theDouble", FieldCache.DOUBLE_POINT_PARSER, true);

    // The double[] takes one slots, and docsWithField should also
    // have been populated:
    assertEquals(2, cache.getCacheEntries().length);
    Bits bits = cache.getDocsWithField(reader, "theDouble", FieldCache.DOUBLE_POINT_PARSER);

    // No new entries should appear:
    assertEquals(2, cache.getCacheEntries().length);
    assertTrue(bits instanceof Bits.MatchAllBits);

    NumericDocValues ints = cache.getNumerics(reader, "sparse", FieldCache.INT_POINT_PARSER, true);
    assertEquals(4, cache.getCacheEntries().length);
    Bits docsWithField = cache.getDocsWithField(reader, "sparse", FieldCache.INT_POINT_PARSER);
    assertEquals(4, cache.getCacheEntries().length);
    for (int i = 0; i < docsWithField.length(); i++) {
      if (i%2 == 0) {
        assertTrue(docsWithField.get(i));
        assertEquals(i, ints.get(i));
      } else {
        assertFalse(docsWithField.get(i));
      }
    }

    NumericDocValues numInts = cache.getNumerics(reader, "numInt", FieldCache.INT_POINT_PARSER, random().nextBoolean());
    docsWithField = cache.getDocsWithField(reader, "numInt", FieldCache.INT_POINT_PARSER);
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
                  Bits docsWithField = cache.getDocsWithField(reader, "sparse", FieldCache.INT_POINT_PARSER);
                  for (int i = 0; i < docsWithField.length(); i++) {
                    assertEquals(i%2 == 0, docsWithField.get(i));
                  }
                } else {
                  NumericDocValues ints = cache.getNumerics(reader, "sparse", FieldCache.INT_POINT_PARSER, true);
                  Bits docsWithField = cache.getDocsWithField(reader, "sparse", FieldCache.INT_POINT_PARSER);
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
      FieldCache.DEFAULT.getNumerics(ar, "binary", FieldCache.INT_POINT_PARSER, false);
    });
    
    BinaryDocValues binary = FieldCache.DEFAULT.getTerms(ar, "binary", true);
    final BytesRef term = binary.get(0);
    assertEquals("binary value", term.utf8ToString());
    
    expectThrows(IllegalStateException.class, () -> {
      FieldCache.DEFAULT.getTermsIndex(ar, "binary");
    });
    
    expectThrows(IllegalStateException.class, () -> {
      FieldCache.DEFAULT.getDocTermOrds(ar, "binary", null);
    });
    
    expectThrows(IllegalStateException.class, () -> {
      new DocTermOrds(ar, null, "binary");
    });
    
    Bits bits = FieldCache.DEFAULT.getDocsWithField(ar, "binary", null);
    assertTrue(bits.get(0));
    
    // Sorted type: can be retrieved via getTerms(), getTermsIndex(), getDocTermOrds()
    expectThrows(IllegalStateException.class, () -> {
      FieldCache.DEFAULT.getNumerics(ar, "sorted", FieldCache.INT_POINT_PARSER, false);
    });
    
    expectThrows(IllegalStateException.class, () -> {
      new DocTermOrds(ar, null, "sorted");
    });
    
    binary = FieldCache.DEFAULT.getTerms(ar, "sorted", true);
    BytesRef scratch = binary.get(0);
    assertEquals("sorted value", scratch.utf8ToString());
    
    SortedDocValues sorted = FieldCache.DEFAULT.getTermsIndex(ar, "sorted");
    assertEquals(0, sorted.getOrd(0));
    assertEquals(1, sorted.getValueCount());
    scratch = sorted.get(0);
    assertEquals("sorted value", scratch.utf8ToString());
    
    SortedSetDocValues sortedSet = FieldCache.DEFAULT.getDocTermOrds(ar, "sorted", null);
    sortedSet.setDocument(0);
    assertEquals(0, sortedSet.nextOrd());
    assertEquals(SortedSetDocValues.NO_MORE_ORDS, sortedSet.nextOrd());
    assertEquals(1, sortedSet.getValueCount());
    
    bits = FieldCache.DEFAULT.getDocsWithField(ar, "sorted", null);
    assertTrue(bits.get(0));
    
    // Numeric type: can be retrieved via getInts() and so on
    NumericDocValues numeric = FieldCache.DEFAULT.getNumerics(ar, "numeric", FieldCache.INT_POINT_PARSER, false);
    assertEquals(42, numeric.get(0));
    
    expectThrows(IllegalStateException.class, () -> {
      FieldCache.DEFAULT.getTerms(ar, "numeric", true);
    });
    
    expectThrows(IllegalStateException.class, () -> {
      FieldCache.DEFAULT.getTermsIndex(ar, "numeric");
    });
    
    expectThrows(IllegalStateException.class, () -> {
      FieldCache.DEFAULT.getDocTermOrds(ar, "numeric", null);
    });
    
    expectThrows(IllegalStateException.class, () -> {
      new DocTermOrds(ar, null, "numeric");
    });
    
    bits = FieldCache.DEFAULT.getDocsWithField(ar, "numeric", null);
    assertTrue(bits.get(0));
    
    // SortedSet type: can be retrieved via getDocTermOrds() 
    expectThrows(IllegalStateException.class, () -> {
      FieldCache.DEFAULT.getNumerics(ar, "sortedset", FieldCache.INT_POINT_PARSER, false);
    });
    
    expectThrows(IllegalStateException.class, () -> {
      FieldCache.DEFAULT.getTerms(ar, "sortedset", true);
    });
    
    expectThrows(IllegalStateException.class, () -> {
      FieldCache.DEFAULT.getTermsIndex(ar, "sortedset");
    });
    
    expectThrows(IllegalStateException.class, () -> {
      new DocTermOrds(ar, null, "sortedset");
    });
    
    sortedSet = FieldCache.DEFAULT.getDocTermOrds(ar, "sortedset", null);
    sortedSet.setDocument(0);
    assertEquals(0, sortedSet.nextOrd());
    assertEquals(1, sortedSet.nextOrd());
    assertEquals(SortedSetDocValues.NO_MORE_ORDS, sortedSet.nextOrd());
    assertEquals(2, sortedSet.getValueCount());
    
    bits = FieldCache.DEFAULT.getDocsWithField(ar, "sortedset", null);
    assertTrue(bits.get(0));
    
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
    
    NumericDocValues ints = cache.getNumerics(ar, "bogusints", FieldCache.INT_POINT_PARSER, true);
    assertEquals(0, ints.get(0));
    
    NumericDocValues longs = cache.getNumerics(ar, "boguslongs", FieldCache.LONG_POINT_PARSER, true);
    assertEquals(0, longs.get(0));
    
    NumericDocValues floats = cache.getNumerics(ar, "bogusfloats", FieldCache.FLOAT_POINT_PARSER, true);
    assertEquals(0, floats.get(0));
    
    NumericDocValues doubles = cache.getNumerics(ar, "bogusdoubles", FieldCache.DOUBLE_POINT_PARSER, true);
    assertEquals(0, doubles.get(0));
    
    BinaryDocValues binaries = cache.getTerms(ar, "bogusterms", true);
    BytesRef scratch = binaries.get(0);
    assertEquals(0, scratch.length);
    
    SortedDocValues sorted = cache.getTermsIndex(ar, "bogustermsindex");
    assertEquals(-1, sorted.getOrd(0));
    scratch = sorted.get(0);
    assertEquals(0, scratch.length);
    
    SortedSetDocValues sortedSet = cache.getDocTermOrds(ar, "bogusmultivalued", null);
    sortedSet.setDocument(0);
    assertEquals(SortedSetDocValues.NO_MORE_ORDS, sortedSet.nextOrd());
    
    Bits bits = cache.getDocsWithField(ar, "bogusbits", null);
    assertFalse(bits.get(0));
    
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
    doc.add(new StoredField("bogusterms", "bogus"));
    doc.add(new StoredField("bogustermsindex", "bogus"));
    doc.add(new StoredField("bogusmultivalued", "bogus"));
    doc.add(new StoredField("bogusbits", "bogus"));
    iw.addDocument(doc);
    DirectoryReader ir = iw.getReader();
    iw.close();
    
    LeafReader ar = getOnlyLeafReader(ir);
    
    final FieldCache cache = FieldCache.DEFAULT;
    cache.purgeAllCaches();
    assertEquals(0, cache.getCacheEntries().length);
    
    NumericDocValues ints = cache.getNumerics(ar, "bogusints", FieldCache.INT_POINT_PARSER, true);
    assertEquals(0, ints.get(0));
    
    NumericDocValues longs = cache.getNumerics(ar, "boguslongs", FieldCache.LONG_POINT_PARSER, true);
    assertEquals(0, longs.get(0));
    
    NumericDocValues floats = cache.getNumerics(ar, "bogusfloats", FieldCache.FLOAT_POINT_PARSER, true);
    assertEquals(0, floats.get(0));
    
    NumericDocValues doubles = cache.getNumerics(ar, "bogusdoubles", FieldCache.DOUBLE_POINT_PARSER, true);
    assertEquals(0, doubles.get(0));
    
    BinaryDocValues binaries = cache.getTerms(ar, "bogusterms", true);
    BytesRef scratch = binaries.get(0);
    assertEquals(0, scratch.length);
    
    SortedDocValues sorted = cache.getTermsIndex(ar, "bogustermsindex");
    assertEquals(-1, sorted.getOrd(0));
    scratch = sorted.get(0);
    assertEquals(0, scratch.length);
    
    SortedSetDocValues sortedSet = cache.getDocTermOrds(ar, "bogusmultivalued", null);
    sortedSet.setDocument(0);
    assertEquals(SortedSetDocValues.NO_MORE_ORDS, sortedSet.nextOrd());
    
    Bits bits = cache.getDocsWithField(ar, "bogusbits", null);
    assertFalse(bits.get(0));
    
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
    LongPoint field = new LongPoint("f", 0L);
    StoredField field2 = new StoredField("f", 0L);
    doc.add(field);
    doc.add(field2);
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
        field2.setLongValue(v);
        iw.addDocument(doc);
      }
    }
    iw.forceMerge(1);
    final DirectoryReader reader = iw.getReader();
    final NumericDocValues longs = FieldCache.DEFAULT.getNumerics(getOnlyLeafReader(reader), "f", FieldCache.LONG_POINT_PARSER, false);
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
    IntPoint field = new IntPoint("f", 0);
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
    final NumericDocValues ints = FieldCache.DEFAULT.getNumerics(getOnlyLeafReader(reader), "f", FieldCache.INT_POINT_PARSER, false);
    for (int i = 0; i < values.length; ++i) {
      assertEquals(values[i], ints.get(i));
    }
    reader.close();
    iw.close();
    dir.close();
  }

}
