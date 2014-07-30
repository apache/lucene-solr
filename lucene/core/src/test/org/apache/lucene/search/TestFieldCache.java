package org.apache.lucene.search;

/**
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
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
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocTermOrds;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.SlowCompositeReaderWrapper;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.FieldCache.Bytes;
import org.apache.lucene.search.FieldCache.Doubles;
import org.apache.lucene.search.FieldCache.Floats;
import org.apache.lucene.search.FieldCache.Ints;
import org.apache.lucene.search.FieldCache.Longs;
import org.apache.lucene.search.FieldCache.Shorts;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class TestFieldCache extends LuceneTestCase {
  private static AtomicReader reader;
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
    RandomIndexWriter writer= new RandomIndexWriter(random(), directory, newIndexWriterConfig(new MockAnalyzer(random())).setMergePolicy(newLogMergePolicy()));
    long theLong = Long.MAX_VALUE;
    double theDouble = Double.MAX_VALUE;
    byte theByte = Byte.MAX_VALUE;
    short theShort = Short.MAX_VALUE;
    int theInt = Integer.MAX_VALUE;
    float theFloat = Float.MAX_VALUE;
    unicodeStrings = new String[NUM_DOCS];
    multiValued = new BytesRef[NUM_DOCS][NUM_ORDS];
    if (VERBOSE) {
      System.out.println("TEST: setUp");
    }
    for (int i = 0; i < NUM_DOCS; i++){
      Document doc = new Document();
      doc.add(newStringField("theLong", String.valueOf(theLong--), Field.Store.NO));
      doc.add(newStringField("theDouble", String.valueOf(theDouble--), Field.Store.NO));
      doc.add(newStringField("theByte", String.valueOf(theByte--), Field.Store.NO));
      doc.add(newStringField("theShort", String.valueOf(theShort--), Field.Store.NO));
      doc.add(newStringField("theInt", String.valueOf(theInt--), Field.Store.NO));
      doc.add(newStringField("theFloat", String.valueOf(theFloat--), Field.Store.NO));
      if (i%2 == 0) {
        doc.add(newStringField("sparse", String.valueOf(i), Field.Store.NO));
      }

      if (i%2 == 0) {
        doc.add(new IntField("numInt", i, Field.Store.NO));
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
    IndexReader r = writer.getReader();
    reader = SlowCompositeReaderWrapper.wrap(r);
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
  
  public void testInfoStream() throws Exception {
    try {
      FieldCache cache = FieldCache.DEFAULT;
      ByteArrayOutputStream bos = new ByteArrayOutputStream(1024);
      cache.setInfoStream(new PrintStream(bos, false, IOUtils.UTF_8));
      cache.getDoubles(reader, "theDouble", false);
      cache.getFloats(reader, "theDouble", false);
      assertTrue(bos.toString(IOUtils.UTF_8).indexOf("WARNING") != -1);
    } finally {
      FieldCache.DEFAULT.setInfoStream(null);
      FieldCache.DEFAULT.purgeAllCaches();
    }
  }

  public void test() throws IOException {
    FieldCache cache = FieldCache.DEFAULT;
    FieldCache.Doubles doubles = cache.getDoubles(reader, "theDouble", random().nextBoolean());
    assertSame("Second request to cache return same array", doubles, cache.getDoubles(reader, "theDouble", random().nextBoolean()));
    assertSame("Second request with explicit parser return same array", doubles, cache.getDoubles(reader, "theDouble", FieldCache.DEFAULT_DOUBLE_PARSER, random().nextBoolean()));
    for (int i = 0; i < NUM_DOCS; i++) {
      assertTrue(doubles.get(i) + " does not equal: " + (Double.MAX_VALUE - i), doubles.get(i) == (Double.MAX_VALUE - i));
    }
    
    FieldCache.Longs longs = cache.getLongs(reader, "theLong", random().nextBoolean());
    assertSame("Second request to cache return same array", longs, cache.getLongs(reader, "theLong", random().nextBoolean()));
    assertSame("Second request with explicit parser return same array", longs, cache.getLongs(reader, "theLong", FieldCache.DEFAULT_LONG_PARSER, random().nextBoolean()));
    for (int i = 0; i < NUM_DOCS; i++) {
      assertTrue(longs.get(i) + " does not equal: " + (Long.MAX_VALUE - i) + " i=" + i, longs.get(i) == (Long.MAX_VALUE - i));
    }
    
    FieldCache.Bytes bytes = cache.getBytes(reader, "theByte", random().nextBoolean());
    assertSame("Second request to cache return same array", bytes, cache.getBytes(reader, "theByte", random().nextBoolean()));
    assertSame("Second request with explicit parser return same array", bytes, cache.getBytes(reader, "theByte", FieldCache.DEFAULT_BYTE_PARSER, random().nextBoolean()));
    for (int i = 0; i < NUM_DOCS; i++) {
      assertTrue(bytes.get(i) + " does not equal: " + (Byte.MAX_VALUE - i), bytes.get(i) == (byte) (Byte.MAX_VALUE - i));
    }
    
    FieldCache.Shorts shorts = cache.getShorts(reader, "theShort", random().nextBoolean());
    assertSame("Second request to cache return same array", shorts, cache.getShorts(reader, "theShort", random().nextBoolean()));
    assertSame("Second request with explicit parser return same array", shorts, cache.getShorts(reader, "theShort", FieldCache.DEFAULT_SHORT_PARSER, random().nextBoolean()));
    for (int i = 0; i < NUM_DOCS; i++) {
      assertTrue(shorts.get(i) + " does not equal: " + (Short.MAX_VALUE - i), shorts.get(i) == (short) (Short.MAX_VALUE - i));
    }
    
    FieldCache.Ints ints = cache.getInts(reader, "theInt", random().nextBoolean());
    assertSame("Second request to cache return same array", ints, cache.getInts(reader, "theInt", random().nextBoolean()));
    assertSame("Second request with explicit parser return same array", ints, cache.getInts(reader, "theInt", FieldCache.DEFAULT_INT_PARSER, random().nextBoolean()));
    for (int i = 0; i < NUM_DOCS; i++) {
      assertTrue(ints.get(i) + " does not equal: " + (Integer.MAX_VALUE - i), ints.get(i) == (Integer.MAX_VALUE - i));
    }
    
    FieldCache.Floats floats = cache.getFloats(reader, "theFloat", random().nextBoolean());
    assertSame("Second request to cache return same array", floats, cache.getFloats(reader, "theFloat", random().nextBoolean()));
    assertSame("Second request with explicit parser return same array", floats, cache.getFloats(reader, "theFloat", FieldCache.DEFAULT_FLOAT_PARSER, random().nextBoolean()));
    for (int i = 0; i < NUM_DOCS; i++) {
      assertTrue(floats.get(i) + " does not equal: " + (Float.MAX_VALUE - i), floats.get(i) == (Float.MAX_VALUE - i));
    }

    Bits docsWithField = cache.getDocsWithField(reader, "theLong");
    assertSame("Second request to cache return same array", docsWithField, cache.getDocsWithField(reader, "theLong"));
    assertTrue("docsWithField(theLong) must be class Bits.MatchAllBits", docsWithField instanceof Bits.MatchAllBits);
    assertTrue("docsWithField(theLong) Size: " + docsWithField.length() + " is not: " + NUM_DOCS, docsWithField.length() == NUM_DOCS);
    for (int i = 0; i < docsWithField.length(); i++) {
      assertTrue(docsWithField.get(i));
    }
    
    docsWithField = cache.getDocsWithField(reader, "sparse");
    assertSame("Second request to cache return same array", docsWithField, cache.getDocsWithField(reader, "sparse"));
    assertFalse("docsWithField(sparse) must not be class Bits.MatchAllBits", docsWithField instanceof Bits.MatchAllBits);
    assertTrue("docsWithField(sparse) Size: " + docsWithField.length() + " is not: " + NUM_DOCS, docsWithField.length() == NUM_DOCS);
    for (int i = 0; i < docsWithField.length(); i++) {
      assertEquals(i%2 == 0, docsWithField.get(i));
    }

    // getTermsIndex
    SortedDocValues termsIndex = cache.getTermsIndex(reader, "theRandomUnicodeString");
    for (int i = 0; i < NUM_DOCS; i++) {
      final BytesRef term;
      final int ord = termsIndex.getOrd(i);
      if (ord == -1) {
        term = null;
      } else {
        term = termsIndex.lookupOrd(ord);
      }
      final String s = term == null ? null : term.utf8ToString();
      assertTrue("for doc " + i + ": " + s + " does not equal: " + unicodeStrings[i], unicodeStrings[i] == null || unicodeStrings[i].equals(s));
    }

    int nTerms = termsIndex.getValueCount();

    TermsEnum tenum = termsIndex.termsEnum();
    for (int i=0; i<nTerms; i++) {
      BytesRef val1 = tenum.next();
      BytesRef val = termsIndex.lookupOrd(i);
      // System.out.println("i="+i);
      assertEquals(val, val1);
    }

    // seek the enum around (note this isn't a great test here)
    int num = atLeast(100);
    for (int i = 0; i < num; i++) {
      int k = random().nextInt(nTerms);
      BytesRef val = BytesRef.deepCopyOf(termsIndex.lookupOrd(k));
      assertEquals(TermsEnum.SeekStatus.FOUND, tenum.seekCeil(val));
      assertEquals(val, tenum.term());
    }

    for(int i=0;i<nTerms;i++) {
      BytesRef val = BytesRef.deepCopyOf(termsIndex.lookupOrd(i));
      assertEquals(TermsEnum.SeekStatus.FOUND, tenum.seekCeil(val));
      assertEquals(val, tenum.term());
    }

    // test bad field
    termsIndex = cache.getTermsIndex(reader, "bogusfield");

    // getTerms
    BinaryDocValues terms = cache.getTerms(reader, "theRandomUnicodeString", true);
    Bits bits = cache.getDocsWithField(reader, "theRandomUnicodeString");
    for (int i = 0; i < NUM_DOCS; i++) {
      BytesRef br = terms.get(i);
      final BytesRef term;
      if (!bits.get(i)) {
        term = null;
      } else {
        term = br;
      }
      final String s = term == null ? null : term.utf8ToString();
      assertTrue("for doc " + i + ": " + s + " does not equal: " + unicodeStrings[i], unicodeStrings[i] == null || unicodeStrings[i].equals(s));
    }

    // test bad field
    terms = cache.getTerms(reader, "bogusfield", false);

    // getDocTermOrds
    SortedSetDocValues termOrds = cache.getDocTermOrds(reader, "theRandomUnicodeMultiValuedField");
    int numEntries = cache.getCacheEntries().length;
    // ask for it again, and check that we didnt create any additional entries:
    termOrds = cache.getDocTermOrds(reader, "theRandomUnicodeMultiValuedField");
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
    termOrds = cache.getDocTermOrds(reader, "bogusfield");
    assertTrue(termOrds.getValueCount() == 0);

    FieldCache.DEFAULT.purgeByCacheKey(reader.getCoreCacheKey());
  }

  public void testEmptyIndex() throws Exception {
    Directory dir = newDirectory();
    IndexWriter writer= new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())).setMaxBufferedDocs(500));
    writer.close();
    IndexReader r = DirectoryReader.open(dir);
    AtomicReader reader = SlowCompositeReaderWrapper.wrap(r);
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
    cache.getDoubles(reader, "theDouble", true);

    // The double[] takes two slots (one w/ null parser, one
    // w/ real parser), and docsWithField should also
    // have been populated:
    assertEquals(3, cache.getCacheEntries().length);
    Bits bits = cache.getDocsWithField(reader, "theDouble");

    // No new entries should appear:
    assertEquals(3, cache.getCacheEntries().length);
    assertTrue(bits instanceof Bits.MatchAllBits);

    FieldCache.Ints ints = cache.getInts(reader, "sparse", true);
    assertEquals(6, cache.getCacheEntries().length);
    Bits docsWithField = cache.getDocsWithField(reader, "sparse");
    assertEquals(6, cache.getCacheEntries().length);
    for (int i = 0; i < docsWithField.length(); i++) {
      if (i%2 == 0) {
        assertTrue(docsWithField.get(i));
        assertEquals(i, ints.get(i));
      } else {
        assertFalse(docsWithField.get(i));
      }
    }

    FieldCache.Ints numInts = cache.getInts(reader, "numInt", random().nextBoolean());
    docsWithField = cache.getDocsWithField(reader, "numInt");
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
                  Bits docsWithField = cache.getDocsWithField(reader, "sparse");
                  for (int i = 0; i < docsWithField.length(); i++) {
                    assertEquals(i%2 == 0, docsWithField.get(i));
                  }
                } else {
                  FieldCache.Ints ints = cache.getInts(reader, "sparse", true);
                  Bits docsWithField = cache.getDocsWithField(reader, "sparse");
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
    assumeTrue("3.x does not support docvalues", defaultCodecSupportsDocValues());
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(null);
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);
    Document doc = new Document();
    doc.add(new BinaryDocValuesField("binary", new BytesRef("binary value")));
    doc.add(new SortedDocValuesField("sorted", new BytesRef("sorted value")));
    doc.add(new NumericDocValuesField("numeric", 42));
    if (defaultCodecSupportsSortedSet()) {
      doc.add(new SortedSetDocValuesField("sortedset", new BytesRef("sortedset value1")));
      doc.add(new SortedSetDocValuesField("sortedset", new BytesRef("sortedset value2")));
    }
    iw.addDocument(doc);
    DirectoryReader ir = iw.getReader();
    iw.close();
    AtomicReader ar = getOnlySegmentReader(ir);
    
    // Binary type: can be retrieved via getTerms()
    try {
      FieldCache.DEFAULT.getInts(ar, "binary", false);
      fail();
    } catch (IllegalStateException expected) {}
    
    BinaryDocValues binary = FieldCache.DEFAULT.getTerms(ar, "binary", true);
    BytesRef scratch = binary.get(0);
    assertEquals("binary value", scratch.utf8ToString());
    
    try {
      FieldCache.DEFAULT.getTermsIndex(ar, "binary");
      fail();
    } catch (IllegalStateException expected) {}
    
    try {
      FieldCache.DEFAULT.getDocTermOrds(ar, "binary");
      fail();
    } catch (IllegalStateException expected) {}
    
    try {
      new DocTermOrds(ar, null, "binary");
      fail();
    } catch (IllegalStateException expected) {}
    
    Bits bits = FieldCache.DEFAULT.getDocsWithField(ar, "binary");
    assertTrue(bits.get(0));
    
    // Sorted type: can be retrieved via getTerms(), getTermsIndex(), getDocTermOrds()
    try {
      FieldCache.DEFAULT.getInts(ar, "sorted", false);
      fail();
    } catch (IllegalStateException expected) {}
    
    try {
      new DocTermOrds(ar, null, "sorted");
      fail();
    } catch (IllegalStateException expected) {}
    
    binary = FieldCache.DEFAULT.getTerms(ar, "sorted", true);
    scratch = binary.get(0);
    assertEquals("sorted value", scratch.utf8ToString());
    
    SortedDocValues sorted = FieldCache.DEFAULT.getTermsIndex(ar, "sorted");
    assertEquals(0, sorted.getOrd(0));
    assertEquals(1, sorted.getValueCount());
    scratch = sorted.get(0);
    assertEquals("sorted value", scratch.utf8ToString());
    
    SortedSetDocValues sortedSet = FieldCache.DEFAULT.getDocTermOrds(ar, "sorted");
    sortedSet.setDocument(0);
    assertEquals(0, sortedSet.nextOrd());
    assertEquals(SortedSetDocValues.NO_MORE_ORDS, sortedSet.nextOrd());
    assertEquals(1, sortedSet.getValueCount());
    
    bits = FieldCache.DEFAULT.getDocsWithField(ar, "sorted");
    assertTrue(bits.get(0));
    
    // Numeric type: can be retrieved via getInts() and so on
    Ints numeric = FieldCache.DEFAULT.getInts(ar, "numeric", false);
    assertEquals(42, numeric.get(0));
    
    try {
      FieldCache.DEFAULT.getTerms(ar, "numeric", true);
      fail();
    } catch (IllegalStateException expected) {}
    
    try {
      FieldCache.DEFAULT.getTermsIndex(ar, "numeric");
      fail();
    } catch (IllegalStateException expected) {}
    
    try {
      FieldCache.DEFAULT.getDocTermOrds(ar, "numeric");
      fail();
    } catch (IllegalStateException expected) {}
    
    try {
      new DocTermOrds(ar, null, "numeric");
      fail();
    } catch (IllegalStateException expected) {}
    
    bits = FieldCache.DEFAULT.getDocsWithField(ar, "numeric");
    assertTrue(bits.get(0));
    
    // SortedSet type: can be retrieved via getDocTermOrds() 
    if (defaultCodecSupportsSortedSet()) {
      try {
        FieldCache.DEFAULT.getInts(ar, "sortedset", false);
        fail();
      } catch (IllegalStateException expected) {}
    
      try {
        FieldCache.DEFAULT.getTerms(ar, "sortedset", true);
        fail();
      } catch (IllegalStateException expected) {}
    
      try {
        FieldCache.DEFAULT.getTermsIndex(ar, "sortedset");
        fail();
      } catch (IllegalStateException expected) {}
      
      try {
        new DocTermOrds(ar, null, "sortedset");
        fail();
      } catch (IllegalStateException expected) {}
    
      sortedSet = FieldCache.DEFAULT.getDocTermOrds(ar, "sortedset");
      sortedSet.setDocument(0);
      assertEquals(0, sortedSet.nextOrd());
      assertEquals(1, sortedSet.nextOrd());
      assertEquals(SortedSetDocValues.NO_MORE_ORDS, sortedSet.nextOrd());
      assertEquals(2, sortedSet.getValueCount());
    
      bits = FieldCache.DEFAULT.getDocsWithField(ar, "sortedset");
      assertTrue(bits.get(0));
    }
    
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
    
    AtomicReader ar = getOnlySegmentReader(ir);
    
    final FieldCache cache = FieldCache.DEFAULT;
    cache.purgeAllCaches();
    assertEquals(0, cache.getCacheEntries().length);
    
    Bytes bytes = cache.getBytes(ar, "bogusbytes", true);
    assertEquals(0, bytes.get(0));

    Shorts shorts = cache.getShorts(ar, "bogusshorts", true);
    assertEquals(0, shorts.get(0));
    
    Ints ints = cache.getInts(ar, "bogusints", true);
    assertEquals(0, ints.get(0));
    
    Longs longs = cache.getLongs(ar, "boguslongs", true);
    assertEquals(0, longs.get(0));
    
    Floats floats = cache.getFloats(ar, "bogusfloats", true);
    assertEquals(0, floats.get(0), 0.0f);
    
    Doubles doubles = cache.getDoubles(ar, "bogusdoubles", true);
    assertEquals(0, doubles.get(0), 0.0D);
    
    BinaryDocValues binaries = cache.getTerms(ar, "bogusterms", true);
    BytesRef scratch = binaries.get(0);
    assertEquals(0, scratch.length);
    
    SortedDocValues sorted = cache.getTermsIndex(ar, "bogustermsindex");
    assertEquals(-1, sorted.getOrd(0));
    scratch = sorted.get(0);
    assertEquals(0, scratch.length);
    
    SortedSetDocValues sortedSet = cache.getDocTermOrds(ar, "bogusmultivalued");
    sortedSet.setDocument(0);
    assertEquals(SortedSetDocValues.NO_MORE_ORDS, sortedSet.nextOrd());
    
    Bits bits = cache.getDocsWithField(ar, "bogusbits");
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
    
    AtomicReader ar = getOnlySegmentReader(ir);
    
    final FieldCache cache = FieldCache.DEFAULT;
    cache.purgeAllCaches();
    assertEquals(0, cache.getCacheEntries().length);
    
    Bytes bytes = cache.getBytes(ar, "bogusbytes", true);
    assertEquals(0, bytes.get(0));

    Shorts shorts = cache.getShorts(ar, "bogusshorts", true);
    assertEquals(0, shorts.get(0));
    
    Ints ints = cache.getInts(ar, "bogusints", true);
    assertEquals(0, ints.get(0));
    
    Longs longs = cache.getLongs(ar, "boguslongs", true);
    assertEquals(0, longs.get(0));
    
    Floats floats = cache.getFloats(ar, "bogusfloats", true);
    assertEquals(0, floats.get(0), 0.0f);
    
    Doubles doubles = cache.getDoubles(ar, "bogusdoubles", true);
    assertEquals(0, doubles.get(0), 0.0D);
    
    BinaryDocValues binaries = cache.getTerms(ar, "bogusterms", true);
    BytesRef scratch = binaries.get(0);
    assertEquals(0, scratch.length);
    
    SortedDocValues sorted = cache.getTermsIndex(ar, "bogustermsindex");
    assertEquals(-1, sorted.getOrd(0));
    scratch = sorted.get(0);
    assertEquals(0, scratch.length);
    
    SortedSetDocValues sortedSet = cache.getDocTermOrds(ar, "bogusmultivalued");
    sortedSet.setDocument(0);
    assertEquals(SortedSetDocValues.NO_MORE_ORDS, sortedSet.nextOrd());
    
    Bits bits = cache.getDocsWithField(ar, "bogusbits");
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
    LongField field = new LongField("f", 0L, Store.YES);
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
    final FieldCache.Longs longs = FieldCache.DEFAULT.getLongs(getOnlySegmentReader(reader), "f", false);
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
    IntField field = new IntField("f", 0, Store.YES);
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
    final FieldCache.Ints ints = FieldCache.DEFAULT.getInts(getOnlySegmentReader(reader), "f", false);
    for (int i = 0; i < values.length; ++i) {
      assertEquals(values[i], ints.get(i));
    }
    reader.close();
    iw.close();
    dir.close();
  }

}
