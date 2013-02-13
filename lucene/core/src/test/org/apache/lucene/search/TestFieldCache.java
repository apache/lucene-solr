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
import java.util.Locale;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.*;
import org.apache.lucene.search.FieldCache.Ints;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;
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
    RandomIndexWriter writer= new RandomIndexWriter(random(), directory, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())).setMergePolicy(newLogMergePolicy()));
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
      cache.setInfoStream(new PrintStream(bos, false, "UTF-8"));
      cache.getDoubles(reader, "theDouble", false);
      cache.getFloats(reader, "theDouble", false);
      assertTrue(bos.toString("UTF-8").indexOf("WARNING") != -1);
    } finally {
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
    assertSame("Second request to cache return same array", termsIndex, cache.getTermsIndex(reader, "theRandomUnicodeString"));
    final BytesRef br = new BytesRef();
    for (int i = 0; i < NUM_DOCS; i++) {
      final BytesRef term;
      final int ord = termsIndex.getOrd(i);
      if (ord == -1) {
        term = null;
      } else {
        termsIndex.lookupOrd(ord, br);
        term = br;
      }
      final String s = term == null ? null : term.utf8ToString();
      assertTrue("for doc " + i + ": " + s + " does not equal: " + unicodeStrings[i], unicodeStrings[i] == null || unicodeStrings[i].equals(s));
    }

    int nTerms = termsIndex.getValueCount();

    TermsEnum tenum = new SortedDocValuesTermsEnum(termsIndex);
    BytesRef val = new BytesRef();
    for (int i=0; i<nTerms; i++) {
      BytesRef val1 = tenum.next();
      termsIndex.lookupOrd(i, val);
      // System.out.println("i="+i);
      assertEquals(val, val1);
    }

    // seek the enum around (note this isn't a great test here)
    int num = atLeast(100);
    for (int i = 0; i < num; i++) {
      int k = random().nextInt(nTerms);
      termsIndex.lookupOrd(k, val);
      assertEquals(TermsEnum.SeekStatus.FOUND, tenum.seekCeil(val));
      assertEquals(val, tenum.term());
    }

    for(int i=0;i<nTerms;i++) {
      termsIndex.lookupOrd(i, val);
      assertEquals(TermsEnum.SeekStatus.FOUND, tenum.seekCeil(val));
      assertEquals(val, tenum.term());
    }

    // test bad field
    termsIndex = cache.getTermsIndex(reader, "bogusfield");

    // getTerms
    BinaryDocValues terms = cache.getTerms(reader, "theRandomUnicodeString");
    assertSame("Second request to cache return same array", terms, cache.getTerms(reader, "theRandomUnicodeString"));
    for (int i = 0; i < NUM_DOCS; i++) {
      terms.get(i, br);
      final BytesRef term;
      if (br.bytes == BinaryDocValues.MISSING) {
        term = null;
      } else {
        term = br;
      }
      final String s = term == null ? null : term.utf8ToString();
      assertTrue("for doc " + i + ": " + s + " does not equal: " + unicodeStrings[i], unicodeStrings[i] == null || unicodeStrings[i].equals(s));
    }

    // test bad field
    terms = cache.getTerms(reader, "bogusfield");

    // getDocTermOrds
    DocTermOrds termOrds = cache.getDocTermOrds(reader, "theRandomUnicodeMultiValuedField");
    TermsEnum termsEnum = termOrds.getOrdTermsEnum(reader);
    assertSame("Second request to cache return same DocTermOrds", termOrds, cache.getDocTermOrds(reader, "theRandomUnicodeMultiValuedField"));
    DocTermOrds.TermOrdsIterator reuse = null;
    for (int i = 0; i < NUM_DOCS; i++) {
      reuse = termOrds.lookup(i, reuse);
      final int[] buffer = new int[5];
      // This will remove identical terms. A DocTermOrds doesn't return duplicate ords for a docId
      List<BytesRef> values = new ArrayList<BytesRef>(new LinkedHashSet<BytesRef>(Arrays.asList(multiValued[i])));
      for (;;) {
        int chunk = reuse.read(buffer);
        if (chunk == 0) {
          for (int ord = 0; ord < values.size(); ord++) {
            BytesRef term = values.get(ord);
            assertNull(String.format(Locale.ROOT, "Document[%d] misses field must be null. Has value %s for ord %d", i, term, ord), term);
          }
          break;
        }

        for(int idx=0; idx < chunk; idx++) {
          int key = buffer[idx];
          termsEnum.seekExact((long) key);
          String actual = termsEnum.term().utf8ToString();
          String expected = values.get(idx).utf8ToString();
          if (!expected.equals(actual)) {
              reuse = termOrds.lookup(i, reuse);
              reuse.read(buffer);
          }
          assertTrue(String.format(Locale.ROOT, "Expected value %s for doc %d and ord %d, but was %s", expected, i, idx, actual), expected.equals(actual));
        }

        if (chunk <= buffer.length) {
          break;
        }
      }
    }

    // test bad field
    termOrds = cache.getDocTermOrds(reader, "bogusfield");

    FieldCache.DEFAULT.purge(reader);
  }

  public void testEmptyIndex() throws Exception {
    Directory dir = newDirectory();
    IndexWriter writer= new IndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random())).setMaxBufferedDocs(500));
    writer.close();
    IndexReader r = DirectoryReader.open(dir);
    AtomicReader reader = SlowCompositeReaderWrapper.wrap(r);
    FieldCache.DEFAULT.getTerms(reader, "foobar");
    FieldCache.DEFAULT.getTermsIndex(reader, "foobar");
    FieldCache.DEFAULT.purge(reader);
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
        s = _TestUtil.randomUnicodeString(random());
      }
    } else {
      s = _TestUtil.randomUnicodeString(random());
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
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(new BinaryDocValuesField("binary", new BytesRef("binary value")));
    doc.add(new SortedDocValuesField("sorted", new BytesRef("sorted value")));
    doc.add(new NumericDocValuesField("numeric", 42));
    iw.addDocument(doc);
    DirectoryReader ir = iw.getReader();
    iw.close();
    AtomicReader ar = getOnlySegmentReader(ir);
    
    BytesRef scratch = new BytesRef();
    
    // Binary type: can be retrieved via getTerms()
    try {
      FieldCache.DEFAULT.getInts(ar, "binary", false);
      fail();
    } catch (IllegalStateException expected) {}
    
    BinaryDocValues binary = FieldCache.DEFAULT.getTerms(ar, "binary");
    binary.get(0, scratch);
    assertEquals("binary value", scratch.utf8ToString());
    
    try {
      FieldCache.DEFAULT.getTermsIndex(ar, "binary");
      fail();
    } catch (IllegalStateException expected) {}
    
    Bits bits = FieldCache.DEFAULT.getDocsWithField(ar, "binary");
    assertTrue(bits instanceof Bits.MatchAllBits);
    
    // Sorted type: can be retrieved via getTerms() or getTermsIndex()
    try {
      FieldCache.DEFAULT.getInts(ar, "sorted", false);
      fail();
    } catch (IllegalStateException expected) {}
    
    binary = FieldCache.DEFAULT.getTerms(ar, "sorted");
    binary.get(0, scratch);
    assertEquals("sorted value", scratch.utf8ToString());
    
    SortedDocValues sorted = FieldCache.DEFAULT.getTermsIndex(ar, "sorted");
    assertEquals(0, sorted.getOrd(0));
    assertEquals(1, sorted.getValueCount());
    sorted.get(0, scratch);
    assertEquals("sorted value", scratch.utf8ToString());
    
    bits = FieldCache.DEFAULT.getDocsWithField(ar, "sorted");
    assertTrue(bits instanceof Bits.MatchAllBits);
    
    // Numeric type: can be retrieved via getInts() and so on
    Ints numeric = FieldCache.DEFAULT.getInts(ar, "numeric", false);
    assertEquals(42, numeric.get(0));
    
    try {
      FieldCache.DEFAULT.getTerms(ar, "numeric");
      fail();
    } catch (IllegalStateException expected) {}
    
    try {
      FieldCache.DEFAULT.getTermsIndex(ar, "numeric");
      fail();
    } catch (IllegalStateException expected) {}
    
    bits = FieldCache.DEFAULT.getDocsWithField(ar, "numeric");
    assertTrue(bits instanceof Bits.MatchAllBits);
    
    ir.close();
    dir.close();
  }
}
