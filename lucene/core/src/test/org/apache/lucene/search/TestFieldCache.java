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
import org.apache.lucene.document.Document;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.*;
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
      doc.add(newField("theLong", String.valueOf(theLong--), StringField.TYPE_UNSTORED));
      doc.add(newField("theDouble", String.valueOf(theDouble--), StringField.TYPE_UNSTORED));
      doc.add(newField("theByte", String.valueOf(theByte--), StringField.TYPE_UNSTORED));
      doc.add(newField("theShort", String.valueOf(theShort--), StringField.TYPE_UNSTORED));
      doc.add(newField("theInt", String.valueOf(theInt--), StringField.TYPE_UNSTORED));
      doc.add(newField("theFloat", String.valueOf(theFloat--), StringField.TYPE_UNSTORED));
      if (i%2 == 0) {
        doc.add(newField("sparse", String.valueOf(i), StringField.TYPE_UNSTORED));
      }

      if (i%2 == 0) {
        doc.add(new IntField("numInt", i));
      }

      // sometimes skip the field:
      if (random().nextInt(40) != 17) {
        unicodeStrings[i] = generateString(i);
        doc.add(newField("theRandomUnicodeString", unicodeStrings[i], StringField.TYPE_STORED));
      }

      // sometimes skip the field:
      if (random().nextInt(10) != 8) {
        for (int j = 0; j < NUM_ORDS; j++) {
          String newValue = generateString(i);
          multiValued[i][j] = new BytesRef(newValue);
          doc.add(newField("theRandomUnicodeMultiValuedField", newValue, StringField.TYPE_STORED));
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
      cache.setInfoStream(new PrintStream(bos));
      cache.getDoubles(reader, "theDouble", false);
      cache.getFloats(reader, "theDouble", false);
      assertTrue(bos.toString().indexOf("WARNING") != -1);
    } finally {
      FieldCache.DEFAULT.purgeAllCaches();
    }
  }

  public void test() throws IOException {
    FieldCache cache = FieldCache.DEFAULT;
    double [] doubles = cache.getDoubles(reader, "theDouble", random().nextBoolean());
    assertSame("Second request to cache return same array", doubles, cache.getDoubles(reader, "theDouble", random().nextBoolean()));
    assertSame("Second request with explicit parser return same array", doubles, cache.getDoubles(reader, "theDouble", FieldCache.DEFAULT_DOUBLE_PARSER, random().nextBoolean()));
    assertTrue("doubles Size: " + doubles.length + " is not: " + NUM_DOCS, doubles.length == NUM_DOCS);
    for (int i = 0; i < doubles.length; i++) {
      assertTrue(doubles[i] + " does not equal: " + (Double.MAX_VALUE - i), doubles[i] == (Double.MAX_VALUE - i));

    }
    
    long [] longs = cache.getLongs(reader, "theLong", random().nextBoolean());
    assertSame("Second request to cache return same array", longs, cache.getLongs(reader, "theLong", random().nextBoolean()));
    assertSame("Second request with explicit parser return same array", longs, cache.getLongs(reader, "theLong", FieldCache.DEFAULT_LONG_PARSER, random().nextBoolean()));
    assertTrue("longs Size: " + longs.length + " is not: " + NUM_DOCS, longs.length == NUM_DOCS);
    for (int i = 0; i < longs.length; i++) {
      assertTrue(longs[i] + " does not equal: " + (Long.MAX_VALUE - i) + " i=" + i, longs[i] == (Long.MAX_VALUE - i));

    }
    
    byte [] bytes = cache.getBytes(reader, "theByte", random().nextBoolean());
    assertSame("Second request to cache return same array", bytes, cache.getBytes(reader, "theByte", random().nextBoolean()));
    assertSame("Second request with explicit parser return same array", bytes, cache.getBytes(reader, "theByte", FieldCache.DEFAULT_BYTE_PARSER, random().nextBoolean()));
    assertTrue("bytes Size: " + bytes.length + " is not: " + NUM_DOCS, bytes.length == NUM_DOCS);
    for (int i = 0; i < bytes.length; i++) {
      assertTrue(bytes[i] + " does not equal: " + (Byte.MAX_VALUE - i), bytes[i] == (byte) (Byte.MAX_VALUE - i));

    }
    
    short [] shorts = cache.getShorts(reader, "theShort", random().nextBoolean());
    assertSame("Second request to cache return same array", shorts, cache.getShorts(reader, "theShort", random().nextBoolean()));
    assertSame("Second request with explicit parser return same array", shorts, cache.getShorts(reader, "theShort", FieldCache.DEFAULT_SHORT_PARSER, random().nextBoolean()));
    assertTrue("shorts Size: " + shorts.length + " is not: " + NUM_DOCS, shorts.length == NUM_DOCS);
    for (int i = 0; i < shorts.length; i++) {
      assertTrue(shorts[i] + " does not equal: " + (Short.MAX_VALUE - i), shorts[i] == (short) (Short.MAX_VALUE - i));

    }
    
    int [] ints = cache.getInts(reader, "theInt", random().nextBoolean());
    assertSame("Second request to cache return same array", ints, cache.getInts(reader, "theInt", random().nextBoolean()));
    assertSame("Second request with explicit parser return same array", ints, cache.getInts(reader, "theInt", FieldCache.DEFAULT_INT_PARSER, random().nextBoolean()));
    assertTrue("ints Size: " + ints.length + " is not: " + NUM_DOCS, ints.length == NUM_DOCS);
    for (int i = 0; i < ints.length; i++) {
      assertTrue(ints[i] + " does not equal: " + (Integer.MAX_VALUE - i), ints[i] == (Integer.MAX_VALUE - i));

    }
    
    float [] floats = cache.getFloats(reader, "theFloat", random().nextBoolean());
    assertSame("Second request to cache return same array", floats, cache.getFloats(reader, "theFloat", random().nextBoolean()));
    assertSame("Second request with explicit parser return same array", floats, cache.getFloats(reader, "theFloat", FieldCache.DEFAULT_FLOAT_PARSER, random().nextBoolean()));
    assertTrue("floats Size: " + floats.length + " is not: " + NUM_DOCS, floats.length == NUM_DOCS);
    for (int i = 0; i < floats.length; i++) {
      assertTrue(floats[i] + " does not equal: " + (Float.MAX_VALUE - i), floats[i] == (Float.MAX_VALUE - i));

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
    FieldCache.DocTermsIndex termsIndex = cache.getTermsIndex(reader, "theRandomUnicodeString");
    assertSame("Second request to cache return same array", termsIndex, cache.getTermsIndex(reader, "theRandomUnicodeString"));
    assertTrue("doubles Size: " + termsIndex.size() + " is not: " + NUM_DOCS, termsIndex.size() == NUM_DOCS);
    final BytesRef br = new BytesRef();
    for (int i = 0; i < NUM_DOCS; i++) {
      final BytesRef term = termsIndex.getTerm(i, br);
      final String s = term == null ? null : term.utf8ToString();
      assertTrue("for doc " + i + ": " + s + " does not equal: " + unicodeStrings[i], unicodeStrings[i] == null || unicodeStrings[i].equals(s));
    }

    int nTerms = termsIndex.numOrd();
    // System.out.println("nTerms="+nTerms);

    TermsEnum tenum = termsIndex.getTermsEnum();
    BytesRef val = new BytesRef();
    for (int i=1; i<nTerms; i++) {
      BytesRef val1 = tenum.next();
      BytesRef val2 = termsIndex.lookup(i,val);
      // System.out.println("i="+i);
      assertEquals(val2, val1);
    }

    // seek the enum around (note this isn't a great test here)
    int num = atLeast(100);
    for (int i = 0; i < num; i++) {
      int k = _TestUtil.nextInt(random(), 1, nTerms-1);
      BytesRef val1 = termsIndex.lookup(k, val);
      assertEquals(TermsEnum.SeekStatus.FOUND, tenum.seekCeil(val1));
      assertEquals(val1, tenum.term());
    }
    
    // test bad field
    termsIndex = cache.getTermsIndex(reader, "bogusfield");

    // getTerms
    FieldCache.DocTerms terms = cache.getTerms(reader, "theRandomUnicodeString");
    assertSame("Second request to cache return same array", terms, cache.getTerms(reader, "theRandomUnicodeString"));
    assertTrue("doubles Size: " + terms.size() + " is not: " + NUM_DOCS, terms.size() == NUM_DOCS);
    for (int i = 0; i < NUM_DOCS; i++) {
      final BytesRef term = terms.getTerm(i, br);
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
            assertNull(String.format("Document[%d] misses field must be null. Has value %s for ord %d", i, term, ord), term);
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
          assertTrue(String.format("Expected value %s for doc %d and ord %d, but was %s", expected, i, idx, actual), expected.equals(actual));
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
    double[] doubles = cache.getDoubles(reader, "theDouble", true);

    // The double[] takes two slots (one w/ null parser, one
    // w/ real parser), and docsWithField should also
    // have been populated:
    assertEquals(3, cache.getCacheEntries().length);
    Bits bits = cache.getDocsWithField(reader, "theDouble");

    // No new entries should appear:
    assertEquals(3, cache.getCacheEntries().length);
    assertTrue(bits instanceof Bits.MatchAllBits);

    int[] ints = cache.getInts(reader, "sparse", true);
    assertEquals(6, cache.getCacheEntries().length);
    Bits docsWithField = cache.getDocsWithField(reader, "sparse");
    assertEquals(6, cache.getCacheEntries().length);
    for (int i = 0; i < docsWithField.length(); i++) {
      if (i%2 == 0) {
        assertTrue(docsWithField.get(i));
        assertEquals(i, ints[i]);
      } else {
        assertFalse(docsWithField.get(i));
      }
    }

    int[] numInts = cache.getInts(reader, "numInt", random().nextBoolean());
    docsWithField = cache.getDocsWithField(reader, "numInt");
    for (int i = 0; i < docsWithField.length(); i++) {
      if (i%2 == 0) {
        assertTrue(docsWithField.get(i));
        assertEquals(i, numInts[i]);
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
                  int[] ints = cache.getInts(reader, "sparse", true);
                  Bits docsWithField = cache.getDocsWithField(reader, "sparse");
                  for (int i = 0; i < docsWithField.length(); i++) {
                    if (i%2 == 0) {
                      assertTrue(docsWithField.get(i));
                      assertEquals(i, ints[i]);
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
}
