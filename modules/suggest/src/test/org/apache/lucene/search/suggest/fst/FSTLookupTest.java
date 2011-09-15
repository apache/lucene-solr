package org.apache.lucene.search.suggest.fst;

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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Random;

import org.apache.lucene.search.suggest.Lookup.LookupResult;
import org.apache.lucene.search.suggest.fst.FSTLookup;
import org.apache.lucene.util.LuceneTestCase;

import org.apache.lucene.search.suggest.LookupBenchmarkTest;
import org.apache.lucene.search.suggest.TermFreq;
import org.apache.lucene.search.suggest.TermFreqArrayIterator;

/**
 * Unit tests for {@link FSTLookup}.
 */
public class FSTLookupTest extends LuceneTestCase {
  public static TermFreq tf(String t, float v) {
    return new TermFreq(t, v);
  }

  private FSTLookup lookup;

  public void setUp() throws Exception {
    super.setUp();

    lookup = new FSTLookup();
    lookup.build(new TermFreqArrayIterator(evalKeys()));
  }

  private TermFreq[] evalKeys() {
    final TermFreq[] keys = new TermFreq[] {
        tf("one", 0.5f),
        tf("oneness", 1),
        tf("onerous", 1),
        tf("onesimus", 1),
        tf("two", 1),
        tf("twofold", 1),
        tf("twonk", 1),
        tf("thrive", 1),
        tf("through", 1),
        tf("threat", 1),
        tf("three", 1),
        tf("foundation", 1),
        tf("fourier", 1),
        tf("four", 1),
        tf("fourty", 1),
        tf("xo", 1),
      };
    return keys;
  }

  public void testExactMatchHighPriority() throws Exception {
    assertMatchEquals(lookup.lookup("two", true, 1), "two/1.0");
  }

  public void testExactMatchLowPriority() throws Exception {
    assertMatchEquals(lookup.lookup("one", true, 2), 
        "one/0.0",
        "oneness/1.0");
  }

  public void testRequestedCount() throws Exception {
    // 'one' is promoted after collecting two higher ranking results.
    assertMatchEquals(lookup.lookup("one", true, 2), 
        "one/0.0", 
        "oneness/1.0");

    // 'one' is at the top after collecting all alphabetical results. 
    assertMatchEquals(lookup.lookup("one", false, 2), 
        "one/0.0", 
        "oneness/1.0");

    lookup = new FSTLookup(10, false);
    lookup.build(new TermFreqArrayIterator(evalKeys()));
    
    // 'one' is not promoted after collecting two higher ranking results.
    assertMatchEquals(lookup.lookup("one", true, 2),  
        "oneness/1.0",
        "onerous/1.0");

    // 'one' is at the top after collecting all alphabetical results. 
    assertMatchEquals(lookup.lookup("one", false, 2), 
        "one/0.0", 
        "oneness/1.0");
  }

  public void testMiss() throws Exception {
    assertMatchEquals(lookup.lookup("xyz", true, 1));
  }

  public void testAlphabeticWithWeights() throws Exception {
    assertEquals(0, lookup.lookup("xyz", false, 1).size());
  }

  public void testFullMatchList() throws Exception {
    assertMatchEquals(lookup.lookup("one", true, Integer.MAX_VALUE),
        "oneness/1.0", 
        "onerous/1.0",
        "onesimus/1.0", 
        "one/0.0");
  }

  public void testMultilingualInput() throws Exception {
    List<TermFreq> input = LookupBenchmarkTest.readTop50KWiki();

    lookup = new FSTLookup();
    lookup.build(new TermFreqArrayIterator(input));

    for (TermFreq tf : input) {
      assertTrue("Not found: " + tf.term, lookup.get(tf.term) != null);
      assertEquals(tf.term, lookup.lookup(tf.term, true, 1).get(0).key);
    }
  }

  public void testEmptyInput() throws Exception {
    lookup = new FSTLookup();
    lookup.build(new TermFreqArrayIterator(new TermFreq[0]));
    
    assertMatchEquals(lookup.lookup("", true, 10));
  }

  public void testRandom() throws Exception {
    List<TermFreq> freqs = new ArrayList<TermFreq>();
    Random rnd = random;
    for (int i = 0; i < 5000; i++) {
      freqs.add(new TermFreq("" + rnd.nextLong(), rnd.nextInt(100)));
    }
    lookup = new FSTLookup();
    lookup.build(new TermFreqArrayIterator(freqs.toArray(new TermFreq[freqs.size()])));

    for (TermFreq tf : freqs) {
      final String term = tf.term;
      for (int i = 1; i < term.length(); i++) {
        String prefix = term.substring(0, i);
        for (LookupResult lr : lookup.lookup(prefix, true, 10)) {
          assertTrue(lr.key.startsWith(prefix));
        }
      }
    }
  }

  private void assertMatchEquals(List<LookupResult> res, String... expected) {
    String [] result = new String [res.size()];
    for (int i = 0; i < res.size(); i++)
      result[i] = res.get(i).toString();
    
    if (!Arrays.equals(expected, result)) {
      int colLen = Math.max(maxLen(expected), maxLen(result));
      
      StringBuilder b = new StringBuilder();
      String format = "%" + colLen + "s  " + "%" + colLen + "s\n"; 
      b.append(String.format(Locale.ENGLISH, format, "Expected", "Result"));
      for (int i = 0; i < Math.max(result.length, expected.length); i++) {
        b.append(String.format(Locale.ENGLISH, format, 
            i < expected.length ? expected[i] : "--", 
            i < result.length ? result[i] : "--"));
      }

      System.err.println(b.toString());
      fail("Expected different output:\n" + b.toString());
    }
  }

  private int maxLen(String[] result) {
    int len = 0;
    for (String s : result)
      len = Math.max(len, s.length());
    return len;
  }
}
