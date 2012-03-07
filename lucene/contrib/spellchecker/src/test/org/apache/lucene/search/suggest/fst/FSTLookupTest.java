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
import org.apache.lucene.util._TestUtil;

import org.apache.lucene.search.suggest.LookupBenchmarkTest;
import org.apache.lucene.search.suggest.TermFreq;
import org.apache.lucene.search.suggest.TermFreqArrayIterator;

/**
 * Unit tests for {@link FSTLookup}.
 * @deprecated Just to test the old API works
 */
@Deprecated
public class FSTLookupTest extends LuceneTestCase {
  public static TermFreq tf(String t, long v) {
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
        tf("one", 1),
        tf("oneness", 2),
        tf("onerous", 2),
        tf("onesimus", 2),
        tf("two", 2),
        tf("twofold", 2),
        tf("twonk", 2),
        tf("thrive", 2),
        tf("through", 2),
        tf("threat", 2),
        tf("three", 2),
        tf("foundation", 2),
        tf("fourblah", 2),
        tf("fourteen", 2),
        tf("four", 1),
        tf("fourier", 1),
        tf("fourty", 1),
        tf("xo", 1),
      };
    return keys;
  }

  public void testExactMatchHighPriority() throws Exception {
    assertMatchEquals(lookup.lookup("two", true, 1), "two/1");
  }

  public void testExactMatchLowPriority() throws Exception {
    assertMatchEquals(lookup.lookup("one", true, 2), 
        "one/0",
        "oneness/1");
  }

  public void testRequestedCount() throws Exception {
    // 'one' is promoted after collecting two higher ranking results.
    assertMatchEquals(lookup.lookup("one", true, 2), 
        "one/0", 
        "oneness/1");

    // 'one' is at the top after collecting all alphabetical results. 
    assertMatchEquals(lookup.lookup("one", false, 2), 
        "one/0", 
        "oneness/1");

    // 'four' is collected in a bucket and then again as an exact match. 
    assertMatchEquals(lookup.lookup("four", true, 2), 
        "four/0", 
        "fourblah/1");

    // Check reordering of exact matches. 
    assertMatchEquals(lookup.lookup("four", true, 4), 
        "four/0",
        "fourblah/1",
        "fourteen/1",
        "fourier/0");

    lookup = new FSTLookup(10, false);
    lookup.build(new TermFreqArrayIterator(evalKeys()));
    
    // 'one' is not promoted after collecting two higher ranking results.
    assertMatchEquals(lookup.lookup("one", true, 2),  
        "oneness/1",
        "onerous/1");

    // 'one' is at the top after collecting all alphabetical results. 
    assertMatchEquals(lookup.lookup("one", false, 2), 
        "one/0", 
        "oneness/1");
  }

  public void testMiss() throws Exception {
    assertMatchEquals(lookup.lookup("xyz", true, 1));
  }

  public void testAlphabeticWithWeights() throws Exception {
    assertEquals(0, lookup.lookup("xyz", false, 1).size());
  }

  public void testFullMatchList() throws Exception {
    assertMatchEquals(lookup.lookup("one", true, Integer.MAX_VALUE),
        "oneness/1", 
        "onerous/1",
        "onesimus/1", 
        "one/0");
  }

  public void testMultilingualInput() throws Exception {
    List<TermFreq> input = LookupBenchmarkTest.readTop50KWiki();

    lookup = new FSTLookup();
    lookup.build(new TermFreqArrayIterator(input));

    for (TermFreq tf : input) {
      assertTrue("Not found: " + tf.term, lookup.get(_TestUtil.bytesToCharSequence(tf.term, random)) != null);
      assertEquals(tf.term.utf8ToString(), lookup.lookup(_TestUtil.bytesToCharSequence(tf.term, random), true, 1).get(0).key.toString());
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
      final CharSequence term = _TestUtil.bytesToCharSequence(tf.term, random);
      for (int i = 1; i < term.length(); i++) {
        CharSequence prefix = term.subSequence(0, i);
        for (LookupResult lr : lookup.lookup(prefix, true, 10)) {
          assertTrue(lr.key.toString().startsWith(prefix.toString()));
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
