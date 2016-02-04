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
package org.apache.lucene.search.suggest.fst;

import java.nio.charset.StandardCharsets;
import java.util.*;

import org.apache.lucene.search.suggest.Lookup.LookupResult;
import org.apache.lucene.search.suggest.*;
import org.apache.lucene.search.suggest.fst.FSTCompletion.Completion;
import org.apache.lucene.util.*;

/**
 * Unit tests for {@link FSTCompletion}.
 */
public class FSTCompletionTest extends LuceneTestCase {
  public static Input tf(String t, int v) {
    return new Input(t, v);
  }

  private FSTCompletion completion;
  private FSTCompletion completionAlphabetical;

  @Override
  public void setUp() throws Exception {
    super.setUp();

    FSTCompletionBuilder builder = new FSTCompletionBuilder();
    for (Input tf : evalKeys()) {
      builder.add(tf.term, (int) tf.v);
    }
    completion = builder.build();
    completionAlphabetical = new FSTCompletion(completion.getFST(), false, true);
  }

  private Input[] evalKeys() {
    final Input[] keys = new Input[] {
        tf("one", 0),
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
        tf("fourblah", 1),
        tf("fourteen", 1),
        tf("four", 0),
        tf("fourier", 0),
        tf("fourty", 0),
        tf("xo", 1),
      };
    return keys;
  }

  public void testExactMatchHighPriority() throws Exception {
    assertMatchEquals(completion.lookup(stringToCharSequence("two"), 1),
        "two/1.0");
  }

  public void testExactMatchLowPriority() throws Exception {
    assertMatchEquals(completion.lookup(stringToCharSequence("one"), 2), 
        "one/0.0",
        "oneness/1.0");
  }
  
  public void testExactMatchReordering() throws Exception {
    // Check reordering of exact matches. 
    assertMatchEquals(completion.lookup(stringToCharSequence("four"), 4), 
        "four/0.0",
        "fourblah/1.0",
        "fourteen/1.0",
        "fourier/0.0");    
  }

  public void testRequestedCount() throws Exception {
    // 'one' is promoted after collecting two higher ranking results.
    assertMatchEquals(completion.lookup(stringToCharSequence("one"), 2), 
        "one/0.0", 
        "oneness/1.0");

    // 'four' is collected in a bucket and then again as an exact match. 
    assertMatchEquals(completion.lookup(stringToCharSequence("four"), 2), 
        "four/0.0", 
        "fourblah/1.0");

    // Check reordering of exact matches. 
    assertMatchEquals(completion.lookup(stringToCharSequence("four"), 4), 
        "four/0.0",
        "fourblah/1.0",
        "fourteen/1.0",
        "fourier/0.0");

    // 'one' is at the top after collecting all alphabetical results.
    assertMatchEquals(completionAlphabetical.lookup(stringToCharSequence("one"), 2), 
        "one/0.0", 
        "oneness/1.0");
    
    // 'one' is not promoted after collecting two higher ranking results.
    FSTCompletion noPromotion = new FSTCompletion(completion.getFST(), true, false);
    assertMatchEquals(noPromotion.lookup(stringToCharSequence("one"), 2),  
        "oneness/1.0",
        "onerous/1.0");

    // 'one' is at the top after collecting all alphabetical results. 
    assertMatchEquals(completionAlphabetical.lookup(stringToCharSequence("one"), 2), 
        "one/0.0", 
        "oneness/1.0");
  }

  public void testMiss() throws Exception {
    assertMatchEquals(completion.lookup(stringToCharSequence("xyz"), 1));
  }

  public void testAlphabeticWithWeights() throws Exception {
    assertEquals(0, completionAlphabetical.lookup(stringToCharSequence("xyz"), 1).size());
  }

  public void testFullMatchList() throws Exception {
    assertMatchEquals(completion.lookup(stringToCharSequence("one"), Integer.MAX_VALUE),
        "oneness/1.0", 
        "onerous/1.0",
        "onesimus/1.0", 
        "one/0.0");
  }

  public void testThreeByte() throws Exception {
    String key = new String(new byte[] {
        (byte) 0xF0, (byte) 0xA4, (byte) 0xAD, (byte) 0xA2}, StandardCharsets.UTF_8);
    FSTCompletionBuilder builder = new FSTCompletionBuilder();
    builder.add(new BytesRef(key), 0);

    FSTCompletion lookup = builder.build();
    List<Completion> result = lookup.lookup(stringToCharSequence(key), 1);
    assertEquals(1, result.size());
  }

  public void testLargeInputConstantWeights() throws Exception {
    FSTCompletionLookup lookup = new FSTCompletionLookup(10, true);
    
    Random r = random();
    List<Input> keys = new ArrayList<>();
    for (int i = 0; i < 5000; i++) {
      keys.add(new Input(TestUtil.randomSimpleString(r), -1));
    }

    lookup.build(new InputArrayIterator(keys));

    // All the weights were constant, so all returned buckets must be constant, whatever they
    // are.
    Long previous = null; 
    for (Input tf : keys) {
      Long current = ((Number)lookup.get(TestUtil.bytesToCharSequence(tf.term, random()))).longValue();
      if (previous != null) {
        assertEquals(previous, current);
      }
      previous = current;
    }
  }  

  public void testMultilingualInput() throws Exception {
    List<Input> input = LookupBenchmarkTest.readTop50KWiki();

    FSTCompletionLookup lookup = new FSTCompletionLookup();
    lookup.build(new InputArrayIterator(input));
    assertEquals(input.size(), lookup.getCount());
    for (Input tf : input) {
      assertNotNull("Not found: " + tf.term.toString(), lookup.get(TestUtil.bytesToCharSequence(tf.term, random())));
      assertEquals(tf.term.utf8ToString(), lookup.lookup(TestUtil.bytesToCharSequence(tf.term, random()), true, 1).get(0).key.toString());
    }

    List<LookupResult> result = lookup.lookup(stringToCharSequence("wit"), true, 5);
    assertEquals(5, result.size());
    assertTrue(result.get(0).key.toString().equals("wit"));  // exact match.
    assertTrue(result.get(1).key.toString().equals("with")); // highest count.
  }

  public void testEmptyInput() throws Exception {
    completion = new FSTCompletionBuilder().build();
    assertMatchEquals(completion.lookup(stringToCharSequence(""), 10));
  }

  public void testRandom() throws Exception {
    List<Input> freqs = new ArrayList<>();
    Random rnd = random();
    for (int i = 0; i < 2500 + rnd.nextInt(2500); i++) {
      int weight = rnd.nextInt(100); 
      freqs.add(new Input("" + rnd.nextLong(), weight));
    }

    FSTCompletionLookup lookup = new FSTCompletionLookup();
    lookup.build(new InputArrayIterator(freqs.toArray(new Input[freqs.size()])));

    for (Input tf : freqs) {
      final String term = tf.term.utf8ToString();
      for (int i = 1; i < term.length(); i++) {
        String prefix = term.substring(0, i);
        for (LookupResult lr : lookup.lookup(stringToCharSequence(prefix), true, 10)) {
          assertTrue(lr.key.toString().startsWith(prefix));
        }
      }
    }
  }

  private CharSequence stringToCharSequence(String prefix) {
    return TestUtil.stringToCharSequence(prefix, random());
  }

  private void assertMatchEquals(List<Completion> res, String... expected) {
    String [] result = new String [res.size()];
    for (int i = 0; i < res.size(); i++) {
      result[i] = res.get(i).toString();
    }

    if (!Arrays.equals(stripScore(expected), stripScore(result))) {
      int colLen = Math.max(maxLen(expected), maxLen(result));
      
      StringBuilder b = new StringBuilder();
      String format = "%" + colLen + "s  " + "%" + colLen + "s\n"; 
      b.append(String.format(Locale.ROOT, format, "Expected", "Result"));
      for (int i = 0; i < Math.max(result.length, expected.length); i++) {
        b.append(String.format(Locale.ROOT, format, 
            i < expected.length ? expected[i] : "--", 
            i < result.length ? result[i] : "--"));
      }

      System.err.println(b.toString());
      fail("Expected different output:\n" + b.toString());
    }
  }

  private String[] stripScore(String[] expected) {
    String [] result = new String [expected.length];
    for (int i = 0; i < result.length; i++) {
      result[i] = expected[i].replaceAll("\\/[0-9\\.]+", "");
    }
    return result;
  }

  private int maxLen(String[] result) {
    int len = 0;
    for (String s : result)
      len = Math.max(len, s.length());
    return len;
  }
}
