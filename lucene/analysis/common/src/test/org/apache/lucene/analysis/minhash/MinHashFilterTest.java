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

package org.apache.lucene.analysis.minhash;

import java.io.IOException;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.minhash.MinHashFilter.FixedSizeTreeSet;
import org.apache.lucene.analysis.minhash.MinHashFilter.LongPair;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.apache.lucene.util.automaton.RegExp;
import org.junit.Test;

/**
 * Tests for {@link MinHashFilter}
 */
public class MinHashFilterTest extends BaseTokenStreamTestCase {

  @Test
  public void testIntHash() {
    LongPair hash = new LongPair();
    MinHashFilter.murmurhash3_x64_128(MinHashFilter.getBytes(0), 0, 4, 0, hash);
    assertEquals(-3485513579396041028L, hash.val1);
    assertEquals(6383328099726337777L, hash.val2);
  }

  @Test
  public void testStringHash() throws UnsupportedEncodingException {
    LongPair hash = new LongPair();
    byte[] bytes = "woof woof woof woof woof".getBytes("UTF-16LE");
    MinHashFilter.murmurhash3_x64_128(bytes, 0, bytes.length, 0, hash);
    assertEquals(7638079586852243959L, hash.val1);
    assertEquals(4378804943379391304L, hash.val2);
  }

  @Test
  public void testSimpleOrder() throws UnsupportedEncodingException {
    LongPair hash1 = new LongPair();
    hash1.val1 = 1;
    hash1.val2 = 2;
    LongPair hash2 = new LongPair();
    hash2.val1 = 2;
    hash2.val2 = 1;
    assert (hash1.compareTo(hash2) > 0);
  }

  @Test
  public void testHashOrder() {
    assertTrue(!MinHashFilter.isLessThanUnsigned(0L, 0L));
    assertTrue(MinHashFilter.isLessThanUnsigned(0L, -1L));
    assertTrue(MinHashFilter.isLessThanUnsigned(1L, -1L));
    assertTrue(MinHashFilter.isLessThanUnsigned(-2L, -1L));
    assertTrue(MinHashFilter.isLessThanUnsigned(1L, 2L));
    assertTrue(MinHashFilter.isLessThanUnsigned(Long.MAX_VALUE, Long.MIN_VALUE));

    FixedSizeTreeSet<LongPair> minSet = new FixedSizeTreeSet<LongPair>(500);
    HashSet<LongPair> unadded = new HashSet<LongPair>();
    for (int i = 0; i < 100; i++) {
      LongPair hash = new LongPair();
      MinHashFilter.murmurhash3_x64_128(MinHashFilter.getBytes(i), 0, 4, 0, hash);
      LongPair peek = null;
      if (minSet.size() > 0) {
        peek = minSet.last();
      }

      if (!minSet.add(hash)) {
        unadded.add(hash);
      } else {
        if (peek != null) {
          if ((minSet.size() == 500) && !peek.equals(minSet.last())) {
            unadded.add(peek);
          }
        }
      }
    }
    assertEquals(100, minSet.size());
    assertEquals(0, unadded.size());

    HashSet<LongPair> collisionDetection = new HashSet<LongPair>();
    unadded = new HashSet<LongPair>();
    minSet = new FixedSizeTreeSet<LongPair>(500);
    for (int i = 0; i < 1000000; i++) {
      LongPair hash = new LongPair();
      MinHashFilter.murmurhash3_x64_128(MinHashFilter.getBytes(i), 0, 4, 0, hash);
      collisionDetection.add(hash);
      LongPair peek = null;
      if (minSet.size() > 0) {
        peek = minSet.last();
      }

      if (!minSet.add(hash)) {
        unadded.add(hash);
      } else {
        if (peek != null) {
          if ((minSet.size() == 500) && !peek.equals(minSet.last())) {
            unadded.add(peek);
          }
        }
      }
    }
    assertEquals(1000000, collisionDetection.size());
    assertEquals(500, minSet.size());
    assertEquals(999500, unadded.size());

    LongPair last = null;
    LongPair current = null;
    while ((current = minSet.pollLast()) != null) {
      if (last != null) {
        assertTrue(isLessThan(current, last));
      }
      last = current;
    }
  }

  @Test
  public void testHashNotRepeated() {
    FixedSizeTreeSet<LongPair> minSet = new FixedSizeTreeSet<LongPair>(500);
    HashSet<LongPair> unadded = new HashSet<LongPair>();
    for (int i = 0; i < 10000; i++) {
      LongPair hash = new LongPair();
      MinHashFilter.murmurhash3_x64_128(MinHashFilter.getBytes(i), 0, 4, 0, hash);
      LongPair peek = null;
      if (minSet.size() > 0) {
        peek = minSet.last();
      }
      if (!minSet.add(hash)) {
        unadded.add(hash);
      } else {
        if (peek != null) {
          if ((minSet.size() == 500) && !peek.equals(minSet.last())) {
            unadded.add(peek);
          }
        }
      }
    }
    assertEquals(500, minSet.size());

    LongPair last = null;
    LongPair current = null;
    while ((current = minSet.pollLast()) != null) {
      if (last != null) {
        assertTrue(isLessThan(current, last));
      }
      last = current;
    }
  }

  @Test
  public void testMockShingleTokenizer() throws IOException {
    Tokenizer mockShingleTokenizer = createMockShingleTokenizer(5,
        "woof woof woof woof woof" + " " + "woof woof woof woof puff");
    assertTokenStreamContents(mockShingleTokenizer,
        new String[]{"woof woof woof woof woof", "woof woof woof woof puff"});
  }

  @Test
  public void testTokenStreamSingleInput() throws IOException {
    String[] hashes = new String[]{"℁팽徭聙↝ꇁ홱杯"};
    TokenStream ts = createTokenStream(5, "woof woof woof woof woof", 1, 1, 100, false);
    assertTokenStreamContents(ts, hashes, new int[]{0},
        new int[]{24}, new String[]{MinHashFilter.MIN_HASH_TYPE}, new int[]{1}, new int[]{1}, 24, 0, null,
        true);

    ts = createTokenStream(5, "woof woof woof woof woof", 2, 1, 1, false);
    assertTokenStreamContents(ts, new String[]{new String(new char[]{0, 0, 8449, 54077, 64133, 32857, 8605, 41409}),
            new String(new char[]{0, 1, 16887, 58164, 39536, 14926, 6529, 17276})}, new int[]{0, 0},
        new int[]{24, 24}, new String[]{MinHashFilter.MIN_HASH_TYPE, MinHashFilter.MIN_HASH_TYPE}, new int[]{1, 0},
        new int[]{1, 1}, 24, 0, null,
        true);
  }

  @Test
  public void testTokenStream1() throws IOException {
    String[] hashes = new String[]{"℁팽徭聙↝ꇁ홱杯",
        new String(new char[]{36347, 63457, 43013, 56843, 52284, 34231, 57934, 42302})}; // String is degenerate as
    // characters!

    TokenStream ts = createTokenStream(5, "woof woof woof woof woof" + " " + "woof woof woof woof puff", 1, 1, 100,
        false);
    assertTokenStreamContents(ts, hashes, new int[]{0, 0},
        new int[]{49, 49}, new String[]{MinHashFilter.MIN_HASH_TYPE, MinHashFilter.MIN_HASH_TYPE}, new int[]{1, 0},
        new int[]{1, 1}, 49, 0, null, true);
  }

  private ArrayList<String> getTokens(TokenStream ts) throws IOException {
    ArrayList<String> tokens = new ArrayList<>();
    ts.reset();
    while (ts.incrementToken()) {
      CharTermAttribute termAttribute = ts.getAttribute(CharTermAttribute.class);
      String token = new String(termAttribute.buffer(), 0, termAttribute.length());
      tokens.add(token);
    }
    ts.end();
    ts.close();

    return tokens;
  }

  @Test
  public void testTokenStream2() throws IOException {
    TokenStream ts = createTokenStream(5, "woof woof woof woof woof" + " " + "woof woof woof woof puff", 100, 1, 1,
        false);
    ArrayList<String> tokens = getTokens(ts);
    ts.close();

    assertEquals(100, tokens.size());
  }

  @Test
  public void testTokenStream3() throws IOException {
    TokenStream ts = createTokenStream(5, "woof woof woof woof woof" + " " + "woof woof woof woof puff", 10, 1, 10,
        false);
    ArrayList<String> tokens = getTokens(ts);
    ts.close();

    assertEquals(20, tokens.size());
  }

  @Test
  public void testTokenStream4() throws IOException {
    TokenStream ts = createTokenStream(5, "woof woof woof woof woof" + " " + "woof woof woof woof puff", 10, 10, 1,
        false);
    ArrayList<String> tokens = getTokens(ts);
    ts.close();

    assertEquals(20, tokens.size());

    ts = createTokenStream(5, "woof woof woof woof woof" + " " + "woof woof woof woof puff", 10, 10, 1, true);
    tokens = getTokens(ts);
    ts.close();

    assertEquals(100, tokens.size());

  }

  @Test
  public void testTokenStream5() throws IOException {
    TokenStream ts = createTokenStream(5, "woof woof woof woof woof" + " " + "woof woof woof woof puff", 1, 100, 1,
        false);
    ArrayList<String> tokens = getTokens(ts);
    ts.close();

    assertEquals(2, tokens.size());

    ts = createTokenStream(5, "woof woof woof woof woof" + " " + "woof woof woof woof puff", 1, 100, 1, true);
    tokens = getTokens(ts);
    ts.close();

    assertEquals(100, tokens.size());
    HashSet<String> set = new HashSet<>(tokens);
    assertEquals(2, set.size());

    boolean rolled = false;
    String first = null;
    String last = null;
    for (String current : tokens) {
      if (first == null) {
        first = current;
      }
      if (last != null) {
        if (!rolled) {
          if (current.compareTo(last) >= 0) {
            // fine
          } else if (current.equals(first)) {
            rolled = true;
          } else {
            fail("Incorrect hash order");
          }
        } else {
          if (!current.equals(first)) {
            fail("Incorrect hash order");
          }
        }
      }
      last = current;
    }

  }

  public static TokenStream createTokenStream(int shingleSize, String shingles, int hashCount, int bucketCount,
                                              int hashSetSize, boolean withRotation) {
    Tokenizer tokenizer = createMockShingleTokenizer(shingleSize, shingles);
    HashMap<String, String> lshffargs = new HashMap<>();
    lshffargs.put("hashCount", "" + hashCount);
    lshffargs.put("bucketCount", "" + bucketCount);
    lshffargs.put("hashSetSize", "" + hashSetSize);
    lshffargs.put("withRotation", "" + withRotation);
    MinHashFilterFactory lshff = new MinHashFilterFactory(lshffargs);
    return lshff.create(tokenizer);
  }

  private static Tokenizer createMockShingleTokenizer(int shingleSize, String shingles) {
    MockTokenizer tokenizer = new MockTokenizer(
        new CharacterRunAutomaton(new RegExp("[^ \t\r\n]+([ \t\r\n]+[^ \t\r\n]+){" + (shingleSize - 1) + "}").toAutomaton()),
        true);
    tokenizer.setEnableChecks(true);
    if (shingles != null) {
      tokenizer.setReader(new StringReader(shingles));
    }
    return tokenizer;
  }

  private boolean isLessThan(LongPair hash1, LongPair hash2) {
    return MinHashFilter.isLessThanUnsigned(hash1.val2, hash2.val2) || hash1.val2 == hash2.val2 && (MinHashFilter.isLessThanUnsigned(hash1.val1, hash2.val1));
  }
}
