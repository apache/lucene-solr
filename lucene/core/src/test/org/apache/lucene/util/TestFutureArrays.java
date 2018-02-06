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
package org.apache.lucene.util;

import java.nio.charset.StandardCharsets;

/** Test java 8-compatible implementations of {@code java.util.Arrays} methods */
public class TestFutureArrays extends LuceneTestCase {
  
  public void testByteMismatch() {
    assertEquals(1, FutureArrays.mismatch(bytes("ab"), 0, 2, bytes("ac"), 0, 2));
    assertEquals(0, FutureArrays.mismatch(bytes("ab"), 0, 2, bytes("b"), 0, 1));
    assertEquals(-1, FutureArrays.mismatch(bytes("ab"), 0, 2, bytes("ab"), 0, 2));
    assertEquals(1, FutureArrays.mismatch(bytes("ab"), 0, 2, bytes("a"), 0, 1));
    expectThrows(IllegalArgumentException.class, () -> {
      FutureArrays.mismatch(bytes("ab"), 2, 1, bytes("a"), 0, 1);
    });
    expectThrows(IllegalArgumentException.class, () -> {
      FutureArrays.mismatch(bytes("ab"), 2, 1, bytes("a"), 1, 0);
    });
    expectThrows(NullPointerException.class, () -> {
      FutureArrays.mismatch(null, 0, 2, bytes("a"), 0, 1);
    });
    expectThrows(NullPointerException.class, () -> {
      FutureArrays.mismatch(bytes("ab"), 0, 2, null, 0, 1);
    });
    expectThrows(IndexOutOfBoundsException.class, () -> {
      FutureArrays.mismatch(bytes("ab"), 0, 3, bytes("a"), 0, 1);
    });
    expectThrows(IndexOutOfBoundsException.class, () -> {
      FutureArrays.mismatch(bytes("ab"), 0, 2, bytes("a"), 0, 2);
    });
  }
  
  public void testCharMismatch() {
    assertEquals(1, FutureArrays.mismatch(chars("ab"), 0, 2, chars("ac"), 0, 2));
    assertEquals(0, FutureArrays.mismatch(chars("ab"), 0, 2, chars("b"), 0, 1));
    assertEquals(-1, FutureArrays.mismatch(chars("ab"), 0, 2, chars("ab"), 0, 2));
    assertEquals(1, FutureArrays.mismatch(chars("ab"), 0, 2, chars("a"), 0, 1));
    expectThrows(IllegalArgumentException.class, () -> {
      FutureArrays.mismatch(chars("ab"), 2, 1, chars("a"), 0, 1);
    });
    expectThrows(IllegalArgumentException.class, () -> {
      FutureArrays.mismatch(chars("ab"), 2, 1, chars("a"), 1, 0);
    });
    expectThrows(NullPointerException.class, () -> {
      FutureArrays.mismatch(null, 0, 2, chars("a"), 0, 1);
    });
    expectThrows(NullPointerException.class, () -> {
      FutureArrays.mismatch(chars("ab"), 0, 2, null, 0, 1);
    });
    expectThrows(IndexOutOfBoundsException.class, () -> {
      FutureArrays.mismatch(chars("ab"), 0, 3, chars("a"), 0, 1);
    });
    expectThrows(IndexOutOfBoundsException.class, () -> {
      FutureArrays.mismatch(chars("ab"), 0, 2, chars("a"), 0, 2);
    });
  }
  
  public void testByteCompareUnsigned() {
    assertEquals(1, Integer.signum(FutureArrays.compareUnsigned(bytes("ab"), 0, 2, bytes("a"), 0, 1)));
    assertEquals(1, Integer.signum(FutureArrays.compareUnsigned(bytes("ab"), 0, 2, bytes("aa"), 0, 2)));
    assertEquals(0, Integer.signum(FutureArrays.compareUnsigned(bytes("ab"), 0, 2, bytes("ab"), 0, 2)));
    assertEquals(-1, Integer.signum(FutureArrays.compareUnsigned(bytes("a"), 0, 1, bytes("ab"), 0, 2)));

    expectThrows(IllegalArgumentException.class, () -> {
      FutureArrays.compareUnsigned(bytes("ab"), 2, 1, bytes("a"), 0, 1);
    });
    expectThrows(IllegalArgumentException.class, () -> {
      FutureArrays.compareUnsigned(bytes("ab"), 2, 1, bytes("a"), 1, 0);
    });
    expectThrows(NullPointerException.class, () -> {
      FutureArrays.compareUnsigned(null, 0, 2, bytes("a"), 0, 1);
    });
    expectThrows(NullPointerException.class, () -> {
      FutureArrays.compareUnsigned(bytes("ab"), 0, 2, null, 0, 1);
    });
    expectThrows(IndexOutOfBoundsException.class, () -> {
      FutureArrays.compareUnsigned(bytes("ab"), 0, 3, bytes("a"), 0, 1);
    });
    expectThrows(IndexOutOfBoundsException.class, () -> {
      FutureArrays.compareUnsigned(bytes("ab"), 0, 2, bytes("a"), 0, 2);
    });
  }
  
  public void testCharCompare() {
    assertEquals(1, Integer.signum(FutureArrays.compare(chars("ab"), 0, 2, chars("a"), 0, 1)));
    assertEquals(1, Integer.signum(FutureArrays.compare(chars("ab"), 0, 2, chars("aa"), 0, 2)));
    assertEquals(0, Integer.signum(FutureArrays.compare(chars("ab"), 0, 2, chars("ab"), 0, 2)));
    assertEquals(-1, Integer.signum(FutureArrays.compare(chars("a"), 0, 1, chars("ab"), 0, 2)));

    expectThrows(IllegalArgumentException.class, () -> {
      FutureArrays.compare(chars("ab"), 2, 1, chars("a"), 0, 1);
    });
    expectThrows(IllegalArgumentException.class, () -> {
      FutureArrays.compare(chars("ab"), 2, 1, chars("a"), 1, 0);
    });
    expectThrows(NullPointerException.class, () -> {
      FutureArrays.compare(null, 0, 2, chars("a"), 0, 1);
    });
    expectThrows(NullPointerException.class, () -> {
      FutureArrays.compare(chars("ab"), 0, 2, null, 0, 1);
    });
    expectThrows(IndexOutOfBoundsException.class, () -> {
      FutureArrays.compare(chars("ab"), 0, 3, chars("a"), 0, 1);
    });
    expectThrows(IndexOutOfBoundsException.class, () -> {
      FutureArrays.compare(chars("ab"), 0, 2, chars("a"), 0, 2);
    });
  }
  
  public void testIntCompare() {
    assertEquals(1, Integer.signum(FutureArrays.compare(ints("ab"), 0, 2, ints("a"), 0, 1)));
    assertEquals(1, Integer.signum(FutureArrays.compare(ints("ab"), 0, 2, ints("aa"), 0, 2)));
    assertEquals(0, Integer.signum(FutureArrays.compare(ints("ab"), 0, 2, ints("ab"), 0, 2)));
    assertEquals(-1, Integer.signum(FutureArrays.compare(ints("a"), 0, 1, ints("ab"), 0, 2)));

    expectThrows(IllegalArgumentException.class, () -> {
      FutureArrays.compare(ints("ab"), 2, 1, ints("a"), 0, 1);
    });
    expectThrows(IllegalArgumentException.class, () -> {
      FutureArrays.compare(ints("ab"), 2, 1, ints("a"), 1, 0);
    });
    expectThrows(NullPointerException.class, () -> {
      FutureArrays.compare(null, 0, 2, ints("a"), 0, 1);
    });
    expectThrows(NullPointerException.class, () -> {
      FutureArrays.compare(ints("ab"), 0, 2, null, 0, 1);
    });
    expectThrows(IndexOutOfBoundsException.class, () -> {
      FutureArrays.compare(ints("ab"), 0, 3, ints("a"), 0, 1);
    });
    expectThrows(IndexOutOfBoundsException.class, () -> {
      FutureArrays.compare(ints("ab"), 0, 2, ints("a"), 0, 2);
    });
  }
  
  public void testLongCompare() {
    assertEquals(1, Integer.signum(FutureArrays.compare(longs("ab"), 0, 2, longs("a"), 0, 1)));
    assertEquals(1, Integer.signum(FutureArrays.compare(longs("ab"), 0, 2, longs("aa"), 0, 2)));
    assertEquals(0, Integer.signum(FutureArrays.compare(longs("ab"), 0, 2, longs("ab"), 0, 2)));
    assertEquals(-1, Integer.signum(FutureArrays.compare(longs("a"), 0, 1, longs("ab"), 0, 2)));

    expectThrows(IllegalArgumentException.class, () -> {
      FutureArrays.compare(longs("ab"), 2, 1, longs("a"), 0, 1);
    });
    expectThrows(IllegalArgumentException.class, () -> {
      FutureArrays.compare(longs("ab"), 2, 1, longs("a"), 1, 0);
    });
    expectThrows(NullPointerException.class, () -> {
      FutureArrays.compare(null, 0, 2, longs("a"), 0, 1);
    });
    expectThrows(NullPointerException.class, () -> {
      FutureArrays.compare(longs("ab"), 0, 2, null, 0, 1);
    });
    expectThrows(IndexOutOfBoundsException.class, () -> {
      FutureArrays.compare(longs("ab"), 0, 3, longs("a"), 0, 1);
    });
    expectThrows(IndexOutOfBoundsException.class, () -> {
      FutureArrays.compare(longs("ab"), 0, 2, longs("a"), 0, 2);
    });
  }
  
  public void testByteEquals() {
    assertFalse(FutureArrays.equals(bytes("ab"), 0, 2, bytes("a"), 0, 1));
    assertFalse(FutureArrays.equals(bytes("ab"), 0, 2, bytes("aa"), 0, 2));
    assertTrue(FutureArrays.equals(bytes("ab"), 0, 2, bytes("ab"), 0, 2));
    assertFalse(FutureArrays.equals(bytes("a"), 0, 1, bytes("ab"), 0, 2));

    expectThrows(IllegalArgumentException.class, () -> {
      FutureArrays.equals(bytes("ab"), 2, 1, bytes("a"), 0, 1);
    });
    expectThrows(IllegalArgumentException.class, () -> {
      FutureArrays.equals(bytes("ab"), 2, 1, bytes("a"), 1, 0);
    });
    expectThrows(NullPointerException.class, () -> {
      FutureArrays.equals(null, 0, 2, bytes("a"), 0, 1);
    });
    expectThrows(NullPointerException.class, () -> {
      FutureArrays.equals(bytes("ab"), 0, 2, null, 0, 1);
    });
    expectThrows(IndexOutOfBoundsException.class, () -> {
      FutureArrays.equals(bytes("ab"), 0, 3, bytes("a"), 0, 1);
    });
    expectThrows(IndexOutOfBoundsException.class, () -> {
      FutureArrays.equals(bytes("ab"), 0, 2, bytes("a"), 0, 2);
    });
  }
  
  public void testCharEquals() {
    assertFalse(FutureArrays.equals(chars("ab"), 0, 2, chars("a"), 0, 1));
    assertFalse(FutureArrays.equals(chars("ab"), 0, 2, chars("aa"), 0, 2));
    assertTrue(FutureArrays.equals(chars("ab"), 0, 2, chars("ab"), 0, 2));
    assertFalse(FutureArrays.equals(chars("a"), 0, 1, chars("ab"), 0, 2));

    expectThrows(IllegalArgumentException.class, () -> {
      FutureArrays.equals(chars("ab"), 2, 1, chars("a"), 0, 1);
    });
    expectThrows(IllegalArgumentException.class, () -> {
      FutureArrays.equals(chars("ab"), 2, 1, chars("a"), 1, 0);
    });
    expectThrows(NullPointerException.class, () -> {
      FutureArrays.equals(null, 0, 2, chars("a"), 0, 1);
    });
    expectThrows(NullPointerException.class, () -> {
      FutureArrays.equals(chars("ab"), 0, 2, null, 0, 1);
    });
    expectThrows(IndexOutOfBoundsException.class, () -> {
      FutureArrays.equals(chars("ab"), 0, 3, chars("a"), 0, 1);
    });
    expectThrows(IndexOutOfBoundsException.class, () -> {
      FutureArrays.equals(chars("ab"), 0, 2, chars("a"), 0, 2);
    });
  }
  
  public void testIntEquals() {
    assertFalse(FutureArrays.equals(ints("ab"), 0, 2, ints("a"), 0, 1));
    assertFalse(FutureArrays.equals(ints("ab"), 0, 2, ints("aa"), 0, 2));
    assertTrue(FutureArrays.equals(ints("ab"), 0, 2, ints("ab"), 0, 2));
    assertFalse(FutureArrays.equals(ints("a"), 0, 1, ints("ab"), 0, 2));

    expectThrows(IllegalArgumentException.class, () -> {
      FutureArrays.equals(ints("ab"), 2, 1, ints("a"), 0, 1);
    });
    expectThrows(IllegalArgumentException.class, () -> {
      FutureArrays.equals(ints("ab"), 2, 1, ints("a"), 1, 0);
    });
    expectThrows(NullPointerException.class, () -> {
      FutureArrays.equals(null, 0, 2, ints("a"), 0, 1);
    });
    expectThrows(NullPointerException.class, () -> {
      FutureArrays.equals(ints("ab"), 0, 2, null, 0, 1);
    });
    expectThrows(IndexOutOfBoundsException.class, () -> {
      FutureArrays.equals(ints("ab"), 0, 3, ints("a"), 0, 1);
    });
    expectThrows(IndexOutOfBoundsException.class, () -> {
      FutureArrays.equals(ints("ab"), 0, 2, ints("a"), 0, 2);
    });
  }
  
  public void testLongEquals() {
    assertFalse(FutureArrays.equals(longs("ab"), 0, 2, longs("a"), 0, 1));
    assertFalse(FutureArrays.equals(longs("ab"), 0, 2, longs("aa"), 0, 2));
    assertTrue(FutureArrays.equals(longs("ab"), 0, 2, longs("ab"), 0, 2));
    assertFalse(FutureArrays.equals(longs("a"), 0, 1, longs("ab"), 0, 2));

    expectThrows(IllegalArgumentException.class, () -> {
      FutureArrays.equals(longs("ab"), 2, 1, longs("a"), 0, 1);
    });
    expectThrows(IllegalArgumentException.class, () -> {
      FutureArrays.equals(longs("ab"), 2, 1, longs("a"), 1, 0);
    });
    expectThrows(NullPointerException.class, () -> {
      FutureArrays.equals(null, 0, 2, longs("a"), 0, 1);
    });
    expectThrows(NullPointerException.class, () -> {
      FutureArrays.equals(longs("ab"), 0, 2, null, 0, 1);
    });
    expectThrows(IndexOutOfBoundsException.class, () -> {
      FutureArrays.equals(longs("ab"), 0, 3, longs("a"), 0, 1);
    });
    expectThrows(IndexOutOfBoundsException.class, () -> {
      FutureArrays.equals(longs("ab"), 0, 2, longs("a"), 0, 2);
    });
  }
  
  private byte[] bytes(String s) {
    return s.getBytes(StandardCharsets.UTF_8);
  }
  
  private char[] chars(String s) {
    return s.toCharArray();
  }
  
  private int[] ints(String s) {
    int ints[] = new int[s.length()];
    for (int i = 0; i < s.length(); i++) {
      ints[i] = s.charAt(i);
    }
    return ints;
  }
  
  private long[] longs(String s) {
    long longs[] = new long[s.length()];
    for (int i = 0; i < s.length(); i++) {
      longs[i] = s.charAt(i);
    }
    return longs;
  }
}
