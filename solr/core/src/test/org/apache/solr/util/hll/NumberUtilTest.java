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

package org.apache.solr.util.hll;

import org.apache.lucene.util.LuceneTestCase;
import org.junit.Test;

/**
 * Unit tests for {@link NumberUtil}.
 */
public class NumberUtilTest extends LuceneTestCase {
  /**
   * Tests {@link NumberUtil#log2(double)}
   */
  @Test
  public void testLog2() {
    // Special cases
    assertEquals(Double.NaN, NumberUtil.log2(-1), 0);
    assertEquals(Double.NEGATIVE_INFINITY, NumberUtil.log2(0), 0);

    // Normal cases
    assertEquals(0, NumberUtil.log2(1), 0);
    assertEquals(1, NumberUtil.log2(2), 0);
    assertEquals(2, NumberUtil.log2(4), 0);
    assertEquals(1.5849625007211562, NumberUtil.log2(3), 0.000000000000001);
    assertEquals(4, NumberUtil.log2(16), 0);
    assertEquals(10, NumberUtil.log2(1024), 0);
  }

  /**
   * Tests {@link NumberUtil#fromHex(String, int, int)} edge cases
   */
  @Test
  public void testFromHexEdgeCases() {
    try {
      NumberUtil.fromHex(null, 0, 0);
      fail("Should throw IllegalArgumentException for null string");
    } catch (IllegalArgumentException e) {
      assertEquals("String is null.", e.getMessage());
    }

    try {
      NumberUtil.fromHex("ad", 3, 0);
      fail("Should throw IllegalArgumentException because offset greater than string length");
    } catch (IllegalArgumentException e) {
      assertEquals("Offset is greater than the length (3 >= 2).", e.getMessage());
    }

    try {
      NumberUtil.fromHex("ab", -1, 2);
      fail("Should throw IllegalArgumentException because offset greater than string length");
    } catch (IllegalArgumentException e) {
      assertEquals("Offset is negative.", e.getMessage());
    }

    try {
      NumberUtil.fromHex("ab", 0, 3);
      fail("Should throw IllegalArgumentException because count not divisible by 2");
    } catch (IllegalArgumentException e) {
      assertEquals("Count is not divisible by two (3).", e.getMessage());
    }

    try {
      NumberUtil.fromHex("a", 0, 2);
      fail("Should throw IllegalArgumentException because string length minus offset less than 2");
    } catch (IllegalArgumentException e) {
      assertEquals("Number of characters not divisible by two (1).", e.getMessage());
    }

    try {
      NumberUtil.fromHex("ga", 0, 2);
      fail("Should throw IllegalArgumentException because 'g' is invalid character");
    } catch (IllegalArgumentException e) {
      assertEquals("Character is not in [a-fA-F0-9] ('g').", e.getMessage());
    }
  }

  /**
   * Tests {@link NumberUtil#fromHex(String, int, int)}
   */
  @Test
  public void testFromHex() {
    byte[] result = NumberUtil.fromHex("ab", 0, 2);
    assertEquals(1, result.length);
    assertEquals(-85, result[0]);

    result = NumberUtil.fromHex("10", 0, 2);
    assertEquals(1, result.length);
    assertEquals(16, result[0]);

    result = NumberUtil.fromHex("ab0c", 0, 4);
    assertEquals(2, result.length);
    assertEquals(-85, result[0]);
    assertEquals(12, result[1]);

    result = NumberUtil.fromHex("0a", 0, 4);
    assertEquals(1, result.length);
    assertEquals(10, result[0]);

    result = NumberUtil.fromHex("", 0, 4);
    assertEquals(0, result.length);

    result = NumberUtil.fromHex("ab", 0, 0);
    assertEquals(0, result.length);

    result = NumberUtil.fromHex("ff", 0, 2);
    assertEquals(1, result.length);
    assertEquals(-1, result[0]);
  }

  /**
   * Tests {@link NumberUtil#toHex(byte[], int, int)} edge cases
   */
  @Test
  public void testToHexEdgeCases() {
    try {
      NumberUtil.toHex(null, 0, 1);
      fail("Should throw IllegalArgumentException because bytes is null.");
    } catch (IllegalArgumentException e) {
      assertEquals("Bytes is null.", e.getMessage());
    }

    try {
      NumberUtil.toHex(new byte[]{-85}, 1, 1);
      fail("Should throw IllegalArgumentException because offset is equal to bytes length.");
    } catch (IllegalArgumentException e) {
      assertEquals("Offset is greater than the length (1 >= 1).", e.getMessage());
    }

    try {
      NumberUtil.toHex(new byte[]{-85}, -1, 1);
      fail("Should throw IllegalArgumentException because offset is negative.");
    } catch (IllegalArgumentException e) {
      assertEquals("Offset is negative.", e.getMessage());
    }

    try {
      NumberUtil.toHex(new byte[]{-85}, 0, -1);
      fail("Should throw IllegalArgumentException because count is negative.");
    } catch (IllegalArgumentException e) {
      assertEquals("Count is negative.", e.getMessage());
    }
  }

  /**
   * Tests {@link NumberUtil#toHex(byte[], int, int)}
   */
  @Test
  public void testToHex() {
    String result = NumberUtil.toHex(new byte[]{-85}, 0, 1);
    assertEquals("AB", result);

    result = NumberUtil.toHex(new byte[]{-85}, 0, 0);
    assertEquals("", result);

    result = NumberUtil.toHex(new byte[]{-85}, 0, 5);
    assertEquals("AB", result);

    result = NumberUtil.toHex(new byte[]{-85, 10}, 0, 2);
    assertEquals("AB0A", result);

    result = NumberUtil.toHex(new byte[]{-85, 10}, 1, 2);
    assertEquals("0A", result);

    result = NumberUtil.toHex(new byte[]{-1}, 0, 1);
    assertEquals("FF", result);
  }
}
