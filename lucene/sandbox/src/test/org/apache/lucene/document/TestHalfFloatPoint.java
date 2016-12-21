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
package org.apache.lucene.document;

import java.util.Arrays;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.TestUtil;

public class TestHalfFloatPoint extends LuceneTestCase {

  private void testHalfFloat(String sbits, float value) {
    short bits = (short) Integer.parseInt(sbits, 2);
    float converted = HalfFloatPoint.shortBitsToHalfFloat(bits);
    assertEquals(value, converted, 0f);
    short bits2 = HalfFloatPoint.halfFloatToShortBits(converted);
    assertEquals(bits, bits2);
  }

  public void testHalfFloatConversion() {
    assertEquals(0, HalfFloatPoint.halfFloatToShortBits(0f));
    assertEquals((short)(1 << 15), HalfFloatPoint.halfFloatToShortBits(-0f));
    assertEquals(0, HalfFloatPoint.halfFloatToShortBits(Float.MIN_VALUE)); // rounded to zero

    testHalfFloat("0011110000000000", 1);
    testHalfFloat("0011110000000001", 1.0009765625f);
    testHalfFloat("1100000000000000", -2);
    testHalfFloat("0111101111111111", 65504); // max value
    testHalfFloat("0000010000000000", (float) Math.pow(2, -14)); // minimum positive normal
    testHalfFloat("0000001111111111", (float) (Math.pow(2, -14) - Math.pow(2, -24))); // maximum subnormal
    testHalfFloat("0000000000000001", (float) Math.pow(2, -24)); // minimum positive subnormal
    testHalfFloat("0000000000000000", 0f);
    testHalfFloat("1000000000000000", -0f);
    testHalfFloat("0111110000000000", Float.POSITIVE_INFINITY);
    testHalfFloat("1111110000000000", Float.NEGATIVE_INFINITY);
    testHalfFloat("0111111000000000", Float.NaN);
    testHalfFloat("0011010101010101", 0.333251953125f);
  }

  public void testRoundShift() {
    assertEquals(0, HalfFloatPoint.roundShift(0, 2));
    assertEquals(0, HalfFloatPoint.roundShift(1, 2));
    assertEquals(0, HalfFloatPoint.roundShift(2, 2)); // tie so round to 0 since it ends with a 0
    assertEquals(1, HalfFloatPoint.roundShift(3, 2));
    assertEquals(1, HalfFloatPoint.roundShift(4, 2));
    assertEquals(1, HalfFloatPoint.roundShift(5, 2));
    assertEquals(2, HalfFloatPoint.roundShift(6, 2)); // tie so round to 2 since it ends with a 0
    assertEquals(2, HalfFloatPoint.roundShift(7, 2));
    assertEquals(2, HalfFloatPoint.roundShift(8, 2));
    assertEquals(2, HalfFloatPoint.roundShift(9, 2));
    assertEquals(2, HalfFloatPoint.roundShift(10, 2)); // tie so round to 2 since it ends with a 0
    assertEquals(3, HalfFloatPoint.roundShift(11, 2));
    assertEquals(3, HalfFloatPoint.roundShift(12, 2));
    assertEquals(3, HalfFloatPoint.roundShift(13, 2));
    assertEquals(4, HalfFloatPoint.roundShift(14, 2)); // tie so round to 4 since it ends with a 0
    assertEquals(4, HalfFloatPoint.roundShift(15, 2));
    assertEquals(4, HalfFloatPoint.roundShift(16, 2));
  }

  public void testRounding() {
    float[] values = new float[0];
    int o = 0;
    for (int i = Short.MIN_VALUE; i <= Short.MAX_VALUE; ++i) {
      float v = HalfFloatPoint.sortableShortToHalfFloat((short) i);
      if (Float.isFinite(v)) {
        if (o == values.length) {
          values = ArrayUtil.grow(values);
        }
        values[o++] = v;
      }
    }
    values = Arrays.copyOf(values, o);

    int iters = atLeast(1000000);
    for (int iter = 0; iter < iters; ++iter) {
      float f;
      if (random().nextBoolean()) {
        int floatBits = random().nextInt();
        f = Float.intBitsToFloat(floatBits);
      } else {
        f =  (float) ((2 * random().nextFloat() - 1) * Math.pow(2, TestUtil.nextInt(random(), -16, 16)));
      }
      float rounded = HalfFloatPoint.shortBitsToHalfFloat(HalfFloatPoint.halfFloatToShortBits(f));
      if (Float.isFinite(f) == false) {
        assertEquals(Float.floatToIntBits(f), Float.floatToIntBits(rounded), 0f);
      } else if (Float.isFinite(rounded) == false) {
        assertFalse(Float.isNaN(rounded));
        assertTrue(Math.abs(f) >= 65520);
      } else {
        int index = Arrays.binarySearch(values, f);
        float closest;
        if (index >= 0) {
          closest = values[index];
        } else {
          index = -1 - index;
          closest = Float.POSITIVE_INFINITY;
          if (index < values.length) {
            closest = values[index];
          }
          if (index - 1 >= 0) {
            if (f - values[index - 1] < closest - f) {
              closest = values[index - 1];
            } else if (f - values[index - 1] == closest - f
                && Integer.numberOfTrailingZeros(Float.floatToIntBits(values[index - 1])) > Integer.numberOfTrailingZeros(Float.floatToIntBits(closest))) {
              // in case of tie, round to even
              closest = values[index - 1];
            }
          }
        }
        assertEquals(closest, rounded, 0f);
      }
    }
  }

  public void testSortableBits() {
    int low = Short.MIN_VALUE;
    int high = Short.MAX_VALUE;
    while (Float.isNaN(HalfFloatPoint.sortableShortToHalfFloat((short) low))) {
      ++low;
    }
    while (HalfFloatPoint.sortableShortToHalfFloat((short) low) == Float.NEGATIVE_INFINITY) {
      ++low;
    }
    while (Float.isNaN(HalfFloatPoint.sortableShortToHalfFloat((short) high))) {
      --high;
    }
    while (HalfFloatPoint.sortableShortToHalfFloat((short) high) == Float.POSITIVE_INFINITY) {
      --high;
    }
    for (int i = low; i <= high + 1; ++i) {
      float previous = HalfFloatPoint.sortableShortToHalfFloat((short) (i - 1));
      float current = HalfFloatPoint.sortableShortToHalfFloat((short) i);
      assertEquals(i, HalfFloatPoint.halfFloatToSortableShort(current));
      assertTrue(Float.compare(previous, current) < 0);
    }
  }

  public void testSortableBytes() {
    for (int i = Short.MIN_VALUE + 1; i <= Short.MAX_VALUE; ++i) {
      byte[] previous = new byte[HalfFloatPoint.BYTES];
      HalfFloatPoint.shortToSortableBytes((short) (i - 1), previous, 0);
      byte[] current = new byte[HalfFloatPoint.BYTES];
      HalfFloatPoint.shortToSortableBytes((short) i, current, 0);
      assertTrue(StringHelper.compare(HalfFloatPoint.BYTES, previous, 0, current, 0) < 0);
      assertEquals(i, HalfFloatPoint.sortableBytesToShort(current, 0));
    }
  }

  /** Add a single value and search for it */
  public void testBasics() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

    // add a doc with an single dimension
    Document document = new Document();
    document.add(new HalfFloatPoint("field", 1.25f));
    writer.addDocument(document);

    // search and verify we found our doc
    IndexReader reader = writer.getReader();
    IndexSearcher searcher = newSearcher(reader);
    assertEquals(1, searcher.count(HalfFloatPoint.newExactQuery("field", 1.25f)));
    assertEquals(0, searcher.count(HalfFloatPoint.newExactQuery("field", 1f)));
    assertEquals(0, searcher.count(HalfFloatPoint.newExactQuery("field", 2f)));
    assertEquals(1, searcher.count(HalfFloatPoint.newRangeQuery("field", 1f, 2f)));
    assertEquals(0, searcher.count(HalfFloatPoint.newRangeQuery("field", 0f, 1f)));
    assertEquals(0, searcher.count(HalfFloatPoint.newRangeQuery("field", 1.5f, 2f)));
    assertEquals(1, searcher.count(HalfFloatPoint.newSetQuery("field", 1.25f)));
    assertEquals(1, searcher.count(HalfFloatPoint.newSetQuery("field", 1f, 1.25f)));
    assertEquals(0, searcher.count(HalfFloatPoint.newSetQuery("field", 1f)));
    assertEquals(0, searcher.count(HalfFloatPoint.newSetQuery("field")));

    reader.close();
    writer.close();
    dir.close();
  }

  /** Add a single multi-dimensional value and search for it */
  public void testBasicsMultiDims() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

    // add a doc with two dimensions
    Document document = new Document();
    document.add(new HalfFloatPoint("field", 1.25f, -2f));
    writer.addDocument(document);

    // search and verify we found our doc
    IndexReader reader = writer.getReader();
    IndexSearcher searcher = newSearcher(reader);
    assertEquals(1, searcher.count(HalfFloatPoint.newRangeQuery("field",
        new float[]{0, -5}, new float[]{1.25f, -1})));
    assertEquals(0, searcher.count(HalfFloatPoint.newRangeQuery("field",
        new float[]{0, 0}, new float[]{2, 2})));
    assertEquals(0, searcher.count(HalfFloatPoint.newRangeQuery("field",
        new float[]{-10, -10}, new float[]{1, 2})));

    reader.close();
    writer.close();
    dir.close();
  }

  public void testNextUp() {
    assertEquals(Float.NaN, HalfFloatPoint.nextUp(Float.NaN), 0f);
    assertEquals(Float.POSITIVE_INFINITY, HalfFloatPoint.nextUp(Float.POSITIVE_INFINITY), 0f);
    assertEquals(-65504, HalfFloatPoint.nextUp(Float.NEGATIVE_INFINITY), 0f);
    assertEquals(HalfFloatPoint.shortBitsToHalfFloat((short) 0), HalfFloatPoint.nextUp(-0f), 0f);
    assertEquals(HalfFloatPoint.shortBitsToHalfFloat((short) 1), HalfFloatPoint.nextUp(0f), 0f);
    // values that cannot be exactly represented as a half float
    assertEquals(HalfFloatPoint.nextUp(0f), HalfFloatPoint.nextUp(Float.MIN_VALUE), 0f);
    assertEquals(Float.floatToIntBits(-0f), Float.floatToIntBits(HalfFloatPoint.nextUp(-Float.MIN_VALUE)));
    assertEquals(Float.floatToIntBits(0f), Float.floatToIntBits(HalfFloatPoint.nextUp(-0f)));
  }

  public void testNextDown() {
    assertEquals(Float.NaN, HalfFloatPoint.nextDown(Float.NaN), 0f);
    assertEquals(Float.NEGATIVE_INFINITY, HalfFloatPoint.nextDown(Float.NEGATIVE_INFINITY), 0f);
    assertEquals(65504, HalfFloatPoint.nextDown(Float.POSITIVE_INFINITY), 0f);
    assertEquals(Float.floatToIntBits(-0f), Float.floatToIntBits(HalfFloatPoint.nextDown(0f)));
    // values that cannot be exactly represented as a half float
    assertEquals(Float.floatToIntBits(0f), Float.floatToIntBits(HalfFloatPoint.nextDown(Float.MIN_VALUE)));
    assertEquals(HalfFloatPoint.nextDown(-0f), HalfFloatPoint.nextDown(-Float.MIN_VALUE), 0f);
    assertEquals(Float.floatToIntBits(-0f), Float.floatToIntBits(HalfFloatPoint.nextDown(+0f)));
  }
}
