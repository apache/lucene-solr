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

package org.apache.lucene.codecs.lucene70;

import java.util.Locale;
import java.util.Random;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.packed.PackedInts;

public class TestLongCompressor extends LuceneTestCase {

  // Simple money test that builds a collection of random longs, compresses them with LongCompressor and
  // checks if all values from the compressed version are equal to the source
  public void testLongCompressorMonkey() {
    final int RUNS = 10;
    final int[] MAX_SIZES = new int[]{0, 1, 10, 1000, 100_000};
    final int[] MAX_VALUE = new int[]{0, 1, 10, 1000, 100_000};
    for (int run = 0 ; run < RUNS ; run++) {
      for (int maxSize: MAX_SIZES) {
        int size = maxSize == 0 ? 0 : random().nextInt(maxSize);
        for (int maxValue: MAX_VALUE) {
          int minValue = maxValue == 0 ? 0 : random().nextInt(maxValue);
          double minChance = random().nextDouble();
          longCompressorMonkeyTest(run, size, minValue, maxValue, minChance, random().nextLong());
        }
      }
    }
  }

  public void testVerySparse() {
    final int SIZE = 674932;
    final int EVERY = SIZE/896;
    PackedInts.Mutable ranks = PackedInts.getMutable(674932, 16, PackedInts.DEFAULT);
    for (int i = 0 ; i < SIZE; i+=EVERY) {
      ranks.set(i, random().nextInt(65535));
    }
    PackedInts.Reader sparsed = LongCompressor.compress(ranks);
    assertFalse("The input and the sparsed should not be the same", ranks == sparsed);
  }

  private void longCompressorMonkeyTest(
      int run, int size, int minValue, int maxValue, double minChance, long randomSeed) {
    final String description = String.format(Locale.ENGLISH,
        "run=%d, size=%d, minValue=%d, maxValue=%d, minChance=%1.2f, seed=%d",
        run, size, minValue, maxValue, minChance, randomSeed);
    Random innerRandom = new Random(randomSeed);
    PackedInts.Mutable expected = PackedInts.getMutable(size, PackedInts.bitsRequired(maxValue), PackedInts.DEFAULT);
    for (int i = 0 ; i < size ; i++) {
      if (innerRandom.nextDouble() <= minChance) {
        continue;
      }
      expected.set(i, maxValue-minValue == 0 ? minValue : innerRandom.nextInt(maxValue-minValue)+minValue);
    }
    assertSparseValid(description, expected);
  }

  private void assertSparseValid(String description, PackedInts.Reader values) {
    try {
      PackedInts.Reader sparsed = LongCompressor.compress(values, values.size());

      for (int i = 0; i < values.size(); i++) {
        assertEquals("The value at index " + i + " should be as expected for " + description,
            values.get(i), sparsed.get(i));
      }
    } catch (Exception e) {
      throw new RuntimeException("Unexpected Exception for " + description, e);
    }
  }


}
