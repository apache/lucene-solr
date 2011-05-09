package org.apache.lucene.index.codecs;

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

import java.io.IOException;

import org.apache.lucene.util.LuceneTestCase;

public abstract class CodecTestCase extends LuceneTestCase {

  private static final int LIST_SIZE = 32768;

  /**
   * The different block sizes to test
   */
  protected static final int[] BLOCK_SIZES = {32, 256, 512, 2048};

  /** both are inclusive! */
  public static long nextLong(long start, long end) {
    double r = random.nextDouble();
    return (long) ((r * end) + ((1.0 - r) * start) + r);
  }
  
  public void doTestIntegerRange(final int minBits, final int maxBits, final int[] blockSizes) throws IOException {
    final int[] input = new int[LIST_SIZE];

    for (int i = minBits; i <= maxBits; i++) {
      if (VERBOSE) {
        System.out.println("TEST bits=" + i);
      }

      final long min = i == 1 ? 0 : (1L << (i - 1));
      final long max = ((1L << i) - 1);

      for (int j = 0; j < LIST_SIZE; j++) {
        input[j] = (int) nextLong(min, max);
      }

      for (final int blockSize : blockSizes) {
        if (VERBOSE)
          System.out.println("Perform Integer Range Test: bits = {" + i + "}, block size = {" + blockSize + "}");
        this.doTest(input, blockSize);
      }
    }
  }

  public void doTestIntegerRange(final int minBits, final int maxBits) throws IOException {
    this.doTestIntegerRange(minBits, maxBits, BLOCK_SIZES);
  }

  protected abstract void doTest(int[] input, int blockSize) throws IOException;

}
