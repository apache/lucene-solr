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

/**
 * Stable radix sorter for variable-length strings.
 *
 * @lucene.internal
 */
public abstract class StableMSBRadixSorter extends MSBRadixSorter {

  private final int[] fixedStartOffsets;

  public StableMSBRadixSorter(int maxLength) {
    super(maxLength);
    fixedStartOffsets = new int[HISTOGRAM_SIZE];
  }

  /** Save the i-th value into the j-th position in temporary storage. */
  protected abstract void save(int i, int j);

  /** Restore values between i-th and j-th(excluding) in temporary storage into original storage. */
  protected abstract void restore(int i, int j);

  @Override
  protected Sorter getFallbackSorter(int k) {
    return new InPlaceMergeSorter() {
      @Override
      protected void swap(int i, int j) {
        StableMSBRadixSorter.this.swap(i, j);
      }

      @Override
      protected int compare(int i, int j) {
        for (int o = k; o < maxLength; ++o) {
          final int b1 = byteAt(i, o);
          final int b2 = byteAt(j, o);
          if (b1 != b2) {
            return b1 - b2;
          } else if (b1 == -1) {
            break;
          }
        }
        return 0;
      }
    };
  }

  /**
   * Reorder elements in stable way, since Dutch sort does not guarantee ordering for same values.
   *
   * <p>When this method returns, startOffsets and endOffsets are equal.
   */
  @Override
  protected void reorder(int from, int to, int[] startOffsets, int[] endOffsets, int k) {
    System.arraycopy(startOffsets, 0, fixedStartOffsets, 0, startOffsets.length);
    for (int i = 0; i < HISTOGRAM_SIZE; ++i) {
      final int limit = endOffsets[i];
      for (int h1 = fixedStartOffsets[i]; h1 < limit; h1++) {
        final int b = getBucket(from + h1, k);
        final int h2 = startOffsets[b]++;
        save(from + h1, from + h2);
      }
    }
    restore(from, to);
  }
}
