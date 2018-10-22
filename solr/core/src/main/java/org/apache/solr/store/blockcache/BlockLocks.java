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
package org.apache.solr.store.blockcache;

import java.util.concurrent.atomic.AtomicLongArray;

import org.apache.lucene.util.LongBitSet;

/**
 * @lucene.experimental
 */
public class BlockLocks {
  
  private AtomicLongArray bits;
  private int wlen;
  
  public BlockLocks(long numBits) {
    int length = LongBitSet.bits2words(numBits);
    bits = new AtomicLongArray(length);
    wlen = length;
  }
  
  /**
   * Find the next clear bit in the bit set.
   * 
   * @param index
   *          index
   * @return next next bit
   */
  public int nextClearBit(int index) {
    int i = index >> 6;
    if (i >= wlen) return -1;
    int subIndex = index & 0x3f; // index within the word
    long word = ~bits.get(i) >> subIndex; // skip all the bits to the right of
                                          // index
    if (word != 0) {
      return (i << 6) + subIndex + Long.numberOfTrailingZeros(word);
    }
    while (++i < wlen) {
      word = ~bits.get(i);
      if (word != 0) {
        return (i << 6) + Long.numberOfTrailingZeros(word);
      }
    }
    return -1;
  }
  
  /**
   * Thread safe set operation that will set the bit if and only if the bit was
   * not previously set.
   * 
   * @param index
   *          the index position to set.
   * @return returns true if the bit was set and false if it was already set.
   */
  public boolean set(int index) {
    int wordNum = index >> 6; // div 64
    int bit = index & 0x3f; // mod 64
    long bitmask = 1L << bit;
    long word, oword;
    do {
      word = bits.get(wordNum);
      // if set another thread stole the lock
      if ((word & bitmask) != 0) {
        return false;
      }
      oword = word;
      word |= bitmask;
    } while (!bits.compareAndSet(wordNum, oword, word));
    return true;
  }
  
  public void clear(int index) {
    int wordNum = index >> 6;
    int bit = index & 0x03f;
    long bitmask = 1L << bit;
    long word, oword;
    do {
      word = bits.get(wordNum);
      oword = word;
      word &= ~bitmask;
    } while (!bits.compareAndSet(wordNum, oword, word));
  }
}
