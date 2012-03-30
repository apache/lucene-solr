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

package org.apache.lucene.util;

import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;

// TODO: maybe merge with BitVector?  Problem is BitVector
// caches its cardinality...

/** BitSet of fixed length (numBits), backed by accessible
 *  ({@link #getBits}) long[], accessed with an int index,
 *  implementing Bits and DocIdSet.  Unlike {@link
 *  OpenBitSet} this bit set does not auto-expand, cannot
 *  handle long index, and does not have fastXX/XX variants
 *  (just X).
 *
 * @lucene.internal
 **/

public final class FixedBitSet extends DocIdSet implements Bits {
  private final long[] bits;
  private int numBits;

  /** returns the number of 64 bit words it would take to hold numBits */
  public static int bits2words(int numBits) {
    int numLong = numBits >>> 6;
    if ((numBits & 63) != 0) {
      numLong++;
    }
    return numLong;
  }

  public FixedBitSet(int numBits) {
    this.numBits = numBits;
    bits = new long[bits2words(numBits)];
  }

  /** Makes full copy. */
  public FixedBitSet(FixedBitSet other) {
    bits = new long[other.bits.length];
    System.arraycopy(other.bits, 0, bits, 0, bits.length);
    numBits = other.numBits;
  }

  @Override
  public DocIdSetIterator iterator() {
    return new OpenBitSetIterator(bits, bits.length);
  }

  @Override
  public Bits bits() {
    return this;
  }

  @Override
  public int length() {
    return numBits;
  }

  /** This DocIdSet implementation is cacheable. */
  @Override
  public boolean isCacheable() {
    return true;
  }

  /** Expert. */
  public long[] getBits() {
    return bits;
  }

  /** Returns number of set bits.  NOTE: this visits every
   *  long in the backing bits array, and the result is not
   *  internally cached! */
  public int cardinality() {
    return (int) BitUtil.pop_array(bits, 0, bits.length);
  }

  public boolean get(int index) {
    assert index >= 0 && index < numBits: "index=" + index;
    int i = index >> 6;               // div 64
    // signed shift will keep a negative index and force an
    // array-index-out-of-bounds-exception, removing the need for an explicit check.
    int bit = index & 0x3f;           // mod 64
    long bitmask = 1L << bit;
    return (bits[i] & bitmask) != 0;
  }

  public void set(int index) {
    assert index >= 0 && index < numBits;
    int wordNum = index >> 6;      // div 64
    int bit = index & 0x3f;     // mod 64
    long bitmask = 1L << bit;
    bits[wordNum] |= bitmask;
  }

  public boolean getAndSet(int index) {
    assert index >= 0 && index < numBits;
    int wordNum = index >> 6;      // div 64
    int bit = index & 0x3f;     // mod 64
    long bitmask = 1L << bit;
    boolean val = (bits[wordNum] & bitmask) != 0;
    bits[wordNum] |= bitmask;
    return val;
  }

  public void clear(int index) {
    assert index >= 0 && index < numBits;
    int wordNum = index >> 6;
    int bit = index & 0x03f;
    long bitmask = 1L << bit;
    bits[wordNum] &= ~bitmask;
  }

  public boolean getAndClear(int index) {
    assert index >= 0 && index < numBits;
    int wordNum = index >> 6;      // div 64
    int bit = index & 0x3f;     // mod 64
    long bitmask = 1L << bit;
    boolean val = (bits[wordNum] & bitmask) != 0;
    bits[wordNum] &= ~bitmask;
    return val;
  }

  /** Returns the index of the first set bit starting at the index specified.
   *  -1 is returned if there are no more set bits.
   */
  public int nextSetBit(int index) {
    assert index >= 0 && index < numBits;
    int i = index >> 6;
    final int subIndex = index & 0x3f;      // index within the word
    long word = bits[i] >> subIndex;  // skip all the bits to the right of index

    if (word!=0) {
      return (i<<6) + subIndex + BitUtil.ntz(word);
    }

    while(++i < bits.length) {
      word = bits[i];
      if (word != 0) {
        return (i<<6) + BitUtil.ntz(word);
      }
    }

    return -1;
  }

  /** Returns the index of the last set bit before or on the index specified.
   *  -1 is returned if there are no more set bits.
   */
  public int prevSetBit(int index) {
    assert index >= 0 && index < numBits: "index=" + index + " numBits=" + numBits;
    int i = index >> 6;
    final int subIndex = index & 0x3f;  // index within the word
    long word = (bits[i] << (63-subIndex));  // skip all the bits to the left of index

    if (word != 0) {
      return (i << 6) + subIndex - Long.numberOfLeadingZeros(word); // See LUCENE-3197
    }

    while (--i >= 0) {
      word = bits[i];
      if (word !=0 ) {
        return (i << 6) + 63 - Long.numberOfLeadingZeros(word);
      }
    }

    return -1;
  }

  /** Does in-place OR of the bits provided by the
   *  iterator. */
  public void or(DocIdSetIterator iter) throws IOException {
    if (iter instanceof OpenBitSetIterator && iter.docID() == -1) {
      final OpenBitSetIterator obs = (OpenBitSetIterator) iter;
      or(obs.arr, obs.words);
      // advance after last doc that would be accepted if standard
      // iteration is used (to exhaust it):
      obs.advance(numBits);
    } else {
      int doc;
      while ((doc = iter.nextDoc()) < numBits) {
        set(doc);
      }
    }
  }

  /** this = this OR other */
  public void or(FixedBitSet other) {
    or(other.bits, other.bits.length);
  }
  
  private void or(final long[] otherArr, final int otherLen) {
    final long[] thisArr = this.bits;
    int pos = Math.min(thisArr.length, otherLen);
    while (--pos >= 0) {
      thisArr[pos] |= otherArr[pos];
    }
  }

  /** Does in-place AND of the bits provided by the
   *  iterator. */
  public void and(DocIdSetIterator iter) throws IOException {
    if (iter instanceof OpenBitSetIterator && iter.docID() == -1) {
      final OpenBitSetIterator obs = (OpenBitSetIterator) iter;
      and(obs.arr, obs.words);
      // advance after last doc that would be accepted if standard
      // iteration is used (to exhaust it):
      obs.advance(numBits);
    } else {
      if (numBits == 0) return;
      int disiDoc, bitSetDoc = nextSetBit(0);
      while (bitSetDoc != -1 && (disiDoc = iter.advance(bitSetDoc)) < numBits) {
        clear(bitSetDoc, disiDoc);
        disiDoc++;
        bitSetDoc = (disiDoc < numBits) ? nextSetBit(disiDoc) : -1;
      }
      if (bitSetDoc != -1) {
        clear(bitSetDoc, numBits);
      }
    }
  }

  /** this = this AND other */
  public void and(FixedBitSet other) {
    and(other.bits, other.bits.length);
  }
  
  private void and(final long[] otherArr, final int otherLen) {
    final long[] thisArr = this.bits;
    int pos = Math.min(thisArr.length, otherLen);
    while(--pos >= 0) {
      thisArr[pos] &= otherArr[pos];
    }
    if (thisArr.length > otherLen) {
      Arrays.fill(thisArr, otherLen, thisArr.length, 0L);
    }
  }

  /** Does in-place AND NOT of the bits provided by the
   *  iterator. */
  public void andNot(DocIdSetIterator iter) throws IOException {
    if (iter instanceof OpenBitSetIterator && iter.docID() == -1) {
      final OpenBitSetIterator obs = (OpenBitSetIterator) iter;
      andNot(obs.arr, obs.words);
      // advance after last doc that would be accepted if standard
      // iteration is used (to exhaust it):
      obs.advance(numBits);
    } else {
      int doc;
      while ((doc = iter.nextDoc()) < numBits) {
        clear(doc);
      }
    }
  }

  /** this = this AND NOT other */
  public void andNot(FixedBitSet other) {
    andNot(other.bits, other.bits.length);
  }
  
  private void andNot(final long[] otherArr, final int otherLen) {
    final long[] thisArr = this.bits;
    int pos = Math.min(thisArr.length, otherLen);
    while(--pos >= 0) {
      thisArr[pos] &= ~otherArr[pos];
    }
  }

  // NOTE: no .isEmpty() here because that's trappy (ie,
  // typically isEmpty is low cost, but this one wouldn't
  // be)

  /** Flips a range of bits
   *
   * @param startIndex lower index
   * @param endIndex one-past the last bit to flip
   */
  public void flip(int startIndex, int endIndex) {
    assert startIndex >= 0 && startIndex < numBits;
    assert endIndex >= 0 && endIndex <= numBits;
    if (endIndex <= startIndex) {
      return;
    }

    int startWord = startIndex >> 6;
    int endWord = (endIndex-1) >> 6;

    /*** Grrr, java shifting wraps around so -1L>>>64 == -1
     * for that reason, make sure not to use endmask if the bits to flip will
     * be zero in the last word (redefine endWord to be the last changed...)
    long startmask = -1L << (startIndex & 0x3f);     // example: 11111...111000
    long endmask = -1L >>> (64-(endIndex & 0x3f));   // example: 00111...111111
    ***/

    long startmask = -1L << startIndex;
    long endmask = -1L >>> -endIndex;  // 64-(endIndex&0x3f) is the same as -endIndex due to wrap

    if (startWord == endWord) {
      bits[startWord] ^= (startmask & endmask);
      return;
    }

    bits[startWord] ^= startmask;

    for (int i=startWord+1; i<endWord; i++) {
      bits[i] = ~bits[i];
    }

    bits[endWord] ^= endmask;
  }

  /** Sets a range of bits
   *
   * @param startIndex lower index
   * @param endIndex one-past the last bit to set
   */
  public void set(int startIndex, int endIndex) {
    assert startIndex >= 0 && startIndex < numBits;
    assert endIndex >= 0 && endIndex <= numBits;
    if (endIndex <= startIndex) {
      return;
    }

    int startWord = startIndex >> 6;
    int endWord = (endIndex-1) >> 6;

    long startmask = -1L << startIndex;
    long endmask = -1L >>> -endIndex;  // 64-(endIndex&0x3f) is the same as -endIndex due to wrap

    if (startWord == endWord) {
      bits[startWord] |= (startmask & endmask);
      return;
    }

    bits[startWord] |= startmask;
    Arrays.fill(bits, startWord+1, endWord, -1L);
    bits[endWord] |= endmask;
  }

  /** Clears a range of bits.
   *
   * @param startIndex lower index
   * @param endIndex one-past the last bit to clear
   */
  public void clear(int startIndex, int endIndex) {
    assert startIndex >= 0 && startIndex < numBits;
    assert endIndex >= 0 && endIndex <= numBits;
    if (endIndex <= startIndex) {
      return;
    }

    int startWord = startIndex >> 6;
    int endWord = (endIndex-1) >> 6;

    long startmask = -1L << startIndex;
    long endmask = -1L >>> -endIndex;  // 64-(endIndex&0x3f) is the same as -endIndex due to wrap

    // invert masks since we are clearing
    startmask = ~startmask;
    endmask = ~endmask;

    if (startWord == endWord) {
      bits[startWord] &= (startmask | endmask);
      return;
    }

    bits[startWord] &= startmask;
    Arrays.fill(bits, startWord+1, endWord, 0L);
    bits[endWord] &= endmask;
  }

  @Override
  public FixedBitSet clone() {
    return new FixedBitSet(this);
  }

  /** returns true if both sets have the same bits set */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof FixedBitSet)) {
      return false;
    }
    FixedBitSet other = (FixedBitSet) o;
    if (numBits != other.length()) {
      return false;
    }
    return Arrays.equals(bits, other.bits);
  }

  @Override
  public int hashCode() {
    long h = 0;
    for (int i = bits.length; --i>=0;) {
      h ^= bits[i];
      h = (h << 1) | (h >>> 63); // rotate left
    }
    // fold leftmost bits into right and add a constant to prevent
    // empty sets from returning 0, which is too common.
    return (int) ((h>>32) ^ h) + 0x98761234;
  }
}
