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
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionLengthAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;

/**
 * Generate min hash tokens from an incoming stream of tokens. The incoming tokens would typically be 5 word shingles.
 * 
 * The number of hashes used and the number of minimum values for each hash can be set. You could have 1 hash and keep
 * the 100 lowest values or 100 hashes and keep the lowest one for each. Hashes can also be bucketed in ranges over the
 * 128-bit hash space,
 * 
 * A 128-bit hash is used internally. 5 word shingles from 10e5 words generate 10e25 combinations So a 64 bit hash would
 * have collisions (1.8e19)
 * 
 * When using different hashes 32 bits are used for the hash position leaving scope for 8e28 unique hashes. A single
 * hash will use all 128 bits.
 *
 */
public class MinHashFilter extends TokenFilter {
  private static final int HASH_CACHE_SIZE = 512;

  private static final LongPair[] cachedIntHashes = new LongPair[HASH_CACHE_SIZE];

  public static final int DEFAULT_HASH_COUNT = 1;

  public static final int DEFAULT_HASH_SET_SIZE = 1;

  public static final int DEFAULT_BUCKET_COUNT = 512;

  static final String MIN_HASH_TYPE = "MIN_HASH";

  private final List<List<FixedSizeTreeSet<LongPair>>> minHashSets;

  private int hashSetSize = DEFAULT_HASH_SET_SIZE;

  private int bucketCount = DEFAULT_BUCKET_COUNT;

  private int hashCount = DEFAULT_HASH_COUNT;

  private boolean requiresInitialisation = true;

  private State endState;

  private int hashPosition = -1;

  private int bucketPosition = -1;

  private long bucketSize;
  
  private final boolean withRotation;
  
  private int endOffset;
  
  private boolean exhausted = false;

  private final CharTermAttribute termAttribute = addAttribute(CharTermAttribute.class);
  private final OffsetAttribute offsetAttribute = addAttribute(OffsetAttribute.class);
  private final TypeAttribute typeAttribute = addAttribute(TypeAttribute.class);
  private final PositionIncrementAttribute posIncAttribute = addAttribute(PositionIncrementAttribute.class);
  private final PositionLengthAttribute posLenAttribute = addAttribute(PositionLengthAttribute.class);

  static {
    for (int i = 0; i < HASH_CACHE_SIZE; i++) {
      cachedIntHashes[i] = new LongPair();
      murmurhash3_x64_128(getBytes(i), 0, 4, 0, cachedIntHashes[i]);
    }
  }

  static byte[] getBytes(int i) {
    byte[] answer = new byte[4];
    answer[3] = (byte) (i);
    answer[2] = (byte) (i >> 8);
    answer[1] = (byte) (i >> 16);
    answer[0] = (byte) (i >> 24);
    return answer;
  }

  /**
   * create a MinHash filter
   *
   * @param input the token stream
   * @param hashCount the no. of hashes
   * @param bucketCount the no. of buckets for hashing
   * @param hashSetSize the no. of min hashes to keep
   * @param withRotation whether rotate or not hashes while incrementing tokens
   */
  public MinHashFilter(TokenStream input, int hashCount, int bucketCount, int hashSetSize, boolean withRotation) {
    super(input);
    if (hashCount <= 0) {
      throw new IllegalArgumentException("hashCount must be greater than zero");
    }
    if (bucketCount <= 0) {
      throw new IllegalArgumentException("bucketCount must be greater than zero");
    }
    if (hashSetSize <= 0) {
      throw new IllegalArgumentException("hashSetSize must be greater than zero");
    }
    this.hashCount = hashCount;
    this.bucketCount = bucketCount;
    this.hashSetSize = hashSetSize;
    this.withRotation = withRotation;
    this.bucketSize = (1L << 32) / bucketCount;
    if((1L << 32) % bucketCount != 0)
    {
      bucketSize++;
    }
    minHashSets = new ArrayList<>(this.hashCount);
    for (int i = 0; i < this.hashCount; i++) {
      ArrayList<FixedSizeTreeSet<LongPair>> buckets = new ArrayList<>(this.bucketCount);
      minHashSets.add(buckets);
      for (int j = 0; j < this.bucketCount; j++) {
        FixedSizeTreeSet<LongPair> minSet = new FixedSizeTreeSet<>(this.hashSetSize);
        buckets.add(minSet);
      }
    }
    doRest();
  }

  @Override
  public final boolean incrementToken() throws IOException {
    // Pull the underlying stream of tokens
    // Hash each token found
    // Generate the required number of variants of this hash
    // Keep the minimum hash value found so far of each variant

    int positionIncrement = 0;
    if (requiresInitialisation) {
      requiresInitialisation = false;
      boolean found = false;
      // First time through so we pull and hash everything
      while (input.incrementToken()) {
        found = true;
        String current = new String(termAttribute.buffer(), 0, termAttribute.length());

        for (int i = 0; i < hashCount; i++) {
          byte[] bytes = current.getBytes("UTF-16LE");
          LongPair hash = new LongPair();
          murmurhash3_x64_128(bytes, 0, bytes.length, 0, hash);
          LongPair rehashed = combineOrdered(hash, getIntHash(i));
          minHashSets.get(i).get((int) ((rehashed.val2 >>> 32) / bucketSize)).add(rehashed);
        }
        endOffset = offsetAttribute.endOffset();
      }
      exhausted = true;
      input.end();
      // We need the end state so an underlying shingle filter can have its state restored correctly.
      endState = captureState();
      if (!found) {
        return false;
      }
      
      positionIncrement = 1;
      // fix up any wrap around bucket values. ...
      if (withRotation && (hashSetSize == 1)) {
        for (int hashLoop = 0; hashLoop < hashCount; hashLoop++) {
          for (int bucketLoop = 0; bucketLoop < bucketCount; bucketLoop++) {
            if (minHashSets.get(hashLoop).get(bucketLoop).size() == 0) {
              for (int bucketOffset = 1; bucketOffset < bucketCount; bucketOffset++) {
                if (minHashSets.get(hashLoop).get((bucketLoop + bucketOffset) % bucketCount).size() > 0) {
                  LongPair replacementHash = minHashSets.get(hashLoop).get((bucketLoop + bucketOffset) % bucketCount)
                      .first();
                  minHashSets.get(hashLoop).get(bucketLoop).add(replacementHash);
                  break;
                }
              }
            }
          }
        }
      }

    }
   
    clearAttributes();

    while (hashPosition < hashCount) {
      if (hashPosition == -1) {
        hashPosition++;
      } else {
        while (bucketPosition < bucketCount) {
          if (bucketPosition == -1) {
            bucketPosition++;
          } else {
            LongPair hash = minHashSets.get(hashPosition).get(bucketPosition).pollFirst();
            if (hash != null) {
              termAttribute.setEmpty();
              if (hashCount > 1) {
                termAttribute.append(int0(hashPosition));
                termAttribute.append(int1(hashPosition));
              }
              long high = hash.val2;
              termAttribute.append(long0(high));
              termAttribute.append(long1(high));
              termAttribute.append(long2(high));
              termAttribute.append(long3(high));
              long low = hash.val1;
              termAttribute.append(long0(low));
              termAttribute.append(long1(low));
              if (hashCount == 1) {
                termAttribute.append(long2(low));
                termAttribute.append(long3(low));
              }
              posIncAttribute.setPositionIncrement(positionIncrement);
              offsetAttribute.setOffset(0, endOffset);
              typeAttribute.setType(MIN_HASH_TYPE);
              posLenAttribute.setPositionLength(1);
              return true;
            } else {
              bucketPosition++;
            }
          }

        }
        bucketPosition = -1;
        hashPosition++;
      }
    }
    return false;
  }

  private static LongPair getIntHash(int i) {
    if (i < HASH_CACHE_SIZE) {
      return cachedIntHashes[i];
    } else {
      LongPair answer = new LongPair();
      murmurhash3_x64_128(getBytes(i), 0, 4, 0, answer);
      return answer;
    }
  }

  @Override
  public void end() throws IOException {
    if(!exhausted) {
      input.end();
    }
      
    restoreState(endState);
  }

  @Override
  public void reset() throws IOException {
    super.reset();
    doRest();
  }

  private void doRest() {
    for (int i = 0; i < hashCount; i++) {
      for (int j = 0; j < bucketCount; j++) {
        minHashSets.get(i).get(j).clear();
      }
    }
    endState = null;
    hashPosition = -1;
    bucketPosition = -1;
    requiresInitialisation = true;
    exhausted = false;
  }

  private static char long0(long x) {
    return (char) (x >> 48);
  }

  private static char long1(long x) {
    return (char) (x >> 32);
  }

  private static char long2(long x) {
    return (char) (x >> 16);
  }

  private static char long3(long x) {
    return (char) (x);
  }

  private static char int0(int x) {
    return (char) (x >> 16);
  }

  private static char int1(int x) {
    return (char) (x);
  }

  static boolean isLessThanUnsigned(long n1, long n2) {
    return (n1 < n2) ^ ((n1 < 0) != (n2 < 0));
  }

  static class FixedSizeTreeSet<E extends Comparable<E>> extends TreeSet<E> {

    /**
     * 
     */
    private static final long serialVersionUID = -8237117170340299630L;
    private final int capacity;

    FixedSizeTreeSet() {
      this(20);
    }

    FixedSizeTreeSet(int capacity) {
      super();
      this.capacity = capacity;
    }

    @Override
    public boolean add(final E toAdd) {
      if (capacity <= size()) {
        final E lastElm = last();
        if (toAdd.compareTo(lastElm) > -1) {
          return false;
        } else {
          pollLast();
        }
      }
      return super.add(toAdd);
    }
  }

  private static LongPair combineOrdered(LongPair... hashCodes) {
    LongPair result = new LongPair();
    for (LongPair hashCode : hashCodes) {
      result.val1 = result.val1 * 37 + hashCode.val1;
      result.val2 = result.val2 * 37 + hashCode.val2;

    }
    return result;
  }

  /** 128 bits of state */
  static final class LongPair implements Comparable<LongPair> {
    public long val1;
    public long val2;

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    @Override
    public int compareTo(LongPair other) {
      if (isLessThanUnsigned(val2, other.val2)) {
        return -1;
      } else if (val2 == other.val2) {
        if (isLessThanUnsigned(val1, other.val1)) {
          return -1;
        } else if (val1 == other.val1) {
          return 0;
        } else {
          return 1;
        }
      } else {
        return 1;
      }
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      LongPair longPair = (LongPair) o;

      return val1 == longPair.val1 && val2 == longPair.val2;

    }

    @Override
    public int hashCode() {
      int result = (int) (val1 ^ (val1 >>> 32));
      result = 31 * result + (int) (val2 ^ (val2 >>> 32));
      return result;
    }
  }

  /** Gets a long from a byte buffer in little endian byte order. */
  private static long getLongLittleEndian(byte[] buf, int offset) {
    return ((long) buf[offset + 7] << 56) // no mask needed
        | ((buf[offset + 6] & 0xffL) << 48)
        | ((buf[offset + 5] & 0xffL) << 40)
        | ((buf[offset + 4] & 0xffL) << 32)
        | ((buf[offset + 3] & 0xffL) << 24)
        | ((buf[offset + 2] & 0xffL) << 16)
        | ((buf[offset + 1] & 0xffL) << 8)
        | ((buf[offset] & 0xffL)); // no shift needed
  }

  /** Returns the MurmurHash3_x64_128 hash, placing the result in "out". */
  @SuppressWarnings("fallthrough") // the huge switch is designed to use fall through into cases!
  static void murmurhash3_x64_128(byte[] key, int offset, int len, int seed, LongPair out) {
    // The original algorithm does have a 32 bit unsigned seed.
    // We have to mask to match the behavior of the unsigned types and prevent sign extension.
    long h1 = seed & 0x00000000FFFFFFFFL;
    long h2 = seed & 0x00000000FFFFFFFFL;

    final long c1 = 0x87c37b91114253d5L;
    final long c2 = 0x4cf5ad432745937fL;

    int roundedEnd = offset + (len & 0xFFFFFFF0); // round down to 16 byte block
    for (int i = offset; i < roundedEnd; i += 16) {
      long k1 = getLongLittleEndian(key, i);
      long k2 = getLongLittleEndian(key, i + 8);
      k1 *= c1;
      k1 = Long.rotateLeft(k1, 31);
      k1 *= c2;
      h1 ^= k1;
      h1 = Long.rotateLeft(h1, 27);
      h1 += h2;
      h1 = h1 * 5 + 0x52dce729;
      k2 *= c2;
      k2 = Long.rotateLeft(k2, 33);
      k2 *= c1;
      h2 ^= k2;
      h2 = Long.rotateLeft(h2, 31);
      h2 += h1;
      h2 = h2 * 5 + 0x38495ab5;
    }

    long k1 = 0;
    long k2 = 0;

    switch (len & 15) {
      case 15:
        k2 = (key[roundedEnd + 14] & 0xffL) << 48;
      case 14:
        k2 |= (key[roundedEnd + 13] & 0xffL) << 40;
      case 13:
        k2 |= (key[roundedEnd + 12] & 0xffL) << 32;
      case 12:
        k2 |= (key[roundedEnd + 11] & 0xffL) << 24;
      case 11:
        k2 |= (key[roundedEnd + 10] & 0xffL) << 16;
      case 10:
        k2 |= (key[roundedEnd + 9] & 0xffL) << 8;
      case 9:
        k2 |= (key[roundedEnd + 8] & 0xffL);
        k2 *= c2;
        k2 = Long.rotateLeft(k2, 33);
        k2 *= c1;
        h2 ^= k2;
      case 8:
        k1 = ((long) key[roundedEnd + 7]) << 56;
      case 7:
        k1 |= (key[roundedEnd + 6] & 0xffL) << 48;
      case 6:
        k1 |= (key[roundedEnd + 5] & 0xffL) << 40;
      case 5:
        k1 |= (key[roundedEnd + 4] & 0xffL) << 32;
      case 4:
        k1 |= (key[roundedEnd + 3] & 0xffL) << 24;
      case 3:
        k1 |= (key[roundedEnd + 2] & 0xffL) << 16;
      case 2:
        k1 |= (key[roundedEnd + 1] & 0xffL) << 8;
      case 1:
        k1 |= (key[roundedEnd] & 0xffL);
        k1 *= c1;
        k1 = Long.rotateLeft(k1, 31);
        k1 *= c2;
        h1 ^= k1;
    }

    // ----------
    // finalization

    h1 ^= len;
    h2 ^= len;

    h1 += h2;
    h2 += h1;

    h1 = fmix64(h1);
    h2 = fmix64(h2);

    h1 += h2;
    h2 += h1;

    out.val1 = h1;
    out.val2 = h2;
  }

  private static long fmix64(long k) {
    k ^= k >>> 33;
    k *= 0xff51afd7ed558ccdL;
    k ^= k >>> 33;
    k *= 0xc4ceb9fe1a85ec53L;
    k ^= k >>> 33;
    return k;
  }
}
