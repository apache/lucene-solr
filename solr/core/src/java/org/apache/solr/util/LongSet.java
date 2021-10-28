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

package org.apache.solr.util;


import java.util.NoSuchElementException;

/** Collects long values in a hash set (closed hashing on power-of-two sized long[])
 * @lucene.internal
 */
public class LongSet {

  private static final float LOAD_FACTOR = 0.7f;

  private long[] vals;
  private int cardinality;
  private int mask;
  private int threshold;
  private int zeroCount;  // 1 if a 0 was collected

  public LongSet(int sz) {
    sz = Math.max(org.apache.lucene.util.BitUtil.nextHighestPowerOfTwo(sz), 2);
    vals = new long[sz];
    mask = sz - 1;
    threshold = (int) (sz * LOAD_FACTOR);
  }

  /** Returns the long[] array that has entries filled in with values or "0" for empty.
   * To see if "0" itself is in the set, call containsZero()
   */
  public long[] getBackingArray() {
    return vals;
  }

  public boolean containsZero() {
    return zeroCount != 0;
  }

  /** Adds an additional value to the set, returns true if the set did not already contain the value. */
  public boolean add(long val) {
    if (val == 0) {
      if (zeroCount != 0) {
        return false;
      } else {
        zeroCount = 1;
        return true;
      }
    }
    if (cardinality >= threshold) {
      rehash();
    }

    // For floats: exponent bits start at bit 23 for single precision,
    // and bit 52 for double precision.
    // Many values will only have significant bits just to the right of that.

    // For now, lets just settle to get first 8 significant mantissa bits of double or float in the lowest bits of our hash
    // The upper bits of our hash will be irrelevant.
    int h = (int) (val + (val >>> 44) + (val >>> 15));
    for (int slot = h & mask; ; slot = (slot + 1) & mask) {
      long v = vals[slot];
      if (v == 0) {
        vals[slot] = val;
        cardinality++;
        return true;
      } else if (v == val) {
        // val is already in the set
        break;
      }
    }
    return false;
  }

  private void rehash() {
    long[] oldVals = vals;
    int newCapacity = vals.length << 1;
    vals = new long[newCapacity];
    mask = newCapacity - 1;
    threshold = (int) (newCapacity * LOAD_FACTOR);
    cardinality = 0;

    for (long val : oldVals) {
      if (val != 0) {
        add(val);
      }
    }
  }

  /** The number of values in the set */
  public int cardinality() {
    return cardinality + zeroCount;
  }


  /** Returns an iterator over the values in the set. */
  public LongIterator iterator() {
    return new LongIterator() {
      private int remainingValues = cardinality();
      private int valsIdx = 0;

      @Override
      public boolean hasNext() {
        return remainingValues > 0;
      }

      @Override
      public long next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        remainingValues--;

        if (remainingValues == 0 && zeroCount > 0) {
          return 0;
        }

        while (true) { // guaranteed to find another value if we get here
          long value = vals[valsIdx++];
          if (value != 0) {
            return value;
          }
        }
      }

    };
  }
}
