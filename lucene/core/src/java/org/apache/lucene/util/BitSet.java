package org.apache.lucene.util;

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

import java.io.IOException;

import org.apache.lucene.search.DocIdSetIterator;

/**
 * Base implementation for a bit set.
 * @lucene.internal
 */
public abstract class BitSet implements MutableBits, Accountable {

  /** Set the bit at <code>i</code>. */
  public abstract void set(int i);

  /** Clears a range of bits.
   *
   * @param startIndex lower index
   * @param endIndex one-past the last bit to clear
   */
  public abstract void clear(int startIndex, int endIndex);

  /**
   * Return the number of bits that are set.
   * NOTE: this method is likely to run in linear time
   */
  public abstract int cardinality();

  /**
   * Return an approximation of the cardinality of this set. Some
   * implementations may trade accuracy for speed if they have the ability to
   * estimate the cardinality of the set without iterating over all the data.
   * The default implementation returns {@link #cardinality()}.
   */
  public int approximateCardinality() {
    return cardinality();
  }

  /** Returns the index of the first set bit starting at the index specified.
   *  {@link DocIdSetIterator#NO_MORE_DOCS} is returned if there are no more set bits.
   */
  public abstract int nextSetBit(int i);

  /** Does in-place OR of the bits provided by the
   *  iterator. */
  public void or(DocIdSetIterator iter) throws IOException {
    for (int doc = iter.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = iter.nextDoc()) {
      set(doc);
    }
  }

  /** Does in-place AND of the bits provided by the
   *  iterator. */
  public void and(DocIdSetIterator iter) throws IOException {
    final int length = length();
    if (length == 0) {
      return;
    }
    int disiDoc, bitSetDoc = nextSetBit(0);
    while (bitSetDoc != DocIdSetIterator.NO_MORE_DOCS && (disiDoc = iter.advance(bitSetDoc)) < length) {
      clear(bitSetDoc, disiDoc);
      disiDoc++;
      bitSetDoc = (disiDoc < length) ? nextSetBit(disiDoc) : DocIdSetIterator.NO_MORE_DOCS;
    }
    if (bitSetDoc != DocIdSetIterator.NO_MORE_DOCS) {
      clear(bitSetDoc, length);
    }
  }

  /** this = this AND NOT other */
  public void andNot(DocIdSetIterator iter) throws IOException {
    for (int doc = iter.nextDoc(), len = length(); doc < len; doc = iter.nextDoc()) {
      clear(doc);
    }
  }

}
