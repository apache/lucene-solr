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
package org.apache.solr.search;

import org.apache.lucene.util.FixedBitSet;

/**
 * A {@link BitDocSet} based implementation that mutates the underlying bits for andNot and
 * intersection. This allows for computing the combinations of sets without duplicating the
 * underlying array. This MutableBitDocSet should not be cached because it can be modified.
 *
 * @since solr 9.2
 */
class MutableBitDocSet extends BitDocSet {
  private MutableBitDocSet(FixedBitSet bits, int size) {
    super(bits, size);
  }

  /**
   * Returns a mutable BitDocSet that is a copy of the provided BitDocSet.
   *
   * @param bitDocSet a BitDocSet
   * @return copy of bitDocSet that is now mutable
   */
  public static MutableBitDocSet fromBitDocSet(BitDocSet bitDocSet) {
    // don't call size() since we just want to pass through the size
    // instead of computing it if it was already computed
    return new MutableBitDocSet(bitDocSet.getBits().clone(), bitDocSet.size);
  }

  /**
   * Returns a new BitDocSet with the same bits if the DocSet provided is a MutableBitDocSet.
   * Otherwise, just returns the provided DocSet.
   *
   * @param docSet DocSet to unwrap if it is a MutableBitDocSet
   * @return Unwrapped DocSet that is not mutable
   */
  public static DocSet unwrapIfMutable(DocSet docSet) {
    if (docSet instanceof MutableBitDocSet) {
      MutableBitDocSet mutableBitDocSet = (MutableBitDocSet) docSet;
      // don't call size() since we just want to pass through the size
      // instead of computing it if it was already computed
      return new BitDocSet(mutableBitDocSet.getBits(), mutableBitDocSet.size);
    }
    return docSet;
  }

  private void resetSize() {
    // We need to reset size since we are changing the cardinality of the underlying bits and size
    // is typically cached. This forces size to be recomputed as needed.
    this.size = -1;
  }

  @Override
  public DocIterator iterator() {
    throw new UnsupportedOperationException();
  }

  /**
   * Returns the intersection of this set with another set. This mutates the underlying bits so do
   * not cache the returned bitset.
   *
   * @return a DocSet representing the intersection
   */
  @Override
  public DocSet intersection(DocSet other) {
    // intersection is overloaded in the smaller DocSets to be more
    // efficient, so dispatch off of it instead.
    if (!(other instanceof BitDocSet)) {
      return other.intersection(this);
    }

    // Operates directly on the underlying bitsets.
    FixedBitSet newbits = getBits();
    newbits.and(toBitSet(other));

    resetSize();
    return this;
  }

  @Override
  public int intersectionSize(DocSet other) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean intersects(DocSet other) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int unionSize(DocSet other) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int andNotSize(DocSet other) {
    throw new UnsupportedOperationException();
  }

  /**
   * Returns the documents in this set that are not in the other set. This mutates the underlying
   * bits so do not cache the returned bitset.
   *
   * @return a DocSet representing this AND NOT other
   */
  @Override
  public DocSet andNot(DocSet other) {
    andNot(getBits(), other);
    resetSize();
    return this;
  }

  @Override
  public DocSet union(DocSet other) {
    throw new UnsupportedOperationException();
  }

  @Override
  public BitDocSet clone() {
    throw new UnsupportedOperationException();
  }

  @Override
  public String toString() {
    return "MutableBitDocSet instance of " + super.toString();
  }
}
