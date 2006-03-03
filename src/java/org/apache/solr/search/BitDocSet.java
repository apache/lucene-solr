/**
 * Copyright 2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import java.util.BitSet;

/**
 * <code>BitDocSet</code> represents an unordered set of Lucene Document Ids
 * using a BitSet.  A set bit represents inclusion in the set for that document.
 *
 * @author yonik
 * @version $Id$
 * @since solr 0.9
 */
public class BitDocSet extends DocSetBase {
  final BitSet bits;
  int size;    // number of docs in the set (cached for perf)

  public BitDocSet() {
    bits = new BitSet();
  }

  public BitDocSet(BitSet bits) {
    this.bits = bits;
    size=-1;
  }

  public BitDocSet(BitSet bits, int size) {
    this.bits = bits;
    this.size = size;
  }

  public DocIterator iterator() {
    return new DocIterator() {
      int pos=bits.nextSetBit(0);
      public boolean hasNext() {
        return pos>=0;
      }

      public Integer next() {
        return nextDoc();
      }

      public void remove() {
        bits.clear(pos);
      }

      public int nextDoc() {
        int old=pos;
        pos=bits.nextSetBit(old+1);
        return old;
      }

      public float score() {
        return 0.0f;
      }
    };
  }

  /**
   *
   * @return the <b>internal</b> BitSet that should <b>not</b> be modified.
   */
  public BitSet getBits() {
    return bits;
  }

  public void add(int doc) {
    bits.set(doc);
    size=-1;  // invalidate size
  }

  public void addUnique(int doc) {
    size++;
    bits.set(doc);
  }

  public int size() {
    if (size!=-1) return size;
    return size=bits.cardinality();
  }

  /**
   * The number of set bits - size - is cached.  If the bitset is changed externally,
   * this method should be used to invalidate the previously cached size.
   */
  public void invalidateSize() {
    size=-1;
  }

  public boolean exists(int doc) {
    return bits.get(doc);
  }

  public long memSize() {
    return (bits.size() >> 3) + 16;
  }
}
