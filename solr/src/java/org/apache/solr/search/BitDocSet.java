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

package org.apache.solr.search;

import org.apache.lucene.util.OpenBitSet;
import org.apache.lucene.util.OpenBitSetIterator;
import org.apache.lucene.search.DocIdSetIterator;

/**
 * <code>BitDocSet</code> represents an unordered set of Lucene Document Ids
 * using a BitSet.  A set bit represents inclusion in the set for that document.
 *
 * @version $Id$
 * @since solr 0.9
 */
public class BitDocSet extends DocSetBase {
  final OpenBitSet bits;
  int size;    // number of docs in the set (cached for perf)

  public BitDocSet() {
    bits = new OpenBitSet();
  }

  /** Construct a BitDocSet.
   * The capacity of the OpenBitSet should be at least maxDoc() */
  public BitDocSet(OpenBitSet bits) {
    this.bits = bits;
    size=-1;
  }

  /** Construct a BitDocSet, and provides the number of set bits.
   * The capacity of the OpenBitSet should be at least maxDoc()
   */
  public BitDocSet(OpenBitSet bits, int size) {
    this.bits = bits;
    this.size = size;
  }

  /*** DocIterator using nextSetBit()
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
  ***/

  public DocIterator iterator() {
    return new DocIterator() {
      private final OpenBitSetIterator iter = new OpenBitSetIterator(bits);
      private int pos = iter.nextDoc();
      public boolean hasNext() {
        return pos != DocIdSetIterator.NO_MORE_DOCS;
      }

      public Integer next() {
        return nextDoc();
      }

      public void remove() {
        bits.clear(pos);
      }

      public int nextDoc() {
        int old=pos;
        pos=iter.nextDoc();
        return old;
      }

      public float score() {
        return 0.0f;
      }
    };
  }


  /**
   *
   * @return the <b>internal</b> OpenBitSet that should <b>not</b> be modified.
   */
  @Override
  public OpenBitSet getBits() {
    return bits;
  }

  @Override
  public void add(int doc) {
    bits.set(doc);
    size=-1;  // invalidate size
  }

  @Override
  public void addUnique(int doc) {
    bits.set(doc);
    size=-1;  // invalidate size
  }

  public int size() {
    if (size!=-1) return size;
    return size=(int)bits.cardinality();
  }

  /**
   * The number of set bits - size - is cached.  If the bitset is changed externally,
   * this method should be used to invalidate the previously cached size.
   */
  public void invalidateSize() {
    size=-1;
  }

  /** Returns true of the doc exists in the set.
   *  Should only be called when doc < OpenBitSet.size()
   */
  public boolean exists(int doc) {
    return bits.fastGet(doc);
  }

  @Override
  public int intersectionSize(DocSet other) {
    if (other instanceof BitDocSet) {
      return (int)OpenBitSet.intersectionCount(this.bits, ((BitDocSet)other).bits);
    } else {
      // they had better not call us back!
      return other.intersectionSize(this);
    }
  }

  @Override
  public boolean intersects(DocSet other) {
    if (other instanceof BitDocSet) {
      return bits.intersects(((BitDocSet)other).bits);
    } else {
      // they had better not call us back!
      return other.intersects(this);
    }
  }

  @Override
  public int unionSize(DocSet other) {
    if (other instanceof BitDocSet) {
      // if we don't know our current size, this is faster than
      // size + other.size - intersection_size
      return (int)OpenBitSet.unionCount(this.bits, ((BitDocSet)other).bits);
    } else {
      // they had better not call us back!
      return other.unionSize(this);
    }
  }

  @Override
  public int andNotSize(DocSet other) {
    if (other instanceof BitDocSet) {
      // if we don't know our current size, this is faster than
      // size - intersection_size
      return (int)OpenBitSet.andNotCount(this.bits, ((BitDocSet)other).bits);
    } else {
      return super.andNotSize(other);
    }
  }

  @Override
  public void setBitsOn(OpenBitSet target) {
    target.union(bits);
  }

  @Override
   public DocSet andNot(DocSet other) {
    OpenBitSet newbits = (OpenBitSet)(bits.clone());
     if (other instanceof BitDocSet) {
       newbits.andNot(((BitDocSet)other).bits);
     } else {
       DocIterator iter = other.iterator();
       while (iter.hasNext()) newbits.clear(iter.nextDoc());
     }
     return new BitDocSet(newbits);
  }

  @Override
   public DocSet union(DocSet other) {
     OpenBitSet newbits = (OpenBitSet)(bits.clone());
     if (other instanceof BitDocSet) {
       newbits.union(((BitDocSet)other).bits);
     } else {
       DocIterator iter = other.iterator();
       while (iter.hasNext()) newbits.set(iter.nextDoc());
     }
     return new BitDocSet(newbits);
  }


  public long memSize() {
    return (bits.getBits().length << 3) + 16;
  }

  @Override
  protected BitDocSet clone() {
    return new BitDocSet((OpenBitSet)bits.clone(), size);
  }
}
