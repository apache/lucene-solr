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

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BitsFilteredDocIdSet;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.FixedBitSet.FixedBitSetIterator;
import org.apache.lucene.util.FixedBitDocIdSet;

/**
 * <code>BitDocSet</code> represents an unordered set of Lucene Document Ids
 * using a BitSet.  A set bit represents inclusion in the set for that document.
 *
 * @since solr 0.9
 */
public class BitDocSet extends DocSetBase {
  final FixedBitSet bits;
  int size;    // number of docs in the set (cached for perf)

  public BitDocSet() {
    bits = new FixedBitSet(64);
  }

  /**
   * Construct a BitDocSet. The capacity of the {@link FixedBitSet} should be at
   * least maxDoc()
   */
  public BitDocSet(FixedBitSet bits) {
    this.bits = bits;
    size=-1;
  }

  /**
   * Construct a BitDocSet, and provides the number of set bits. The capacity of
   * the {@link FixedBitSet} should be at least maxDoc()
   */
  public BitDocSet(FixedBitSet bits, int size) {
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

  @Override
  public DocIterator iterator() {
    return new DocIterator() {
      private final FixedBitSetIterator iter = new FixedBitSetIterator(bits, 0L); // cost is not useful here
      private int pos = iter.nextDoc();
      @Override
      public boolean hasNext() {
        return pos != DocIdSetIterator.NO_MORE_DOCS;
      }

      @Override
      public Integer next() {
        return nextDoc();
      }

      @Override
      public void remove() {
        bits.clear(pos);
      }

      @Override
      public int nextDoc() {
        int old=pos;
        pos=iter.nextDoc();
        return old;
      }

      @Override
      public float score() {
        return 0.0f;
      }
    };
  }

  /**
   * @return the <b>internal</b> {@link FixedBitSet} that should <b>not</b> be modified.
   */
  @Override
  public FixedBitSet getBits() {
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

  @Override
  public int size() {
    if (size!=-1) return size;
    return size = bits.cardinality();
  }

  /**
   * The number of set bits - size - is cached.  If the bitset is changed externally,
   * this method should be used to invalidate the previously cached size.
   */
  public void invalidateSize() {
    size=-1;
  }

  /**
   * Returns true of the doc exists in the set. Should only be called when doc <
   * {@link FixedBitSet#length()}.
   */
  @Override
  public boolean exists(int doc) {
    return bits.get(doc);
  }

  @Override
  public int intersectionSize(DocSet other) {
    if (other instanceof BitDocSet) {
      return (int) FixedBitSet.intersectionCount(this.bits, ((BitDocSet) other).bits);
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
      return (int) FixedBitSet.unionCount(this.bits, ((BitDocSet)other).bits);
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
      return (int) FixedBitSet.andNotCount(this.bits, ((BitDocSet)other).bits);
    } else {
      return super.andNotSize(other);
    }
  }

  @Override
  public void addAllTo(DocSet target) {
    if (target instanceof BitDocSet) {
      ((BitDocSet) target).bits.or(bits);
    } else {
      super.addAllTo(target);
    }
  }

  @Override
  public DocSet andNot(DocSet other) {
    FixedBitSet newbits = bits.clone();
    if (other instanceof BitDocSet) {
      newbits.andNot(((BitDocSet) other).bits);
    } else {
      DocIterator iter = other.iterator();
      while (iter.hasNext()) {
        int doc = iter.nextDoc();
        if (doc < newbits.length()) {
          newbits.clear(doc);
        }
      }
    }
    return new BitDocSet(newbits);
  }
  
  @Override
  public DocSet union(DocSet other) {
    FixedBitSet newbits = bits.clone();
    if (other instanceof BitDocSet) {
      BitDocSet otherDocSet = (BitDocSet) other;
      newbits = FixedBitSet.ensureCapacity(newbits, otherDocSet.bits.length());
      newbits.or(otherDocSet.bits);
    } else {
      DocIterator iter = other.iterator();
      while (iter.hasNext()) {
        int doc = iter.nextDoc();
        newbits = FixedBitSet.ensureCapacity(newbits, doc);
        newbits.set(doc);
      }
    }
    return new BitDocSet(newbits);
  }
  
  @Override
  public long memSize() {
    return (bits.getBits().length << 3) + 16;
  }

  @Override
  protected BitDocSet clone() {
    return new BitDocSet(bits.clone(), size);
  }

  @Override
  public Filter getTopFilter() {
    final FixedBitSet bs = bits;
    // TODO: if cardinality isn't cached, do a quick measure of sparseness
    // and return null from bits() if too sparse.

    return new Filter() {
      @Override
      public DocIdSet getDocIdSet(final LeafReaderContext context, final Bits acceptDocs) {
        LeafReader reader = context.reader();
        // all Solr DocSets that are used as filters only include live docs
        final Bits acceptDocs2 = acceptDocs == null ? null : (reader.getLiveDocs() == acceptDocs ? null : acceptDocs);

        if (context.isTopLevel) {
          return BitsFilteredDocIdSet.wrap(new FixedBitDocIdSet(bs), acceptDocs);
        }

        final int base = context.docBase;
        final int maxDoc = reader.maxDoc();
        final int max = base + maxDoc;   // one past the max doc in this segment.

        return BitsFilteredDocIdSet.wrap(new DocIdSet() {
          @Override
          public DocIdSetIterator iterator() {
            return new DocIdSetIterator() {
              int pos = base - 1;
              int adjustedDoc = -1;

              @Override
              public int docID() {
                return adjustedDoc;
              }

              @Override
              public int nextDoc() {
                if (pos >= bs.length() - 1) {
                  return adjustedDoc = NO_MORE_DOCS;
                } else {
                  pos = bs.nextSetBit(pos + 1);
                  return adjustedDoc = (pos >= 0 && pos < max) ? pos - base : NO_MORE_DOCS;
                }
              }

              @Override
              public int advance(int target) {
                if (target == NO_MORE_DOCS) return adjustedDoc = NO_MORE_DOCS;
                int adjusted = target + base;
                if (adjusted >= bs.length()) {
                  return adjustedDoc = NO_MORE_DOCS;
                } else {
                  pos = bs.nextSetBit(adjusted);
                  return adjustedDoc = (pos >= 0 && pos < max) ? pos - base : NO_MORE_DOCS;
                }
              }

              @Override
              public long cost() {
                // we don't want to actually compute cardinality, but
                // if its already been computed, we use it (pro-rated for the segment)
                if (size != -1) {
                  return (long)(size * ((FixedBitSet.bits2words(maxDoc)<<6) / (float)bs.length()));
                } else {
                  return maxDoc;
                }
              }
            };
          }

          @Override
          public boolean isCacheable() {
            return true;
          }

          @Override
          public long ramBytesUsed() {
            return bs.ramBytesUsed();
          }

          @Override
          public Bits bits() {
            return new Bits() {
              @Override
              public boolean get(int index) {
                return bs.get(index + base);
              }

              @Override
              public int length() {
                return maxDoc;
              }
            };
          }

        }, acceptDocs2);
      }
    };
  }
}
