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

import java.io.IOException;
import java.util.Objects;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BitDocIdSet;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.solr.common.SolrException;

/** A base class that may be useful for implementing DocSets */
abstract class DocSetBase implements DocSet {

  public static FixedBitSet toBitSet(DocSet set) {
    if (set instanceof DocSetBase) {
      return ((DocSetBase) set).getBits();
    } else {
      FixedBitSet bits = new FixedBitSet(64);
      for (DocIterator iter = set.iterator(); iter.hasNext();) {
        int nextDoc = iter.nextDoc();
        bits = FixedBitSet.ensureCapacity(bits, nextDoc);
        bits.set(nextDoc);
      }
      return bits;
    }
  }
  
  // Not implemented efficiently... for testing purposes only
  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof DocSet)) return false;
    DocSet other = (DocSet)obj;
    if (this.size() != other.size()) return false;

    if (this instanceof DocList && other instanceof DocList) {
      // compare ordering
      DocIterator i1=this.iterator();
      DocIterator i2=other.iterator();
      while(i1.hasNext() && i2.hasNext()) {
        if (i1.nextDoc() != i2.nextDoc()) return false;
      }
      return true;
      // don't compare matches
    }

    FixedBitSet bs1 = this.getBits();
    FixedBitSet bs2 = toBitSet(other);

// resize both BitSets to make sure they have the same amount of zero padding

    int maxNumBits = bs1.length() > bs2.length() ? bs1.length() : bs2.length();
    bs1 = FixedBitSet.ensureCapacity(bs1, maxNumBits);
    bs2 = FixedBitSet.ensureCapacity(bs2, maxNumBits);

    // if (this.size() != other.size()) return false;
    return bs1.equals(bs2);
  }

  public DocSet clone() {
    throw new RuntimeException(new CloneNotSupportedException());
  }

  /**
   * @throws SolrException Base implementation does not allow modifications
   */
  @Override
  public void add(int doc) {
    throw new SolrException( SolrException.ErrorCode.SERVER_ERROR,"Unsupported Operation");
  }

  /**
   * @throws SolrException Base implementation does not allow modifications
   */
  @Override
  public void addUnique(int doc) {
    throw new SolrException( SolrException.ErrorCode.SERVER_ERROR,"Unsupported Operation");
  }

  /**
   * Return a {@link FixedBitSet} with a bit set for every document in this
   * {@link DocSet}. The default implementation iterates on all docs and sets
   * the relevant bits. You should override if you can provide a more efficient
   * implementation.
   */
  protected FixedBitSet getBits() {
    FixedBitSet bits = new FixedBitSet(size());
    for (DocIterator iter = iterator(); iter.hasNext();) {
      int nextDoc = iter.nextDoc();
      bits = FixedBitSet.ensureCapacity(bits, nextDoc);
      bits.set(nextDoc);
    }
    return bits;
  }

  @Override
  public DocSet intersection(DocSet other) {
    // intersection is overloaded in the smaller DocSets to be more
    // efficient, so dispatch off of it instead.
    if (!(other instanceof BitDocSet)) {
      return other.intersection(this);
    }

    // Default... handle with bitsets.
    FixedBitSet newbits = getBits().clone();
    newbits.and(toBitSet(other));
    return new BitDocSet(newbits);
  }

  @Override
  public boolean intersects(DocSet other) {
    // intersection is overloaded in the smaller DocSets to be more
    // efficient, so dispatch off of it instead.
    if (!(other instanceof BitDocSet)) {
      return other.intersects(this);
    }
    // less efficient way: get the intersection size
    return intersectionSize(other) > 0;
  }

  @Override
  public DocSet union(DocSet other) {
    FixedBitSet otherBits = toBitSet(other);
    FixedBitSet newbits = FixedBitSet.ensureCapacity(getBits().clone(), otherBits.length());
    newbits.or(otherBits);
    return new BitDocSet(newbits);
  }

  @Override
  public int intersectionSize(DocSet other) {
    // intersection is overloaded in the smaller DocSets to be more
    // efficient, so dispatch off of it instead.
    if (!(other instanceof BitDocSet)) {
      return other.intersectionSize(this);
    }
    // less efficient way: do the intersection then get its size
    return intersection(other).size();
  }

  @Override
  public int unionSize(DocSet other) {
    return this.size() + other.size() - this.intersectionSize(other);
  }

  @Override
  public DocSet andNot(DocSet other) {
    FixedBitSet newbits = getBits().clone();
    newbits.andNot(toBitSet(other));
    return new BitDocSet(newbits);
  }

  @Override
  public int andNotSize(DocSet other) {
    return this.size() - this.intersectionSize(other);
  }

  @Override
  public Filter getTopFilter() {
    return new Filter() {
      final FixedBitSet bs = getBits();

      @Override
      public DocIdSet getDocIdSet(final LeafReaderContext context, Bits acceptDocs) {
        LeafReader reader = context.reader();
        // all Solr DocSets that are used as filters only include live docs
        final Bits acceptDocs2 = acceptDocs == null ? null : (reader.getLiveDocs() == acceptDocs ? null : acceptDocs);

        if (context.isTopLevel) {
          return BitsFilteredDocIdSet.wrap(new BitDocIdSet(bs), acceptDocs);
        }

        final int base = context.docBase;
        final int maxDoc = reader.maxDoc();
        final int max = base + maxDoc;   // one past the max doc in this segment.

        return BitsFilteredDocIdSet.wrap(new DocIdSet() {
          @Override
          public DocIdSetIterator iterator() {
            return new DocIdSetIterator() {
              int pos=base-1;
              int adjustedDoc=-1;

              @Override
              public int docID() {
                return adjustedDoc;
              }

              @Override
              public int nextDoc() {
                pos = bs.nextSetBit(pos+1);  // TODO: this is buggy if getBits() returns a bitset that does not have a capacity of maxDoc
                return adjustedDoc = pos<max ? pos-base : NO_MORE_DOCS;
              }

              @Override
              public int advance(int target) {
                if (target==NO_MORE_DOCS) return adjustedDoc=NO_MORE_DOCS;
                pos = bs.nextSetBit(target+base);
                return adjustedDoc = pos<max ? pos-base : NO_MORE_DOCS;
              }

              @Override
              public long cost() {
                return bs.length();
              }
            };
          }

          @Override
          public long ramBytesUsed() {
            return bs.ramBytesUsed();
          }

          @Override
          public Bits bits() {
            // sparse filters should not use random access
            return null;
          }

        }, acceptDocs2);
      }

      @Override
      public String toString(String field) {
        return "DocSetTopFilter";
      }

      @Override
      public boolean equals(Object other) {
        return sameClassAs(other) &&
               Objects.equals(bs, getClass().cast(other).bs);
      }

      @Override
      public int hashCode() {
        return classHash() ^ bs.hashCode();
      }
    };
  }

  @Override
  public void addAllTo(DocSet target) {
    DocIterator iter = iterator();
    while (iter.hasNext()) {
      target.add(iter.nextDoc());
    }
  }


  /** FUTURE: for off-heap */
  @Override
  public void close() throws IOException {
  }
}
