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

import org.apache.lucene.index.AtomicReader;
import org.apache.solr.common.SolrException;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.OpenBitSet;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.BitsFilteredDocIdSet;
import org.apache.lucene.index.AtomicReaderContext;

/** A base class that may be usefull for implementing DocSets */
abstract class DocSetBase implements DocSet {

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

    // if (this.size() != other.size()) return false;
    return this.getBits().equals(other.getBits());
  }

  /**
   * @throws SolrException Base implementation does not allow modifications
   */
  public void add(int doc) {
    throw new SolrException( SolrException.ErrorCode.SERVER_ERROR,"Unsupported Operation");
  }

  /**
   * @throws SolrException Base implementation does not allow modifications
   */
  public void addUnique(int doc) {
    throw new SolrException( SolrException.ErrorCode.SERVER_ERROR,"Unsupported Operation");
  }

  /**
   * Inefficient base implementation.
   *
   * @see BitDocSet#getBits
   */
  public OpenBitSet getBits() {
    OpenBitSet bits = new OpenBitSet();
    for (DocIterator iter = iterator(); iter.hasNext();) {
      bits.set(iter.nextDoc());
    }
    return bits;
  };

  public DocSet intersection(DocSet other) {
    // intersection is overloaded in the smaller DocSets to be more
    // efficient, so dispatch off of it instead.
    if (!(other instanceof BitDocSet)) {
      return other.intersection(this);
    }

    // Default... handle with bitsets.
    OpenBitSet newbits = (OpenBitSet)(this.getBits().clone());
    newbits.and(other.getBits());
    return new BitDocSet(newbits);
  }

  public boolean intersects(DocSet other) {
    // intersection is overloaded in the smaller DocSets to be more
    // efficient, so dispatch off of it instead.
    if (!(other instanceof BitDocSet)) {
      return other.intersects(this);
    }
    // less efficient way: get the intersection size
    return intersectionSize(other) > 0;
  }


  public DocSet union(DocSet other) {
    OpenBitSet newbits = (OpenBitSet)(this.getBits().clone());
    newbits.or(other.getBits());
    return new BitDocSet(newbits);
  }

  public int intersectionSize(DocSet other) {
    // intersection is overloaded in the smaller DocSets to be more
    // efficient, so dispatch off of it instead.
    if (!(other instanceof BitDocSet)) {
      return other.intersectionSize(this);
    }
    // less efficient way: do the intersection then get it's size
    return intersection(other).size();
  }

  public int unionSize(DocSet other) {
    return this.size() + other.size() - this.intersectionSize(other);
  }

  public DocSet andNot(DocSet other) {
    OpenBitSet newbits = (OpenBitSet)(this.getBits().clone());
    newbits.andNot(other.getBits());
    return new BitDocSet(newbits);
  }

  public int andNotSize(DocSet other) {
    return this.size() - this.intersectionSize(other);
  }

  public Filter getTopFilter() {
    final OpenBitSet bs = getBits();

    return new Filter() {
      @Override
      public DocIdSet getDocIdSet(final AtomicReaderContext context, Bits acceptDocs) {
        AtomicReader reader = context.reader();
        // all Solr DocSets that are used as filters only include live docs
        final Bits acceptDocs2 = acceptDocs == null ? null : (reader.getLiveDocs() == acceptDocs ? null : acceptDocs);

        if (context.isTopLevel) {
          return BitsFilteredDocIdSet.wrap(bs, acceptDocs);
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
                pos = bs.nextSetBit(pos+1);
                return adjustedDoc = (pos>=0 && pos<max) ? pos-base : NO_MORE_DOCS;
              }

              @Override
              public int advance(int target) {
                if (target==NO_MORE_DOCS) return adjustedDoc=NO_MORE_DOCS;
                pos = bs.nextSetBit(target+base);
                return adjustedDoc = (pos>=0 && pos<max) ? pos-base : NO_MORE_DOCS;
              }
            };
          }

          @Override
          public boolean isCacheable() {
            return true;
          }

          @Override
          public Bits bits() {
            // sparse filters should not use random access
            return null;
          }

        }, acceptDocs2);
      }
    };
  }

  public void setBitsOn(OpenBitSet target) {
    DocIterator iter = iterator();
    while (iter.hasNext()) {
      target.fastSet(iter.nextDoc());
    }
  }

}
