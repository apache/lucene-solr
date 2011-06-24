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

import org.apache.solr.common.SolrException;
import org.apache.lucene.util.OpenBitSet;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReader.AtomicReaderContext;

import java.io.IOException;

/**
 * <code>DocSet</code> represents an unordered set of Lucene Document Ids.
 *
 * <p>
 * WARNING: Any DocSet returned from SolrIndexSearcher should <b>not</b> be modified as it may have been retrieved from
 * a cache and could be shared.
 * </p>
 *
 *
 * @since solr 0.9
 */
public interface DocSet /* extends Collection<Integer> */ {
  
  /**
   * Adds the specified document if it is not currently in the DocSet
   * (optional operation).
   *
   * @see #addUnique
   * @throws SolrException if the implementation does not allow modifications
   */
  public void add(int doc);

  /**
   * Adds a document the caller knows is not currently in the DocSet
   * (optional operation).
   *
   * <p>
   * This method may be faster then <code>add(doc)</code> in some
   * implementaions provided the caller is certain of the precondition.
   * </p>
   *
   * @see #add
   * @throws SolrException if the implementation does not allow modifications
   */
  public void addUnique(int doc);

  /**
   * Returns the number of documents in the set.
   */
  public int size();

  /**
   * Returns true if a document is in the DocSet.
   */
  public boolean exists(int docid);

  /**
   * Returns an iterator that may be used to iterate over all of the documents in the set.
   *
   * <p>
   * The order of the documents returned by this iterator is
   * non-deterministic, and any scoring information is meaningless
   * </p>
   */
  public DocIterator iterator();

  /**
   * Returns a BitSet view of the DocSet.  Any changes to this BitSet <b>may</b>
   * be reflected in the DocSet, hence if the DocSet is shared or was returned from
   * a SolrIndexSearcher method, it's not safe to modify the BitSet.
   *
   * @return
   * An OpenBitSet with the bit number of every docid set in the set.
   */
  public OpenBitSet getBits();

  /**
   * Returns the approximate amount of memory taken by this DocSet.
   * This is only an approximation and doesn't take into account java object overhead.
   *
   * @return
   * the approximate memory consumption in bytes
   */
  public long memSize();

  /**
   * Returns the intersection of this set with another set.  Neither set is modified - a new DocSet is
   * created and returned.
   * @return a DocSet representing the intersection
   */
  public DocSet intersection(DocSet other);

  /**
   * Returns the number of documents of the intersection of this set with another set.
   * May be more efficient than actually creating the intersection and then getting it's size.
   */
  public int intersectionSize(DocSet other);

  /** Returns true if these sets have any elements in common */
  public boolean intersects(DocSet other);

  /**
   * Returns the union of this set with another set.  Neither set is modified - a new DocSet is
   * created and returned.
   * @return a DocSet representing the union
   */
  public DocSet union(DocSet other);

  /**
   * Returns the number of documents of the union of this set with another set.
   * May be more efficient than actually creating the union and then getting it's size.
   */
  public int unionSize(DocSet other);

  /**
   * Returns the documents in this set that are not in the other set. Neither set is modified - a new DocSet is
   * created and returned.
   * @return a DocSet representing this AND NOT other
   */
  public DocSet andNot(DocSet other);

  /**
   * Returns the number of documents in this set that are not in the other set.
   */
  public int andNotSize(DocSet other);

  /**
   * Returns a Filter for use in Lucene search methods, assuming this DocSet
   * was generated from the top-level MultiReader that the Lucene search
   * methods will be invoked with.
   */
  public Filter getTopFilter();

  /**
   * Takes the docs from this set and sets those bits on the target OpenBitSet.
   * The target should be sized large enough to accommodate all of the documents before calling this method.
   */
  public void setBitsOn(OpenBitSet target);

  public static DocSet EMPTY = new SortedIntDocSet(new int[0], 0);
}

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
      public DocIdSet getDocIdSet(AtomicReaderContext context) throws IOException {
        IndexReader reader = context.reader;

        if (context.isTopLevel) {
          return bs;
        }

        final int base = context.docBase;
        final int maxDoc = reader.maxDoc();
        final int max = base + maxDoc;   // one past the max doc in this segment.

        return new DocIdSet() {
          @Override
          public DocIdSetIterator iterator() throws IOException {
            return new DocIdSetIterator() {
              int pos=base-1;
              int adjustedDoc=-1;

              @Override
              public int docID() {
                return adjustedDoc;
              }

              @Override
              public int nextDoc() throws IOException {
                pos = bs.nextSetBit(pos+1);
                return adjustedDoc = (pos>=0 && pos<max) ? pos-base : NO_MORE_DOCS;
              }

              @Override
              public int advance(int target) throws IOException {
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

        };
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





