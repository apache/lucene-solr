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

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;

/**
 * An immutable ordered set of Lucene Document Ids.
 * It's similar to a Lucene {@link org.apache.lucene.search.DocIdSet}.
 *
 * <p>
 * WARNING: Any DocSet returned from SolrIndexSearcher should <b>not</b> be modified as it may have been retrieved from
 * a cache and could be shared.
 * </p>
 */
public abstract class DocSet implements Accountable, Cloneable /* extends Collection<Integer> */ {

  // package accessible; guarantee known implementations
  DocSet() {
    assert this instanceof BitDocSet || this instanceof SortedIntDocSet;
  }

  /**
   * Returns the number of documents in the set.
   */
  public abstract int size();

  /**
   * Returns true if a document is in the DocSet.
   * If you want to be guaranteed fast random access, use {@link #getBits()} instead.
   */
  public abstract boolean exists(int docid);

  /**
   * Returns an ordered iterator of the documents in the set.  Any scoring information is meaningless.
   */
  //TODO switch to DocIdSetIterator in Solr 9?
  public abstract DocIterator iterator();

  /**
   * Returns the intersection of this set with another set.  Neither set is modified - a new DocSet is
   * created and returned.
   * @return a DocSet representing the intersection
   */
  public abstract DocSet intersection(DocSet other);

  /**
   * Returns the number of documents of the intersection of this set with another set.
   * May be more efficient than actually creating the intersection and then getting its size.
   */
  public abstract int intersectionSize(DocSet other);

  /** Returns true if these sets have any elements in common */
  public abstract boolean intersects(DocSet other);

  /**
   * Returns the union of this set with another set.  Neither set is modified - a new DocSet is
   * created and returned.
   * @return a DocSet representing the union
   */
  public abstract DocSet union(DocSet other);

  /**
   * Returns the number of documents of the union of this set with another set.
   * May be more efficient than actually creating the union and then getting its size.
   */
  public int unionSize(DocSet other) {
    return this.size() + other.size() - this.intersectionSize(other);
  }

  /**
   * Returns the documents in this set that are not in the other set. Neither set is modified - a new DocSet is
   * created and returned.
   * @return a DocSet representing this AND NOT other
   */
  public abstract DocSet andNot(DocSet other);

  /**
   * Returns the number of documents in this set that are not in the other set.
   */
  public int andNotSize(DocSet other) {
    return this.size() - this.intersectionSize(other);
  }

  /**
   * Returns a Filter for use in Lucene search methods, assuming this DocSet
   * was generated from the top-level MultiReader that the Lucene search
   * methods will be invoked with.
   */
  public abstract Filter getTopFilter();

  /**
   * Adds all the docs from this set to the target. The target should be
   * sized large enough to accommodate all of the documents before calling this
   * method.
   */
  public abstract void addAllTo(FixedBitSet target);


  public abstract DocSet clone();

  public static final DocSet EMPTY = new SortedIntDocSet(new int[0], 0);

  /**
   * A {@link Bits} that has fast random access (as is generally required of Bits).
   * It may be necessary to do work to build this.
   */
  public abstract Bits getBits();

  // internal only
  protected abstract FixedBitSet getFixedBitSet();

  // internal only
  protected abstract FixedBitSet getFixedBitSetClone();

}
