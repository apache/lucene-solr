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
import org.apache.solr.common.SolrException;

/**
 * <code>DocSet</code> represents an unordered set of Lucene Document Ids.
 *
 * <p>
 * WARNING: Any DocSet returned from SolrIndexSearcher should <b>not</b> be modified as it may have been retrieved from
 * a cache and could be shared.
 * </p>
 *
 * @since solr 0.9
 */
public interface DocSet extends Accountable, Cloneable /* extends Collection<Integer> */ {
  
  /**
   * Adds the specified document if it is not currently in the DocSet
   * (optional operation).
   *
   * @see #addUnique
   * @throws SolrException if the implementation does not allow modifications
   */
  @Deprecated // to-be read-only, see SOLR-14256
  public void add(int doc);

  /**
   * Adds a document the caller knows is not currently in the DocSet
   * (optional operation).
   *
   * <p>
   * This method may be faster then <code>add(doc)</code> in some
   * implementations provided the caller is certain of the precondition.
   * </p>
   *
   * @see #add
   * @throws SolrException if the implementation does not allow modifications
   */
  @Deprecated // to-be read-only, see SOLR-14256
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
   * Returns the intersection of this set with another set.  Neither set is modified - a new DocSet is
   * created and returned.
   * @return a DocSet representing the intersection
   */
  public DocSet intersection(DocSet other);

  /**
   * Returns the number of documents of the intersection of this set with another set.
   * May be more efficient than actually creating the intersection and then getting its size.
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
   * May be more efficient than actually creating the union and then getting its size.
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
   * Adds all the docs from this set to the target set. The target should be
   * sized large enough to accommodate all of the documents before calling this
   * method.
   */
  @Deprecated // to-be read-only, see SOLR-14256
  public void addAllTo(DocSet target);

  public DocSet clone();

  public static DocSet EMPTY = new SortedIntDocSet(new int[0], 0);
}
