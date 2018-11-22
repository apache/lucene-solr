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
package org.apache.lucene.facet.taxonomy;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.TwoPhaseCommit;

/**
 * TaxonomyWriter is the interface which the faceted-search library uses
 * to dynamically build the taxonomy at indexing time.
 * <P>
 * Notes about concurrent access to the taxonomy:
 * <P>
 * An implementation must allow multiple readers and a single writer to be
 * active concurrently. Readers follow so-called "point in time" semantics,
 * i.e., a reader object will only see taxonomy entries which were available
 * at the time it was created. What the writer writes is only available to
 * (new) readers after the writer's commit() is called.
 * <P>
 * Faceted search keeps two indices - namely Lucene's main index, and this
 * taxonomy index. When one or more readers are active concurrently with the
 * writer, care must be taken to avoid an inconsistency between the state of
 * these two indices: When writing to the indices, the taxonomy must always
 * be committed to disk *before* the main index, because the main index
 * refers to categories listed in the taxonomy.
 * Such control can best be achieved by turning off the main index's
 * "autocommit" feature, and explicitly calling commit() for both indices
 * (first for the taxonomy, then for the main index).
 * In old versions of Lucene (2.2 or earlier), when autocommit could not be
 * turned off, a more complicated solution needs to be used. E.g., use
 * some sort of (possibly inter-process) locking to ensure that a reader
 * is being opened only right after both indices have been flushed (and
 * before anything else is written to them).
 * 
 * @lucene.experimental
 */
public interface TaxonomyWriter extends Closeable, TwoPhaseCommit {
  
  /**
   * addCategory() adds a category with a given path name to the taxonomy,
   * and returns its ordinal. If the category was already present in
   * the taxonomy, its existing ordinal is returned.
   * <P>
   * Before adding a category, addCategory() makes sure that all its
   * ancestor categories exist in the taxonomy as well. As result, the
   * ordinal of a category is guaranteed to be smaller then the ordinal of
   * any of its descendants. 
   */ 
  public int addCategory(FacetLabel categoryPath) throws IOException;
  
  /**
   * getParent() returns the ordinal of the parent category of the category
   * with the given ordinal.
   * <P>
   * When a category is specified as a path name, finding the path of its
   * parent is as trivial as dropping the last component of the path.
   * getParent() is functionally equivalent to calling getPath() on the
   * given ordinal, dropping the last component of the path, and then calling
   * getOrdinal() to get an ordinal back. 
   * <P>
   * If the given ordinal is the ROOT_ORDINAL, an INVALID_ORDINAL is returned.
   * If the given ordinal is a top-level category, the ROOT_ORDINAL is returned.
   * If an invalid ordinal is given (negative or beyond the last available
   * ordinal), an IndexOutOfBoundsException is thrown. However, it is
   * expected that getParent will only be called for ordinals which are
   * already known to be in the taxonomy.
   * TODO (Facet): instead of a getParent(ordinal) method, consider having a
   * <P>
   * getCategory(categorypath, prefixlen) which is similar to addCategory
   * except it doesn't add new categories; This method can be used to get
   * the ordinals of all prefixes of the given category, and it can use
   * exactly the same code and cache used by addCategory() so it means less code.
   */
  public int getParent(int ordinal) throws IOException;

  /**
   * getSize() returns the number of categories in the taxonomy.
   * <P>
   * Because categories are numbered consecutively starting with 0, it
   * means the taxonomy contains ordinals 0 through getSize()-1.
   * <P>
   * Note that the number returned by getSize() is often slightly higher
   * than the number of categories inserted into the taxonomy; This is
   * because when a category is added to the taxonomy, its ancestors
   * are also added automatically (including the root, which always get
   * ordinal 0).
   */
  public int getSize();

  /**
   * Sets the commit user data iterable.  See {@link IndexWriter#setLiveCommitData}.
   */
  public void setLiveCommitData(Iterable<Map.Entry<String,String>> commitUserData);

  /**
   * Returns the commit user data iterable that was set on
   * {@link #setLiveCommitData(Iterable)}.
   */
  public Iterable<Map.Entry<String,String>> getLiveCommitData();
  
}
