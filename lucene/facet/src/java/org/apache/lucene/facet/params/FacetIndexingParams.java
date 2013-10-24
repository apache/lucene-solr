package org.apache.lucene.facet.params;

import java.util.Collections;
import java.util.List;

import org.apache.lucene.facet.search.FacetArrays;
import org.apache.lucene.facet.taxonomy.CategoryPath;

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

/**
 * Defines parameters that are needed for facets indexing. Note that this class
 * does not have any setters. That's because overriding the default parameters
 * is considered expert. If you wish to override them, simply extend this class
 * and override the relevant getter.
 * 
 * <p>
 * <b>NOTE:</b> This class is also used during faceted search in order to e.g.
 * know which field holds the drill-down terms or the fulltree posting.
 * Therefore this class should be initialized once and you should refrain from
 * changing it. Also note that if you make any changes to it (e.g. suddenly
 * deciding that drill-down terms should be read from a different field) and use
 * it on an existing index, things may not work as expected.
 * 
 * @lucene.experimental
 */
public class FacetIndexingParams {
  
  // the default CLP, can be a singleton
  protected static final CategoryListParams DEFAULT_CATEGORY_LIST_PARAMS = new CategoryListParams();

  /**
   * A {@link FacetIndexingParams} which fixes a single
   * {@link CategoryListParams} with
   * {@link CategoryListParams#DEFAULT_ORDINAL_POLICY}.
   */
  public static final FacetIndexingParams DEFAULT = new FacetIndexingParams();
  
  /**
   * The default delimiter with which {@link CategoryPath#components} are
   * concatenated when written to the index, e.g. as drill-down terms. If you
   * choose to override it by overiding {@link #getFacetDelimChar()}, you should
   * make sure that you return a character that's not found in any path
   * component.
   */
  public static final char DEFAULT_FACET_DELIM_CHAR = '\u001F';
  
  private final int partitionSize = Integer.MAX_VALUE;

  protected final CategoryListParams clParams;

  /**
   * Initializes new default params. You should use this constructor only if you
   * intend to override any of the getters, otherwise you can use
   * {@link #DEFAULT} to save unnecessary object allocations.
   */
  public FacetIndexingParams() {
    this(DEFAULT_CATEGORY_LIST_PARAMS);
  }

  /** Initializes new params with the given {@link CategoryListParams}. */
  public FacetIndexingParams(CategoryListParams categoryListParams) {
    clParams = categoryListParams;
  }

  /**
   * Returns the {@link CategoryListParams} for this {@link CategoryPath}. The
   * default implementation returns the same {@link CategoryListParams} for all
   * categories (even if {@code category} is {@code null}).
   * 
   * @see PerDimensionIndexingParams
   */
  public CategoryListParams getCategoryListParams(CategoryPath category) {
    return clParams;
  }

  /**
   * Copies the text required to execute a drill-down query on the given
   * category to the given {@code char[]}, and returns the number of characters
   * that were written.
   * <p>
   * <b>NOTE:</b> You should make sure that the {@code char[]} is large enough,
   * by e.g. calling {@link CategoryPath#fullPathLength()}.
   */
  public int drillDownTermText(CategoryPath path, char[] buffer) {
    return path.copyFullPath(buffer, 0, getFacetDelimChar());
  }
  
  /**
   * Returns the size of a partition. <i>Partitions</i> allow you to divide
   * (hence, partition) the categories space into small sets to e.g. improve RAM
   * consumption during faceted search. For instance, {@code partitionSize=100K}
   * would mean that if your taxonomy index contains 420K categories, they will
   * be divided into 5 groups and at search time a {@link FacetArrays} will be
   * allocated at the size of the partition.
   * 
   * <p>
   * This is real advanced setting and should be changed with care. By default,
   * all categories are put in one partition. You should modify this setting if
   * you have really large taxonomies (e.g. 1M+ nodes).
   */
  public int getPartitionSize() {
    return partitionSize;
  }
  
  /**
   * Returns a list of all {@link CategoryListParams categoryListParams} that
   * are used for facets indexing.
   */
  public List<CategoryListParams> getAllCategoryListParams() {
    return Collections.singletonList(clParams);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((clParams == null) ? 0 : clParams.hashCode());
    result = prime * result + partitionSize;
    
    for (CategoryListParams clp : getAllCategoryListParams()) {
      result ^= clp.hashCode();
    }
    
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof FacetIndexingParams)) {
      return false;
    }
    FacetIndexingParams other = (FacetIndexingParams) obj;
    if (clParams == null) {
      if (other.clParams != null) {
        return false;
      }
    } else if (!clParams.equals(other.clParams)) {
      return false;
    }
    if (partitionSize != other.partitionSize) {
      return false;
    }
    
    Iterable<CategoryListParams> cLs = getAllCategoryListParams();
    Iterable<CategoryListParams> otherCLs = other.getAllCategoryListParams();
    
    return cLs.equals(otherCLs);
  }

  /**
   * Returns the delimiter character used internally for concatenating category
   * path components, e.g. for drill-down terms.
   */
  public char getFacetDelimChar() {
    return DEFAULT_FACET_DELIM_CHAR;
  }

}
