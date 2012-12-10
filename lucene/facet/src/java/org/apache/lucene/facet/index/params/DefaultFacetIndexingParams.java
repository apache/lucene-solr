package org.apache.lucene.facet.index.params;

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.facet.index.categorypolicy.OrdinalPolicy;
import org.apache.lucene.facet.index.categorypolicy.PathPolicy;
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
 * Default implementation for {@link FacetIndexingParams}.
 * <p>
 * Getters for <em>partition-size</em>, {@link OrdinalPolicy} and
 * {@link PathPolicy} are all final, and so the proper way to modify them when
 * extending this class is through {@link #fixedPartitionSize()},
 * {@link #fixedOrdinalPolicy()} or {@link #fixedPathPolicy()} accordingly.
 * 
 * @lucene.experimental
 */
public class DefaultFacetIndexingParams implements FacetIndexingParams {

  /**
   * delimiter between a categories in a path, e.g. Products FACET_DELIM
   * Consumer FACET_DELIM Tv. This should be a character not found in any path
   * component
   */
  public static final char DEFAULT_FACET_DELIM_CHAR = '\uF749';

  private final CategoryListParams clpParams;
  private final OrdinalPolicy ordinalPolicy;
  private final PathPolicy pathPolicy;
  private final int partitionSize;

  public DefaultFacetIndexingParams() {
    this(new CategoryListParams());
  }

  public DefaultFacetIndexingParams(CategoryListParams categoryListParams) {
    clpParams = categoryListParams;
    ordinalPolicy = fixedOrdinalPolicy();
    pathPolicy = fixedPathPolicy();
    partitionSize = fixedPartitionSize();
  }

  public CategoryListParams getCategoryListParams(CategoryPath category) {
    return clpParams;
  }

  public int drillDownTermText(CategoryPath path, char[] buffer) {
    return path.copyToCharArray(buffer, 0, -1, getFacetDelimChar());
  }

  /**
   * "fixed" partition size. 
   * @see #getPartitionSize()
   */
  protected int fixedPartitionSize() {
    return Integer.MAX_VALUE;
  }
  
  /**
   * "fixed" ordinal policy. 
   * @see #getOrdinalPolicy()
   */
  protected OrdinalPolicy fixedOrdinalPolicy() {
    return OrdinalPolicy.ALL_PARENTS;
  }
  
  /**
   * "fixed" path policy. 
   * @see #getPathPolicy()
   */
  protected PathPolicy fixedPathPolicy() {
    return PathPolicy.ALL_CATEGORIES;
  }
  
  public final int getPartitionSize() {
    return partitionSize;
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.lucene.facet.index.params.FacetIndexingParams#getAllCategoryListParams
   * ()
   */
  public Iterable<CategoryListParams> getAllCategoryListParams() {
    List<CategoryListParams> res = new ArrayList<CategoryListParams>();
    res.add(clpParams);
    return res;
  }

  public final OrdinalPolicy getOrdinalPolicy() {
    return ordinalPolicy;
  }

  public final PathPolicy getPathPolicy() {
    return pathPolicy;
  }

  /* (non-Javadoc)
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result
        + ((clpParams == null) ? 0 : clpParams.hashCode());
    result = prime * result
        + ((ordinalPolicy == null) ? 0 : ordinalPolicy.hashCode());
    result = prime * result + partitionSize;
    result = prime * result
        + ((pathPolicy == null) ? 0 : pathPolicy.hashCode());
    
    for (CategoryListParams clp: getAllCategoryListParams()) {
      result ^= clp.hashCode();
    }
    
    return result;
  }

  /* (non-Javadoc)
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof DefaultFacetIndexingParams)) {
      return false;
    }
    DefaultFacetIndexingParams other = (DefaultFacetIndexingParams) obj;
    if (clpParams == null) {
      if (other.clpParams != null) {
        return false;
      }
    } else if (!clpParams.equals(other.clpParams)) {
      return false;
    }
    if (ordinalPolicy == null) {
      if (other.ordinalPolicy != null) {
        return false;
      }
    } else if (!ordinalPolicy.equals(other.ordinalPolicy)) {
      return false;
    }
    if (partitionSize != other.partitionSize) {
      return false;
    }
    if (pathPolicy == null) {
      if (other.pathPolicy != null) {
        return false;
      }
    } else if (!pathPolicy.equals(other.pathPolicy)) {
      return false;
    }
    
    Iterable<CategoryListParams> cLs = getAllCategoryListParams();
    Iterable<CategoryListParams> otherCLs = other.getAllCategoryListParams();
    
    return cLs.equals(otherCLs);
  }

  /**
   * Use {@link #DEFAULT_FACET_DELIM_CHAR} as the delimiter.
   */
  public char getFacetDelimChar() {
    return DEFAULT_FACET_DELIM_CHAR;
  }

}
