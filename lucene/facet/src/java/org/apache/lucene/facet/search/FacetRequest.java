package org.apache.lucene.facet.search;

import org.apache.lucene.facet.params.CategoryListParams.OrdinalPolicy;
import org.apache.lucene.facet.params.FacetIndexingParams;
import org.apache.lucene.facet.range.RangeFacetRequest;
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
 * Defines an aggregation request for a category. Allows specifying the
 * {@link #numResults number of child categories} to return as well as
 * {@link #getSortOrder() which} categories to consider the "top" (highest or
 * lowest ranking ones).
 * <p>
 * If the category being aggregated is hierarchical, you can also specify the
 * {@link #setDepth(int) depth} up which to aggregate child categories as well
 * as how the result should be {@link #setResultMode(ResultMode) constructed}.
 * 
 * @lucene.experimental
 */
public abstract class FacetRequest {
  
  /**
   * When {@link FacetRequest#getDepth()} is greater than 1, defines the
   * structure of the result as well as how constraints such as
   * {@link FacetRequest#numResults} and {@link FacetRequest#getNumLabel()} are
   * applied.
   */
  public enum ResultMode { 
    /**
     * Constraints are applied per node, and the result has a full tree
     * structure. Default result mode.
     */
    PER_NODE_IN_TREE, 
    
    /**
     * Constraints are applied globally, on total number of results, and the
     * result has a flat structure.
     */
    GLOBAL_FLAT
  }
  
  /**
   * Defines which categories to return. If {@link #DESCENDING} (the default),
   * the highest {@link FacetRequest#numResults} weighted categories will be
   * returned, otherwise the lowest ones.
   */
  public enum SortOrder { ASCENDING, DESCENDING }

  /** The category being aggregated in this facet request. */
  public final CategoryPath categoryPath;
  
  /** The number of child categories to return for {@link #categoryPath}. */
  public final int numResults;
  
  private int numLabel;
  private int depth = 1;
  private SortOrder sortOrder = SortOrder.DESCENDING;
  private ResultMode resultMode = ResultMode.PER_NODE_IN_TREE;
  
  // Computed at construction; based on categoryPath and numResults.
  private final int hashCode;
  
  /**
   * Constructor with the given category to aggregate and the number of child
   * categories to return.
   * 
   * @param path
   *          the category to aggregate. Cannot be {@code null}.
   * @param numResults
   *          the number of child categories to return. If set to
   *          {@code Integer.MAX_VALUE}, all immediate child categories will be
   *          returned. Must be greater than 0.
   */
  public FacetRequest(CategoryPath path, int numResults) {
    if (numResults <= 0) {
      throw new IllegalArgumentException("num results must be a positive (>0) number: " + numResults);
    }
    if (path == null) {
      throw new IllegalArgumentException("category path cannot be null!");
    }
    categoryPath = path;
    this.numResults = numResults;
    numLabel = numResults;
    hashCode = categoryPath.hashCode() ^ this.numResults;
  }
  
  /**
   * Returns the {@link FacetsAggregator} which can aggregate the categories of
   * this facet request. The aggregator is expected to aggregate category values
   * into {@link FacetArrays}. If the facet request does not support that, e.g.
   * {@link RangeFacetRequest}, it can return {@code null}. Note though that
   * such requests require a dedicated {@link FacetsAccumulator}.
   */
  public abstract FacetsAggregator createFacetsAggregator(FacetIndexingParams fip);
  
  @Override
  public boolean equals(Object o) {
    if (o instanceof FacetRequest) {
      FacetRequest that = (FacetRequest) o;
     return that.hashCode == this.hashCode &&
          that.categoryPath.equals(this.categoryPath) &&
          that.numResults == this.numResults &&
          that.depth == this.depth &&
          that.resultMode == this.resultMode &&
          that.numLabel == this.numLabel &&
          that.sortOrder == this.sortOrder;
    }
    return false;
  }
  
  /**
   * How deeply to look under {@link #categoryPath}. By default, only its
   * immediate children are aggregated (depth=1). If set to
   * {@code Integer.MAX_VALUE}, the entire sub-tree of the category will be
   * aggregated.
   * <p>
   * <b>NOTE:</b> setting depth to 0 means that only the category itself should
   * be aggregated. In that case, make sure to index the category with
   * {@link OrdinalPolicy#ALL_PARENTS}, unless it is not the root category (the
   * dimension), in which case {@link OrdinalPolicy#ALL_BUT_DIMENSION} is fine
   * too.
   */
  public final int getDepth() {
    // TODO an AUTO_EXPAND option could be useful  
    return depth;
  }
  
  /**
   * Allows to specify the number of categories to label. By default all
   * returned categories are labeled.
   * <p>
   * This allows an app to request a large number of results to return, while
   * labeling them on-demand (e.g. when the UI requests to show more
   * categories).
   */
  public final int getNumLabel() {
    return numLabel;
  }
  
  /** Return the requested result mode (defaults to {@link ResultMode#PER_NODE_IN_TREE}. */
  public final ResultMode getResultMode() {
    return resultMode;
  }
  
  /** Return the requested order of results (defaults to {@link SortOrder#DESCENDING}. */
  public final SortOrder getSortOrder() {
    return sortOrder;
  }
  
  @Override
  public int hashCode() {
    return hashCode; 
  }
  
  /**
   * Sets the depth up to which to aggregate facets.
   * 
   * @see #getDepth()
   */
  public void setDepth(int depth) {
    this.depth = depth;
  }
  
  /**
   * Sets the number of categories to label.
   * 
   * @see #getNumLabel()
   */
  public void setNumLabel(int numLabel) {
    this.numLabel = numLabel;
  }
  
  /**
   * Sets the {@link ResultMode} for this request.
   * 
   * @see #getResultMode()
   */
  public void setResultMode(ResultMode resultMode) {
    this.resultMode = resultMode;
  }

  /**
   * Sets the {@link SortOrder} for this request.
   * 
   * @see #getSortOrder()
   */
  public void setSortOrder(SortOrder sortOrder) {
    this.sortOrder = sortOrder;
  }
  
  @Override
  public String toString() {
    return categoryPath.toString() + " nRes=" + numResults + " nLbl=" + numLabel;
  }
  
}
