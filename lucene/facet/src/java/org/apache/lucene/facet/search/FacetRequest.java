package org.apache.lucene.facet.search;

import java.io.IOException;

import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;

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
 * Request to accumulate facet information for a specified facet and possibly 
 * also some of its descendants, upto a specified depth.
 * <p>
 * The facet request additionally defines what information should 
 * be computed within the facet results, if and how should results
 * be ordered, etc.
 * <P>
 * An example facet request is to look at all sub-categories of "Author", and
 * return the 10 with the highest counts (sorted by decreasing count). 
 * 
 * @lucene.experimental
 */
public abstract class FacetRequest {
  
  /**
   * Result structure manner of applying request's limits such as
   * {@link FacetRequest#getNumLabel()} and {@link FacetRequest#numResults}.
   * Only relevant when {@link FacetRequest#getDepth()} is &gt; 1.
   */
  public enum ResultMode { 
    /** Limits are applied per node, and the result has a full tree structure. */
    PER_NODE_IN_TREE, 
    
    /** Limits are applied globally, on total number of results, and the result has a flat structure. */
    GLOBAL_FLAT
  }
  
  /**
   * Specifies which array of {@link FacetArrays} should be used to resolve
   * values. When set to {@link #INT} or {@link #FLOAT}, allows creating an
   * optimized {@link FacetResultsHandler}, which does not call
   * {@link FacetRequest#getValueOf(FacetArrays, int)} for every ordinals.
   * <p>
   * If set to {@link #BOTH}, the {@link FacetResultsHandler} will use
   * {@link FacetRequest#getValueOf(FacetArrays, int)} to resolve ordinal
   * values, although it is recommended that you consider writing a specialized
   * {@link FacetResultsHandler}.
   */
  public enum FacetArraysSource { INT, FLOAT, BOTH }
  
  /** Requested sort order for the results. */
  public enum SortOrder { ASCENDING, DESCENDING }
  
  /**
   * Default depth for facets accumulation.
   * @see #getDepth()
   */
  public static final int DEFAULT_DEPTH = 1;
  
  /**
   * Default result mode
   * @see #getResultMode()
   */
  public static final ResultMode DEFAULT_RESULT_MODE = ResultMode.PER_NODE_IN_TREE;
  
  public final CategoryPath categoryPath;
  public final int numResults;
  
  private int numLabel;
  private int depth;
  private SortOrder sortOrder;
  
  /**
   * Computed at construction, this hashCode is based on two final members
   * {@link CategoryPath} and <code>numResults</code>
   */
  private final int hashCode;
  
  private ResultMode resultMode = DEFAULT_RESULT_MODE;
  
  /**
   * Initialize the request with a given path, and a requested number of facets
   * results. By default, all returned results would be labeled - to alter this
   * default see {@link #setNumLabel(int)}.
   * <p>
   * <b>NOTE:</b> if <code>numResults</code> is given as
   * <code>Integer.MAX_VALUE</code> than all the facet results would be
   * returned, without any limit.
   * <p>
   * <b>NOTE:</b> it is assumed that the given {@link CategoryPath} is not
   * modified after construction of this object. Otherwise, some things may not
   * function properly, e.g. {@link #hashCode()}.
   * 
   * @throws IllegalArgumentException if numResults is &le; 0
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
    depth = DEFAULT_DEPTH;
    sortOrder = SortOrder.DESCENDING;
    
    hashCode = categoryPath.hashCode() ^ this.numResults;
  }
  
  /**
   * Create an aggregator for this facet request. Aggregator action depends on
   * request definition. For a count request, it will usually increment the
   * count for that facet.
   * 
   * @param useComplements
   *          whether the complements optimization is being used for current
   *          computation.
   * @param arrays
   *          provider for facet arrays in use for current computation.
   * @param taxonomy
   *          reader of taxonomy in effect.
   * @throws IOException If there is a low-level I/O error.
   */
  public Aggregator createAggregator(boolean useComplements, FacetArrays arrays, TaxonomyReader taxonomy) 
      throws IOException {
    throw new UnsupportedOperationException("this FacetRequest does not support this type of Aggregator anymore; " +
        "you should override FacetsAccumulator to return the proper FacetsAggregator");
  }
  
  @Override
  public boolean equals(Object o) {
    if (o instanceof FacetRequest) {
      FacetRequest that = (FacetRequest)o;
      return that.hashCode == this.hashCode &&
          that.categoryPath.equals(this.categoryPath) &&
          that.numResults == this.numResults &&
          that.depth == this.depth &&
          that.resultMode == this.resultMode &&
          that.numLabel == this.numLabel;
    }
    return false;
  }
  
  /**
   * How deeply to look under the given category. If the depth is 0,
   * only the category itself is counted. If the depth is 1, its immediate
   * children are also counted, and so on. If the depth is Integer.MAX_VALUE,
   * all the category's descendants are counted.<br>
   */
  public final int getDepth() {
    // TODO add AUTO_EXPAND option  
    return depth;
  }
  
  /**
   * Returns the {@link FacetArraysSource} this {@link FacetRequest} uses in
   * {@link #getValueOf(FacetArrays, int)}.
   */
  public abstract FacetArraysSource getFacetArraysSource();
  
  /**
   * If getNumLabel() &lt; getNumResults(), only the first getNumLabel() results
   * will have their category paths calculated, and the rest will only be
   * available as ordinals (category numbers) and will have null paths.
   * <P>
   * If Integer.MAX_VALUE is specified, all results are labled.
   * <P>
   * The purpose of this parameter is to avoid having to run the whole faceted
   * search again when the user asks for more values for the facet; The
   * application can ask (getNumResults()) for more values than it needs to
   * show, but keep getNumLabel() only the number it wants to immediately show.
   * The slow-down caused by finding more values is negligible, because the
   * slowest part - finding the categories' paths, is avoided.
   * <p>
   * Depending on the {@link #getResultMode() LimitsMode}, this limit is applied
   * globally or per results node. In the global mode, if this limit is 3, only
   * 3 top results would be labeled. In the per-node mode, if this limit is 3, 3
   * top children of {@link #categoryPath the target category} would be labeled,
   * as well as 3 top children of each of them, and so forth, until the depth
   * defined by {@link #getDepth()}.
   * 
   * @see #getResultMode()
   */
  public final int getNumLabel() {
    return numLabel;
  }
  
  /** Return the requested result mode. */
  public final ResultMode getResultMode() {
    return resultMode;
  }
  
  /** Return the requested order of results. */
  public final SortOrder getSortOrder() {
    return sortOrder;
  }
  
  /**
   * Return the value of a category used for facets computations for this
   * request. For a count request this would be the count for that facet, i.e.
   * an integer number. but for other requests this can be the result of a more
   * complex operation, and the result can be any double precision number.
   * Having this method with a general name <b>value</b> which is double
   * precision allows to have more compact API and code for handling counts and
   * perhaps other requests (such as for associations) very similarly, and by
   * the same code and API, avoiding code duplication.
   * 
   * @param arrays
   *          provider for facet arrays in use for current computation.
   * @param idx
   *          an index into the count arrays now in effect in
   *          <code>arrays</code>. E.g., for ordinal number <i>n</i>, with
   *          partition, of size <i>partitionSize</i>, now covering <i>n</i>,
   *          <code>getValueOf</code> would be invoked with <code>idx</code>
   *          being <i>n</i> % <i>partitionSize</i>.
   */
  // TODO perhaps instead of getValueOf we can have a postProcess(FacetArrays)
  // That, together with getFacetArraysSource should allow ResultHandlers to
  // efficiently obtain the values from the arrays directly
  public abstract double getValueOf(FacetArrays arrays, int idx);
  
  @Override
  public int hashCode() {
    return hashCode; 
  }
  
  public void setDepth(int depth) {
    this.depth = depth;
  }
  
  public void setNumLabel(int numLabel) {
    this.numLabel = numLabel;
  }
  
  /**
   * @param resultMode the resultMode to set
   * @see #getResultMode()
   */
  public void setResultMode(ResultMode resultMode) {
    this.resultMode = resultMode;
  }
  
  public void setSortOrder(SortOrder sortOrder) {
    this.sortOrder = sortOrder;
  }
  
  @Override
  public String toString() {
    return categoryPath.toString()+" nRes="+numResults+" nLbl="+numLabel;
  }
  
}
