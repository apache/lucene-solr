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
package org.apache.solr.analytics.facet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.solr.analytics.facet.compare.FacetResultsComparator;
import org.apache.solr.analytics.util.AnalyticsResponseHeadings;
import org.apache.solr.common.util.NamedList;

import com.google.common.collect.Iterables;

/**
 * A facet that can be sorted by either the facet value or an expression value.
 */
public abstract class SortableFacet extends AnalyticsFacet {
  protected FacetSortSpecification sort = null;

  protected SortableFacet(String name) {
    super(name);
  }

  @Override
  public NamedList<Object> createOldResponse() {
    final NamedList<Object> results = new NamedList<>();
    // Export each expression in the bucket.
    for (FacetBucket bucket : getBuckets()) {
      results.add(bucket.getFacetValue(), new NamedList<>(bucket.getResults()));
    }
    return results;
  }

  @Override
  public Iterable<Map<String,Object>> createResponse() {
    final LinkedList<Map<String,Object>> results = new LinkedList<>();
    // Export each expression in the bucket.
    for (FacetBucket bucket : getBuckets()) {
      Map<String, Object> bucketMap = new HashMap<>();
      bucketMap.put(AnalyticsResponseHeadings.FACET_VALUE, bucket.getFacetValue());
      bucketMap.put(AnalyticsResponseHeadings.RESULTS, bucket.getResults());
      results.add(bucketMap);
    }
    return results;
  }

  private Iterable<FacetBucket> getBuckets() {
    final List<FacetBucket> facetResults = new ArrayList<>();
    reductionData.forEach((facetVal, dataCol) -> {
      collectionManager.setData(dataCol);
      facetResults.add(new FacetBucket(facetVal,expressionCalculator.getResults()));
    });

    return applyOptions(facetResults);
  }

  /**
   * Apply the sorting options to the given facet results.
   *
   * @param facetResults to apply sorting options to
   * @return the sorted results
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  protected Iterable<FacetBucket> applyOptions(List<FacetBucket> facetResults) {
    // Sorting the buckets if a sort specification is provided
    if (sort == null || facetResults.isEmpty()) {
      return facetResults;
    }
    Comparator comp = sort.getComparator();
    Collections.sort(facetResults, comp);

    Iterable<FacetBucket> facetResultsIter = facetResults;
    // apply the limit
    if (sort.getLimit() > 0) {
      if (sort.getOffset() > 0) {
        facetResultsIter = Iterables.skip(facetResultsIter, sort.getOffset());
      }
      facetResultsIter = Iterables.limit(facetResultsIter, sort.getLimit());
    } else if (sort.getLimit() == 0) {
      return new LinkedList<FacetBucket>();
    }
    return facetResultsIter;
  }

  /**
   * Specifies how to sort the buckets of a sortable facet.
   */
  public static class FacetSortSpecification {
    private FacetResultsComparator comparator;
    protected int limit;
    protected int offset;

    public FacetSortSpecification(FacetResultsComparator comparator, int limit, int offset) {
      this.comparator = comparator;
      this.limit = limit;
      this.offset = offset;
    }

    public FacetResultsComparator getComparator() {
      return comparator;
    }

    /**
     * Get the maximum number of buckets to be returned.
     *
     * @return the limit
     */
    public int getLimit() {
      return limit;
    }
    /**
     * Set the maximum number of buckets to be returned.
     *
     * @param limit the maximum number of buckets
     */
    public void setLimit(int limit) {
      this.limit = limit;
    }

    /**
     * Get the first bucket to return, has to be used with the {@code limit} option.
     *
     * @return the bucket offset
     */
    public int getOffset() {
      return offset;
    }
  }

  public SortableFacet.FacetSortSpecification getSort() {
    return sort;
  }

  public void setSort(SortableFacet.FacetSortSpecification sort) {
    this.sort = sort;
  }

  public static class FacetBucket {
    private final String facetValue;
    private final Map<String,Object> expressionResults;

    public FacetBucket(String facetValue, Map<String,Object> expressionResults) {
      this.facetValue = facetValue;
      this.expressionResults = expressionResults;
    }

    public Object getResult(String expression) {
      return expressionResults.get(expression);
    }

    public Map<String,Object> getResults() {
      return expressionResults;
    }

    public String getFacetValue() {
      return facetValue;
    }
  }
}