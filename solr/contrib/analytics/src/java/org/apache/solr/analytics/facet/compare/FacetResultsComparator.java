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
package org.apache.solr.analytics.facet.compare;

import java.util.Comparator;

import org.apache.solr.analytics.facet.SortableFacet.FacetBucket;

/**
 * A comparator used to sort the buckets of facet.
 */
public abstract class FacetResultsComparator implements Comparator<FacetBucket> {
  protected int resultMult;

  /**
   * Create a results comparator assuming an ascending ordering.
   */
  public FacetResultsComparator() {
    setDirection(true);
  }

  /**
   * Set the order direction for comparison.
   *
   * @param ascending whether to compare using an ascending ordering
   */
  public void setDirection(boolean ascending) {
    this.resultMult = ascending ? 1 : -1;
  }

  /**
   * Compare one facet bucket to another.
   *
   * @param b1 the first bucket to compare
   * @param b2 the second bucket to compare
   */
  public abstract int compare(FacetBucket b1, FacetBucket b2);
}