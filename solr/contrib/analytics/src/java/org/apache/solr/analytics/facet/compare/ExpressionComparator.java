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

import org.apache.solr.analytics.facet.SortableFacet.FacetBucket;

/**
 * A comparator used to sort the facet-value buckets of facet.
 */
public class ExpressionComparator<T extends Comparable<T>> extends FacetResultsComparator {
  private final String expression;

  /**
   * Create an entry comparator comparing the given expression.
   *
   * @param expression the name of the expression results to compare
   */
  public ExpressionComparator(String expression) {
    this.expression = expression;
  }

  @SuppressWarnings("unchecked")
  public int compare(FacetBucket b1, FacetBucket b2) {
    T t1 = (T)b1.getResult(expression);
    T t2 = (T)b2.getResult(expression);
    if (t1 == null || t2 == null) {
      return Boolean.compare(t2 == null, t1 == null) * resultMult;
    } else {
      return t1.compareTo(t2) * resultMult;
    }
  }
}