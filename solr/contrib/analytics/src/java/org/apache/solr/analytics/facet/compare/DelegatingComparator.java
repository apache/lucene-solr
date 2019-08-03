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

import java.util.Collection;

import org.apache.solr.analytics.facet.SortableFacet.FacetBucket;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;

/**
 * A comparator used to sort the facet-value buckets of facet, using the delegate comparator if two values are equal.
 */
public class DelegatingComparator extends FacetResultsComparator {
  private final Iterable<FacetResultsComparator> comparators;

  /**
   * Create a delegating results comparator. This comparator will in succession use the given comparators, continuing if the values are equal.
   * Two buckets are considered equal if and only if all comparators find them equal
   *
   * @param comparators the comparators to use in succession
   */
  private DelegatingComparator(Iterable<FacetResultsComparator> comparators) {
    this.comparators = comparators;
  }

  public static FacetResultsComparator joinComparators(Collection<FacetResultsComparator> comparators) throws SolrException {
    if (comparators.size() == 0) {
      throw new SolrException(ErrorCode.BAD_REQUEST,"A sort must have at least 1 comparator criteria.");
    } else if (comparators.size() == 1) {
      return comparators.iterator().next();
    } else {
      return new DelegatingComparator(comparators);
    }
  }

  @Override
  public int compare(FacetBucket b1, FacetBucket b2) {
    int val = 0;
    for (FacetResultsComparator comparator : comparators) {
      val = comparator.compare(b1, b2);
      if (val != 0) {
        break;
      }
    }
    return val;
  }
}