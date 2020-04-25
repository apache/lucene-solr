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
 * A results comparator that compares the name of facet value buckets, which is the string value of the facet value.
 */
public class FacetValueComparator extends FacetResultsComparator {

  /**
   * Create a facet value comparator.
   */
  public FacetValueComparator() {
    super();
  }

  @Override
  public int compare(FacetBucket b1, FacetBucket b2) {
      return b1.getFacetValue().compareTo(b2.getFacetValue()) * resultMult;
  }
}