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
package org.apache.solr.search.facet;

// Any type of facet request that generates a variable number of buckets
// and the ability to sort by those generated buckets.
abstract class FacetRequestSorted extends FacetRequest {
  long offset;
  long limit;
  /**
   * Number of buckets to request beyond the limit to do internally during initial distributed search.
   * -1 means default heuristic.
   */
  int overrequest = -1;
  /**
   * Number of buckets to fill in beyond the limit to do internally during refinement of distributed search.
   * -1 means default heuristic.
   */
  int overrefine = -1;
  long mincount;
  /**
   * The basic sorting to do on buckets, defaults to {@link FacetRequest.FacetSort#COUNT_DESC}
   * @see #prelim_sort
   */
  FacetSort sort;
  /**
   * An optional "Pre-Sort" that defaults to null.
   * If specified, then the <code>prelim_sort</code> is used as an optimization in place of {@link #sort}
   * during collection, and the full {@link #sort} values are only computed for the top candidate buckets
   * (after refinement)
   */
  FacetSort prelim_sort;
  RefineMethod refine; // null, NONE, or SIMPLE

  @Override
  public RefineMethod getRefineMethod() {
    return refine;
  }

  @Override
  public boolean returnsPartial() {
    return super.returnsPartial() || (limit > 0);
  }
}
