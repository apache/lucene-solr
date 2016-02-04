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
package org.apache.lucene.facet.range;

import org.apache.lucene.facet.DrillDownQuery; // javadocs
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.FilteredQuery; // javadocs
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;

/** Base class for a single labeled range.
 *
 *  @lucene.experimental */
public abstract class Range {

  /** Label that identifies this range. */
  public final String label;

  /** Sole constructor. */
  protected Range(String label) {
    if (label == null) {
      throw new NullPointerException("label cannot be null");
    }
    this.label = label;
  }

  /** Returns a new {@link Query} accepting only documents
   *  in this range.  This query might not be very efficient
   *  when run on its own since it is optimized towards
   *  random-access, so it is best used either with
   *  {@link DrillDownQuery#add(String, Query) DrillDownQuery}
   *  or when intersected with another query that can lead the
   *  iteration.  If the {@link ValueSource} is static, e.g. an
   *  indexed numeric field, then it may be more efficient to use
   *  {@link NumericRangeQuery}. The provided fastMatchQuery,
   *  if non-null, will first be consulted, and only if
   *  that is set for each document will the range then be
   *  checked. */
  public abstract Query getQuery(Query fastMatchQuery, ValueSource valueSource);

  /** Returns a new {@link Query} accepting only documents
   *  in this range.  This query might not be very efficient
   *  when run on its own since it is optimized towards
   *  random-access, so it is best used either with
   *  {@link DrillDownQuery#add(String, Query) DrillDownQuery}
   *  or when intersected with another query that can lead the
   *  iteration.  If the {@link ValueSource} is static, e.g. an
   *  indexed numeric field, then it may be more efficient to
   *  use {@link NumericRangeQuery}. */
  public Query getQuery(ValueSource valueSource) {
    return getQuery(null, valueSource);
  }

  /** Invoke this for a useless range. */
  protected void failNoMatch() {
    throw new IllegalArgumentException("range \"" + label + "\" matches nothing");
  }
}
