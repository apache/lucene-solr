package org.apache.lucene.facet.range;

import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.Filter;

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

  /** Returns a new {@link Filter} accepting only documents
   *  in this range.  Note that this filter is not
   *  efficient: it's a linear scan of all docs, testing
   *  each value.  If the {@link ValueSource} is static,
   *  e.g. an indexed numeric field, then it's more
   *  efficient to use {@link NumericRangeFilter}. */
  public abstract Filter getFilter(final ValueSource valueSource);

  /** Invoke this for a useless range. */
  protected void failNoMatch() {
    throw new IllegalArgumentException("range \"" + label + "\" matches nothing");
  }
}
