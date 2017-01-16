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
package org.apache.lucene.facet;

import org.apache.lucene.index.Term;
import org.apache.lucene.queries.TermsQuery;
import org.apache.lucene.search.Query;

import java.util.ArrayList;
import java.util.Collection;

/**
 * A multi-terms {@link Query} over a {@link FacetField}.
 * <p>
 * <b>NOTE:</b>This helper class is an alternative to {@link DrillDownQuery}
 * especially in cases where you don't intend to use {@link DrillSideways}
 *
 * @lucene.experimental
 * @see org.apache.lucene.queries.TermsQuery
 */
public class MultiFacetQuery extends TermsQuery {

  /**
   * Creates a new {@code MultiFacetQuery} filtering the query on the given dimension.
   */
  public MultiFacetQuery(final FacetsConfig facetsConfig, final String dimension, final String[]... paths) {
    super(toTerms(facetsConfig.getDimConfig(dimension), dimension, paths));
  }

  /**
   * Creates a new {@code MultiFacetQuery} filtering the query on the given dimension.
   * <p>
   * <b>NOTE:</b>Uses FacetsConfig.DEFAULT_DIM_CONFIG.
   */
  public MultiFacetQuery(final String dimension, final String[]... paths) {
    super(toTerms(FacetsConfig.DEFAULT_DIM_CONFIG, dimension, paths));
  }

  static Collection<Term> toTerms(final FacetsConfig.DimConfig dimConfig, final String dimension,
          final String[]... paths) {
    final Collection<Term> terms = new ArrayList<>(paths.length);
    for (String[] path : paths)
      terms.add(FacetQuery.toTerm(dimConfig, dimension, path));
    return terms;
  }

}
