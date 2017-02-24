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

import java.util.ArrayList;
import java.util.Collection;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.util.BytesRef;

/**
 * A multi-terms {@link Query} over a {@link FacetField}.
 * <p>
 * <b>NOTE:</b>This helper class is an alternative to {@link DrillDownQuery}
 * especially in cases where you don't intend to use {@link DrillSideways}
 *
 * @lucene.experimental
 * @see org.apache.lucene.search.TermInSetQuery
 */
public class MultiFacetQuery extends TermInSetQuery {

  /**
   * Creates a new {@code MultiFacetQuery} filtering the query on the given dimension.
   */
  public MultiFacetQuery(final FacetsConfig facetsConfig, final String dimension, final String[]... paths) {
    super(facetsConfig.getDimConfig(dimension).indexFieldName, toTerms(dimension, paths));
  }

  /**
   * Creates a new {@code MultiFacetQuery} filtering the query on the given dimension.
   * <p>
   * <b>NOTE:</b>Uses FacetsConfig.DEFAULT_DIM_CONFIG.
   */
  public MultiFacetQuery(final String dimension, final String[]... paths) {
    super(FacetsConfig.DEFAULT_DIM_CONFIG.indexFieldName, toTerms(dimension, paths));
  }

  static Collection<BytesRef> toTerms(final String dimension, final String[]... paths) {
    final Collection<BytesRef> terms = new ArrayList<>(paths.length);
    for (String[] path : paths)
      terms.add(new BytesRef(FacetsConfig.pathToString(dimension, path)));
    return terms;
  }

}
