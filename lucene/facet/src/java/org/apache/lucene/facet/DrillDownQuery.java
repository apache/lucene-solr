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

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;

/**
 * A {@link Query} for drill-down over facet categories. You
 * should call {@link #add(String, String...)} for every group of categories you
 * want to drill-down over.
 * <p>
 * <b>NOTE:</b> if you choose to create your own {@link Query} by calling
 * {@link #term}, it is recommended to wrap it in a {@link BoostQuery}
 * with a boost of {@code 0.0f},
 * so that it does not affect the scores of the documents.
 * 
 * @lucene.experimental
 */
public final class DrillDownQuery extends Query {

  /** Creates a drill-down term. */
  public static Term term(String field, String dim, String... path) {
    return new Term(field, FacetsConfig.pathToString(dim, path));
  }

  private final FacetsConfig config;
  private final Query baseQuery;
  private final List<BooleanQuery.Builder> dimQueries = new ArrayList<>();
  private final Map<String,Integer> drillDownDims = new LinkedHashMap<>();

  /** Used by clone() and DrillSideways */
  DrillDownQuery(FacetsConfig config, Query baseQuery, List<BooleanQuery.Builder> dimQueries, Map<String,Integer> drillDownDims) {
    this.baseQuery = baseQuery;
    this.dimQueries.addAll(dimQueries);
    this.drillDownDims.putAll(drillDownDims);
    this.config = config;
  }

  /** Used by DrillSideways */
  DrillDownQuery(FacetsConfig config, Query filter, DrillDownQuery other) {
    this.baseQuery = new BooleanQuery.Builder()
        .add(other.baseQuery == null ? new MatchAllDocsQuery() : other.baseQuery, Occur.MUST)
        .add(filter, Occur.FILTER)
        .build();
    this.dimQueries.addAll(other.dimQueries);
    this.drillDownDims.putAll(other.drillDownDims);
    this.config = config;
  }

  /** Creates a new {@code DrillDownQuery} without a base query, 
   *  to perform a pure browsing query (equivalent to using
   *  {@link MatchAllDocsQuery} as base). */
  public DrillDownQuery(FacetsConfig config) {
    this(config, null);
  }
  
  /** Creates a new {@code DrillDownQuery} over the given base query. Can be
   *  {@code null}, in which case the result {@link Query} from
   *  {@link #rewrite(IndexReader)} will be a pure browsing query, filtering on
   *  the added categories only. */
  public DrillDownQuery(FacetsConfig config, Query baseQuery) {
    this.baseQuery = baseQuery;
    this.config = config;
  }

  /** Adds one dimension of drill downs; if you pass the same
   *  dimension more than once it is OR'd with the previous
   *  cofnstraints on that dimension, and all dimensions are
   *  AND'd against each other and the base query. */
  public void add(String dim, String... path) {
    String indexedField = config.getDimConfig(dim).indexFieldName;
    add(dim, new TermQuery(term(indexedField, dim, path)));
  }

  /** Expert: add a custom drill-down subQuery.  Use this
   *  when you have a separate way to drill-down on the
   *  dimension than the indexed facet ordinals. */
  public void add(String dim, Query subQuery) {
    assert drillDownDims.size() == dimQueries.size();
    if (drillDownDims.containsKey(dim) == false) {
      drillDownDims.put(dim, drillDownDims.size());
      BooleanQuery.Builder builder = new BooleanQuery.Builder();
      dimQueries.add(builder);
    }
    final int index = drillDownDims.get(dim);
    dimQueries.get(index).add(subQuery, Occur.SHOULD);
  }

  @Override
  public DrillDownQuery clone() {
    return new DrillDownQuery(config, baseQuery, dimQueries, drillDownDims);
  }
  
  @Override
  public int hashCode() {
    return classHash() + Objects.hash(baseQuery, dimQueries);
  }

  @Override
  public boolean equals(Object other) {
    return sameClassAs(other) &&
           equalsTo(getClass().cast(other));
  }

  private boolean equalsTo(DrillDownQuery other) {
    return Objects.equals(baseQuery, other.baseQuery) && 
           dimQueries.equals(other.dimQueries);
  }

  @Override
  public Query rewrite(IndexReader r) throws IOException {
    BooleanQuery rewritten = getBooleanQuery();
    if (rewritten.clauses().isEmpty()) {
      return new MatchAllDocsQuery();
    }
    return rewritten;
  }

  @Override
  public String toString(String field) {
    return getBooleanQuery().toString(field);
  }

  private BooleanQuery getBooleanQuery() {
    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    if (baseQuery != null) {
      bq.add(baseQuery, Occur.MUST);
    }
    for (BooleanQuery.Builder builder : dimQueries) {
      bq.add(builder.build(), Occur.FILTER);
    }
    return bq.build();
  }

  Query getBaseQuery() {
    return baseQuery;
  }

  Query[] getDrillDownQueries() {
    Query[] dimQueries = new Query[this.dimQueries.size()];
    for (int i = 0; i < dimQueries.length; ++i) {
      dimQueries[i] = this.dimQueries.get(i).build();
    }
    return dimQueries;
  }

  Map<String,Integer> getDims() {
    return drillDownDims;
  }
}
