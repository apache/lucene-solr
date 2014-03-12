package org.apache.lucene.facet;

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.FilteredQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;

/**
 * A {@link Query} for drill-down over facet categories. You
 * should call {@link #add(String, String...)} for every group of categories you
 * want to drill-down over.
 * <p>
 * <b>NOTE:</b> if you choose to create your own {@link Query} by calling
 * {@link #term}, it is recommended to wrap it with {@link ConstantScoreQuery}
 * and set the {@link ConstantScoreQuery#setBoost(float) boost} to {@code 0.0f},
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
  private final BooleanQuery query;
  private final Map<String,Integer> drillDownDims = new LinkedHashMap<>();

  /** Used by clone() */
  DrillDownQuery(FacetsConfig config, BooleanQuery query, Map<String,Integer> drillDownDims) {
    this.query = query.clone();
    this.drillDownDims.putAll(drillDownDims);
    this.config = config;
  }

  /** Used by DrillSideways */
  DrillDownQuery(FacetsConfig config, Filter filter, DrillDownQuery other) {
    query = new BooleanQuery(true); // disable coord

    BooleanClause[] clauses = other.query.getClauses();
    if (clauses.length == other.drillDownDims.size()) {
      throw new IllegalArgumentException("cannot apply filter unless baseQuery isn't null; pass ConstantScoreQuery instead");
    }
    assert clauses.length == 1+other.drillDownDims.size(): clauses.length + " vs " + (1+other.drillDownDims.size());
    drillDownDims.putAll(other.drillDownDims);
    query.add(new FilteredQuery(clauses[0].getQuery(), filter), Occur.MUST);
    for(int i=1;i<clauses.length;i++) {
      query.add(clauses[i].getQuery(), Occur.MUST);
    }
    this.config = config;
  }

  /** Used by DrillSideways */
  DrillDownQuery(FacetsConfig config, Query baseQuery, List<Query> clauses, Map<String,Integer> drillDownDims) {
    query = new BooleanQuery(true);
    if (baseQuery != null) {
      query.add(baseQuery, Occur.MUST);      
    }
    for(Query clause : clauses) {
      query.add(clause, Occur.MUST);
    }
    this.drillDownDims.putAll(drillDownDims);
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
    query = new BooleanQuery(true); // disable coord
    if (baseQuery != null) {
      query.add(baseQuery, Occur.MUST);
    }
    this.config = config;
  }

  /** Merges (ORs) a new path into an existing AND'd
   *  clause. */ 
  private void merge(String dim, String[] path) {
    int index = drillDownDims.get(dim);
    if (query.getClauses().length == drillDownDims.size()+1) {
      index++;
    }
    ConstantScoreQuery q = (ConstantScoreQuery) query.clauses().get(index).getQuery();
    if ((q.getQuery() instanceof BooleanQuery) == false) {
      // App called .add(dim, customQuery) and then tried to
      // merge a facet label in:
      throw new RuntimeException("cannot merge with custom Query");
    }
    String indexedField = config.getDimConfig(dim).indexFieldName;

    BooleanQuery bq = (BooleanQuery) q.getQuery();
    bq.add(new TermQuery(term(indexedField, dim, path)), Occur.SHOULD);
  }

  /** Adds one dimension of drill downs; if you pass the same
   *  dimension more than once it is OR'd with the previous
   *  cofnstraints on that dimension, and all dimensions are
   *  AND'd against each other and the base query. */
  public void add(String dim, String... path) {

    if (drillDownDims.containsKey(dim)) {
      merge(dim, path);
      return;
    }
    String indexedField = config.getDimConfig(dim).indexFieldName;

    BooleanQuery bq = new BooleanQuery(true); // disable coord
    bq.add(new TermQuery(term(indexedField, dim, path)), Occur.SHOULD);

    add(dim, bq);
  }

  /** Expert: add a custom drill-down subQuery.  Use this
   *  when you have a separate way to drill-down on the
   *  dimension than the indexed facet ordinals. */
  public void add(String dim, Query subQuery) {

    if (drillDownDims.containsKey(dim)) {
      throw new IllegalArgumentException("dimension \"" + dim + "\" already has a drill-down");
    }
    // TODO: we should use FilteredQuery?

    // So scores of the drill-down query don't have an
    // effect:
    final ConstantScoreQuery drillDownQuery = new ConstantScoreQuery(subQuery);
    drillDownQuery.setBoost(0.0f);

    query.add(drillDownQuery, Occur.MUST);

    drillDownDims.put(dim, drillDownDims.size());
  }

  /** Expert: add a custom drill-down Filter, e.g. when
   *  drilling down after range faceting. */
  public void add(String dim, Filter subFilter) {

    if (drillDownDims.containsKey(dim)) {
      throw new IllegalArgumentException("dimension \"" + dim + "\" already has a drill-down");
    }

    // TODO: we should use FilteredQuery?

    // So scores of the drill-down query don't have an
    // effect:
    final ConstantScoreQuery drillDownQuery = new ConstantScoreQuery(subFilter);
    drillDownQuery.setBoost(0.0f);

    query.add(drillDownQuery, Occur.MUST);

    drillDownDims.put(dim, drillDownDims.size());
  }

  static Filter getFilter(Query query) {
    if (query instanceof ConstantScoreQuery) {
      ConstantScoreQuery csq = (ConstantScoreQuery) query;
      Filter filter = csq.getFilter();
      if (filter != null) {
        return filter;
      } else {
        return getFilter(csq.getQuery());
      }
    } else {
      return null;
    }
  }

  @Override
  public DrillDownQuery clone() {
    return new DrillDownQuery(config, query, drillDownDims);
  }
  
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    return prime * result + query.hashCode();
  }
  
  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof DrillDownQuery)) {
      return false;
    }
    
    DrillDownQuery other = (DrillDownQuery) obj;
    return query.equals(other.query) && super.equals(other);
  }
  
  @Override
  public Query rewrite(IndexReader r) throws IOException {
    if (query.clauses().size() == 0) {
      return new MatchAllDocsQuery();
    }

    List<Filter> filters = new ArrayList<>();
    List<Query> queries = new ArrayList<>();
    List<BooleanClause> clauses = query.clauses();
    Query baseQuery;
    int startIndex;
    if (drillDownDims.size() == query.clauses().size()) {
      baseQuery = new MatchAllDocsQuery();
      startIndex = 0;
    } else {
      baseQuery = clauses.get(0).getQuery();
      startIndex = 1;
    }

    for(int i=startIndex;i<clauses.size();i++) {
      BooleanClause clause = clauses.get(i);
      Query queryClause = clause.getQuery();
      Filter filter = getFilter(queryClause);
      if (filter != null) {
        filters.add(filter);
      } else {
        queries.add(queryClause);
      }
    }

    if (filters.isEmpty()) {
      return query;
    } else {
      // Wrap all filters using FilteredQuery
      
      // TODO: this is hackish; we need to do it because
      // BooleanQuery can't be trusted to handle the
      // "expensive filter" case.  Really, each Filter should
      // know its cost and we should take that more
      // carefully into account when picking the right
      // strategy/optimization:
      Query wrapped;
      if (queries.isEmpty()) {
        wrapped = baseQuery;
      } else {
        // disable coord
        BooleanQuery wrappedBQ = new BooleanQuery(true);
        if ((baseQuery instanceof MatchAllDocsQuery) == false) {
          wrappedBQ.add(baseQuery, BooleanClause.Occur.MUST);
        }
        for(Query q : queries) {
          wrappedBQ.add(q, BooleanClause.Occur.MUST);
        }
        wrapped = wrappedBQ;
      }

      for(Filter filter : filters) {
        wrapped = new FilteredQuery(wrapped, filter, FilteredQuery.QUERY_FIRST_FILTER_STRATEGY);
      }

      return wrapped;
    }
  }

  @Override
  public String toString(String field) {
    return query.toString(field);
  }

  BooleanQuery getBooleanQuery() {
    return query;
  }

  Map<String,Integer> getDims() {
    return drillDownDims;
  }
}
