package org.apache.lucene.facet.search;

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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.lucene.facet.params.CategoryListParams;
import org.apache.lucene.facet.params.FacetIndexingParams;
import org.apache.lucene.facet.taxonomy.CategoryPath;
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
 * A {@link Query} for drill-down over {@link CategoryPath categories}. You
 * should call {@link #add(CategoryPath...)} for every group of categories you
 * want to drill-down over. Each category in the group is {@code OR'ed} with
 * the others, and groups are {@code AND'ed}.
 * <p>
 * <b>NOTE:</b> if you choose to create your own {@link Query} by calling
 * {@link #term}, it is recommended to wrap it with {@link ConstantScoreQuery}
 * and set the {@link ConstantScoreQuery#setBoost(float) boost} to {@code 0.0f},
 * so that it does not affect the scores of the documents.
 * 
 * @lucene.experimental
 */
public final class DrillDownQuery extends Query {

  /** Return a drill-down {@link Term} for a category. */
  public static Term term(FacetIndexingParams iParams, CategoryPath path) {
    CategoryListParams clp = iParams.getCategoryListParams(path);
    char[] buffer = new char[path.fullPathLength()];
    iParams.drillDownTermText(path, buffer);
    return new Term(clp.field, String.valueOf(buffer));
  }
  
  private final BooleanQuery query;
  private final Map<String,Integer> drillDownDims = new LinkedHashMap<String,Integer>();
  final FacetIndexingParams fip;

  /** Used by clone() */
  DrillDownQuery(FacetIndexingParams fip, BooleanQuery query, Map<String,Integer> drillDownDims) {
    this.fip = fip;
    this.query = query.clone();
    this.drillDownDims.putAll(drillDownDims);
  }

  /** Used by DrillSideways */
  DrillDownQuery(Filter filter, DrillDownQuery other) {
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
    fip = other.fip;
  }

  /** Used by DrillSideways */
  DrillDownQuery(FacetIndexingParams fip, Query baseQuery, List<Query> clauses) {
    this.fip = fip;
    this.query = new BooleanQuery(true);
    if (baseQuery != null) {
      query.add(baseQuery, Occur.MUST);      
    }
    for(Query clause : clauses) {
      query.add(clause, Occur.MUST);
      drillDownDims.put(getDim(clause), drillDownDims.size());
    }
  }

  String getDim(Query clause) {
    assert clause instanceof ConstantScoreQuery;
    clause = ((ConstantScoreQuery) clause).getQuery();
    assert clause instanceof TermQuery || clause instanceof BooleanQuery;
    String term;
    if (clause instanceof TermQuery) {
      term = ((TermQuery) clause).getTerm().text();
    } else {
      term = ((TermQuery) ((BooleanQuery) clause).getClauses()[0].getQuery()).getTerm().text();
    }
    return term.split(Pattern.quote(Character.toString(fip.getFacetDelimChar())), 2)[0];
  }

  /**
   * Creates a new {@link DrillDownQuery} without a base query, 
   * to perform a pure browsing query (equivalent to using
   * {@link MatchAllDocsQuery} as base).
   */
  public DrillDownQuery(FacetIndexingParams fip) {
    this(fip, null);
  }
  
  /**
   * Creates a new {@link DrillDownQuery} over the given base query. Can be
   * {@code null}, in which case the result {@link Query} from
   * {@link #rewrite(IndexReader)} will be a pure browsing query, filtering on
   * the added categories only.
   */
  public DrillDownQuery(FacetIndexingParams fip, Query baseQuery) {
    query = new BooleanQuery(true); // disable coord
    if (baseQuery != null) {
      query.add(baseQuery, Occur.MUST);
    }
    this.fip = fip;
  }

  /**
   * Adds one dimension of drill downs; if you pass multiple values they are
   * OR'd, and then the entire dimension is AND'd against the base query.
   */
  public void add(CategoryPath... paths) {
    Query q;
    if (paths[0].length == 0) {
      throw new IllegalArgumentException("all CategoryPaths must have length > 0");
    }
    String dim = paths[0].components[0];
    if (drillDownDims.containsKey(dim)) {
      throw new IllegalArgumentException("dimension '" + dim + "' was already added");
    }
    if (paths.length == 1) {
      q = new TermQuery(term(fip, paths[0]));
    } else {
      BooleanQuery bq = new BooleanQuery(true); // disable coord
      for (CategoryPath cp : paths) {
        if (cp.length == 0) {
          throw new IllegalArgumentException("all CategoryPaths must have length > 0");
        }
        if (!cp.components[0].equals(dim)) {
          throw new IllegalArgumentException("multiple (OR'd) drill-down paths must be under same dimension; got '" 
              + dim + "' and '" + cp.components[0] + "'");
        }
        bq.add(new TermQuery(term(fip, cp)), Occur.SHOULD);
      }
      q = bq;
    }
    drillDownDims.put(dim, drillDownDims.size());

    final ConstantScoreQuery drillDownQuery = new ConstantScoreQuery(q);
    drillDownQuery.setBoost(0.0f);
    query.add(drillDownQuery, Occur.MUST);
  }

  @Override
  public DrillDownQuery clone() {
    return new DrillDownQuery(fip, query, drillDownDims);
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
      // baseQuery given to the ctor was null + no drill-downs were added
      // note that if only baseQuery was given to the ctor, but no drill-down terms
      // is fine, since the rewritten query will be the original base query.
      throw new IllegalStateException("no base query or drill-down categories given");
    }
    return query;
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
