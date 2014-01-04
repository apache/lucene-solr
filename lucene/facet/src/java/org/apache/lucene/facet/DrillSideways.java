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
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.facet.sortedset.SortedSetDocValuesFacetCounts;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesFacetField;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesReaderState;
import org.apache.lucene.facet.taxonomy.FastTaxonomyFacetCounts;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MultiCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.search.Weight;

/**     
 * Computes drill down and sideways counts for the provided
 * {@link DrillDownQuery}.  Drill sideways counts include
 * alternative values/aggregates for the drill-down
 * dimensions so that a dimension does not disappear after
 * the user drills down into it.
 *
 * <p> Use one of the static search
 * methods to do the search, and then get the hits and facet
 * results from the returned {@link DrillSidewaysResult}.
 *
 * <p><b>NOTE</b>: this allocates one {@link
 * FacetsCollector} for each drill-down, plus one.  If your
 * index has high number of facet labels then this will
 * multiply your memory usage.
 *
 * @lucene.experimental
 */
public class DrillSideways {

  /** {@link IndexSearcher} passed to constructor. */
  protected final IndexSearcher searcher;

  /** {@link TaxonomyReader} passed to constructor. */
  protected final TaxonomyReader taxoReader;

  /** {@link SortedSetDocValuesReaderState} passed to
   *  constructor; can be null. */
  protected final SortedSetDocValuesReaderState state;

  /** {@link FacetsConfig} passed to constructor. */
  protected final FacetsConfig config;

  /** Create a new {@code DrillSideways} instance. */
  public DrillSideways(IndexSearcher searcher, FacetsConfig config, TaxonomyReader taxoReader) {
    this(searcher, config, taxoReader, null);
  }
    
  /** Create a new {@code DrillSideways} instance, assuming the categories were
   *  indexed with {@link SortedSetDocValuesFacetField}. */
  public DrillSideways(IndexSearcher searcher, FacetsConfig config, SortedSetDocValuesReaderState state) {
    this(searcher, config, null, state);
  }

  /** Create a new {@code DrillSideways} instance, where some
   *  dimensions were indexed with {@link
   *  SortedSetDocValuesFacetField} and others were indexed
   *  with {@link FacetField}. */
  public DrillSideways(IndexSearcher searcher, FacetsConfig config, TaxonomyReader taxoReader, SortedSetDocValuesReaderState state) {
    this.searcher = searcher;
    this.config = config;
    this.taxoReader = taxoReader;
    this.state = state;
  }

  /** Subclass can override to customize per-dim Facets
   *  impl. */
  protected Facets buildFacetsResult(FacetsCollector drillDowns, FacetsCollector[] drillSideways, String[] drillSidewaysDims) throws IOException {

    Facets drillDownFacets;
    Map<String,Facets> drillSidewaysFacets = new HashMap<String,Facets>();

    if (taxoReader != null) {
      drillDownFacets = new FastTaxonomyFacetCounts(taxoReader, config, drillDowns);
      if (drillSideways != null) {
        for(int i=0;i<drillSideways.length;i++) {
          drillSidewaysFacets.put(drillSidewaysDims[i],
                                  new FastTaxonomyFacetCounts(taxoReader, config, drillSideways[i]));
        }
      }
    } else {
      drillDownFacets = new SortedSetDocValuesFacetCounts(state, drillDowns);
      if (drillSideways != null) {
        for(int i=0;i<drillSideways.length;i++) {
          drillSidewaysFacets.put(drillSidewaysDims[i],
                                  new SortedSetDocValuesFacetCounts(state, drillSideways[i]));
        }
      }
    }

    if (drillSidewaysFacets.isEmpty()) {
      return drillDownFacets;
    } else {
      return new MultiFacets(drillSidewaysFacets, drillDownFacets);
    }
  }

  /**
   * Search, collecting hits with a {@link Collector}, and
   * computing drill down and sideways counts.
   */
  @SuppressWarnings({"rawtypes","unchecked"})
  public DrillSidewaysResult search(DrillDownQuery query, Collector hitCollector) throws IOException {

    Map<String,Integer> drillDownDims = query.getDims();

    FacetsCollector drillDownCollector = new FacetsCollector();
    
    if (drillDownDims.isEmpty()) {
      // There are no drill-down dims, so there is no
      // drill-sideways to compute:
      searcher.search(query, MultiCollector.wrap(hitCollector, drillDownCollector));
      return new DrillSidewaysResult(buildFacetsResult(drillDownCollector, null, null), null);
    }

    BooleanQuery ddq = query.getBooleanQuery();
    BooleanClause[] clauses = ddq.getClauses();

    Query baseQuery;
    int startClause;
    if (clauses.length == drillDownDims.size()) {
      // TODO: we could optimize this pure-browse case by
      // making a custom scorer instead:
      baseQuery = new MatchAllDocsQuery();
      startClause = 0;
    } else {
      assert clauses.length == 1+drillDownDims.size();
      baseQuery = clauses[0].getQuery();
      startClause = 1;
    }

    FacetsCollector[] drillSidewaysCollectors = new FacetsCollector[drillDownDims.size()];
    for (int i = 0; i < drillSidewaysCollectors.length; i++) {
      drillSidewaysCollectors[i] = new FacetsCollector();
    }

    boolean useCollectorMethod = scoreSubDocsAtOnce();

    Term[][] drillDownTerms = null;

    if (!useCollectorMethod) {
      // Optimistic: assume subQueries of the DDQ are either
      // TermQuery or BQ OR of TermQuery; if this is wrong
      // then we detect it and fallback to the mome general
      // but slower DrillSidewaysCollector:
      drillDownTerms = new Term[clauses.length-startClause][];
      for(int i=startClause;i<clauses.length;i++) {
        Query q = clauses[i].getQuery();

        // DrillDownQuery always wraps each subQuery in
        // ConstantScoreQuery:
        assert q instanceof ConstantScoreQuery;

        q = ((ConstantScoreQuery) q).getQuery();

        if (q instanceof TermQuery) {
          drillDownTerms[i-startClause] = new Term[] {((TermQuery) q).getTerm()};
        } else if (q instanceof BooleanQuery) {
          BooleanQuery q2 = (BooleanQuery) q;
          BooleanClause[] clauses2 = q2.getClauses();
          drillDownTerms[i-startClause] = new Term[clauses2.length];
          for(int j=0;j<clauses2.length;j++) {
            if (clauses2[j].getQuery() instanceof TermQuery) {
              drillDownTerms[i-startClause][j] = ((TermQuery) clauses2[j].getQuery()).getTerm();
            } else {
              useCollectorMethod = true;
              break;
            }
          }
        } else {
          useCollectorMethod = true;
        }
      }
    }

    if (useCollectorMethod) {
      // TODO: maybe we could push the "collector method"
      // down into the optimized scorer to have a tighter
      // integration ... and so TermQuery clauses could
      // continue to run "optimized"
      collectorMethod(query, baseQuery, startClause, hitCollector, drillDownCollector, drillSidewaysCollectors);
    } else {
      DrillSidewaysQuery dsq = new DrillSidewaysQuery(baseQuery, drillDownCollector, drillSidewaysCollectors, drillDownTerms);
      searcher.search(dsq, hitCollector);
    }

    return new DrillSidewaysResult(buildFacetsResult(drillDownCollector, drillSidewaysCollectors, drillDownDims.keySet().toArray(new String[drillDownDims.size()])), null);
  }

  /** Uses the more general but slower method of sideways
   *  counting. This method allows an arbitrary subQuery to
   *  implement the drill down for a given dimension. */
  private void collectorMethod(DrillDownQuery ddq, Query baseQuery, int startClause, Collector hitCollector, Collector drillDownCollector, Collector[] drillSidewaysCollectors) throws IOException {

    BooleanClause[] clauses = ddq.getBooleanQuery().getClauses();

    Map<String,Integer> drillDownDims = ddq.getDims();

    BooleanQuery topQuery = new BooleanQuery(true);
    final DrillSidewaysCollector collector = new DrillSidewaysCollector(hitCollector, drillDownCollector, drillSidewaysCollectors,
                                                                                    drillDownDims);

    // TODO: if query is already a BQ we could copy that and
    // add clauses to it, instead of doing BQ inside BQ
    // (should be more efficient)?  Problem is this can
    // affect scoring (coord) ... too bad we can't disable
    // coord on a clause by clause basis:
    topQuery.add(baseQuery, BooleanClause.Occur.MUST);

    // NOTE: in theory we could just make a single BQ, with
    // +query a b c minShouldMatch=2, but in this case,
    // annoyingly, BS2 wraps a sub-scorer that always
    // returns 2 as the .freq(), not how many of the
    // SHOULD clauses matched:
    BooleanQuery subQuery = new BooleanQuery(true);

    Query wrappedSubQuery = new QueryWrapper(subQuery,
                                             new SetWeight() {
                                               @Override
                                               public void set(Weight w) {
                                                 collector.setWeight(w, -1);
                                               }
                                             });
    Query constantScoreSubQuery = new ConstantScoreQuery(wrappedSubQuery);

    // Don't impact score of original query:
    constantScoreSubQuery.setBoost(0.0f);

    topQuery.add(constantScoreSubQuery, BooleanClause.Occur.MUST);

    // Unfortunately this sub-BooleanQuery
    // will never get BS1 because today BS1 only works
    // if topScorer=true... and actually we cannot use BS1
    // anyways because we need subDocsScoredAtOnce:
    int dimIndex = 0;
    for(int i=startClause;i<clauses.length;i++) {
      Query q = clauses[i].getQuery();
      // DrillDownQuery always wraps each subQuery in
      // ConstantScoreQuery:
      assert q instanceof ConstantScoreQuery;
      q = ((ConstantScoreQuery) q).getQuery();

      final int finalDimIndex = dimIndex;
      subQuery.add(new QueryWrapper(q,
                                    new SetWeight() {
                                      @Override
                                      public void set(Weight w) {
                                        collector.setWeight(w, finalDimIndex);
                                      }
                                    }),
                   BooleanClause.Occur.SHOULD);
      dimIndex++;
    }

    // TODO: we could better optimize the "just one drill
    // down" case w/ a separate [specialized]
    // collector...
    int minShouldMatch = drillDownDims.size()-1;
    if (minShouldMatch == 0) {
      // Must add another "fake" clause so BQ doesn't erase
      // itself by rewriting to the single clause:
      Query end = new MatchAllDocsQuery();
      end.setBoost(0.0f);
      subQuery.add(end, BooleanClause.Occur.SHOULD);
      minShouldMatch++;
    }

    subQuery.setMinimumNumberShouldMatch(minShouldMatch);

    // System.out.println("EXE " + topQuery);

    // Collects against the passed-in
    // drillDown/SidewaysCollectors as a side effect:
    searcher.search(topQuery, collector);
  }

  /**
   * Search, sorting by {@link Sort}, and computing
   * drill down and sideways counts.
   */
  public DrillSidewaysResult search(DrillDownQuery query,
                                          Filter filter, FieldDoc after, int topN, Sort sort, boolean doDocScores,
                                          boolean doMaxScore) throws IOException {
    if (filter != null) {
      query = new DrillDownQuery(config, filter, query);
    }
    if (sort != null) {
      int limit = searcher.getIndexReader().maxDoc();
      if (limit == 0) {
        limit = 1; // the collector does not alow numHits = 0
      }
      topN = Math.min(topN, limit);
      final TopFieldCollector hitCollector = TopFieldCollector.create(sort,
                                                                      topN,
                                                                      after,
                                                                      true,
                                                                      doDocScores,
                                                                      doMaxScore,
                                                                      true);
      DrillSidewaysResult r = search(query, hitCollector);
      return new DrillSidewaysResult(r.facets, hitCollector.topDocs());
    } else {
      return search(after, query, topN);
    }
  }

  /**
   * Search, sorting by score, and computing
   * drill down and sideways counts.
   */
  public DrillSidewaysResult search(DrillDownQuery query, int topN) throws IOException {
    return search(null, query, topN);
  }

  /**
   * Search, sorting by score, and computing
   * drill down and sideways counts.
   */
  public DrillSidewaysResult search(ScoreDoc after,
                                          DrillDownQuery query, int topN) throws IOException {
    int limit = searcher.getIndexReader().maxDoc();
    if (limit == 0) {
      limit = 1; // the collector does not alow numHits = 0
    }
    topN = Math.min(topN, limit);
    TopScoreDocCollector hitCollector = TopScoreDocCollector.create(topN, after, true);
    DrillSidewaysResult r = search(query, hitCollector);
    return new DrillSidewaysResult(r.facets, hitCollector.topDocs());
  }

  /** Override this and return true if your collector
   *  (e.g., ToParentBlockJoinCollector) expects all
   *  sub-scorers to be positioned on the document being
   *  collected.  This will cause some performance loss;
   *  default is false.  Note that if you return true from
   *  this method (in a subclass) be sure your collector
   *  also returns false from {@link
   *  Collector#acceptsDocsOutOfOrder}: this will trick
   *  BooleanQuery into also scoring all subDocs at once. */
  protected boolean scoreSubDocsAtOnce() {
    return false;
  }

  /** Result of a drill sideways search, including the
   *  {@link Facets} and {@link TopDocs}. */
  public static class DrillSidewaysResult {
    /** Combined drill down & sideways results. */
    public final Facets facets;

    /** Hits. */
    public final TopDocs hits;

    /** Sole constructor. */
    public DrillSidewaysResult(Facets facets, TopDocs hits) {
      this.facets = facets;
      this.hits = hits;
    }
  }
  private interface SetWeight {
    public void set(Weight w);
  }

  /** Just records which Weight was given out for the
   *  (possibly rewritten) Query. */
  private static class QueryWrapper extends Query {
    private final Query originalQuery;
    private final SetWeight setter;

    public QueryWrapper(Query originalQuery, SetWeight setter) {
      this.originalQuery = originalQuery;
      this.setter = setter;
    }

    @Override
    public Weight createWeight(final IndexSearcher searcher) throws IOException {
      Weight w = originalQuery.createWeight(searcher);
      setter.set(w);
      return w;
    }

    @Override
    public Query rewrite(IndexReader reader) throws IOException {
      Query rewritten = originalQuery.rewrite(reader);
      if (rewritten != originalQuery) {
        return new QueryWrapper(rewritten, setter);
      } else {
        return this;
      }
    }

    @Override
    public String toString(String s) {
      return originalQuery.toString(s);
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof QueryWrapper)) return false;
      final QueryWrapper other = (QueryWrapper) o;
      return super.equals(o) && originalQuery.equals(other.originalQuery);
    }

    @Override
    public int hashCode() {
      return super.hashCode() * 31 + originalQuery.hashCode();
    }
  }
}

