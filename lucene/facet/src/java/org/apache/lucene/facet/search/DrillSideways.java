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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.facet.index.FacetFields;
import org.apache.lucene.facet.params.FacetSearchParams;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesFacetFields;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesReaderState;
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

  protected final IndexSearcher searcher;
  protected final TaxonomyReader taxoReader;
  protected final SortedSetDocValuesReaderState state;
  
  /**
   * Create a new {@code DrillSideways} instance, assuming the categories were
   * indexed with {@link FacetFields}.
   */
  public DrillSideways(IndexSearcher searcher, TaxonomyReader taxoReader) {
    this.searcher = searcher;
    this.taxoReader = taxoReader;
    this.state = null;
  }
  
  /**
   * Create a new {@code DrillSideways} instance, assuming the categories were
   * indexed with {@link SortedSetDocValuesFacetFields}.
   */
  public DrillSideways(IndexSearcher searcher, SortedSetDocValuesReaderState state) {
    this.searcher = searcher;
    this.taxoReader = null;
    this.state = state;
  }

  /** Moves any drill-downs that don't have a corresponding
   *  facet request into the baseQuery.  This is unusual,
   *  yet allowed, because typically the added drill-downs are because
   *  the user has clicked on previously presented facets,
   *  and those same facets would be computed this time
   *  around. */
  private static DrillDownQuery moveDrillDownOnlyClauses(DrillDownQuery in, FacetSearchParams fsp) {
    Set<String> facetDims = new HashSet<String>();
    for(FacetRequest fr : fsp.facetRequests) {
      if (fr.categoryPath.length == 0) {
        throw new IllegalArgumentException("all FacetRequests must have CategoryPath with length > 0");
      }
      facetDims.add(fr.categoryPath.components[0]);
    }

    BooleanClause[] clauses = in.getBooleanQuery().getClauses();
    Map<String,Integer> drillDownDims = in.getDims();

    String[] dimsByIndex = new String[drillDownDims.size()];
    for(Map.Entry<String,Integer> ent : drillDownDims.entrySet()) {
      dimsByIndex[ent.getValue()] = ent.getKey();
    }

    int startClause;
    if (clauses.length == drillDownDims.size()) {
      startClause = 0;
    } else {
      assert clauses.length == 1+drillDownDims.size();
      startClause = 1;
    }

    // Break out drill-down clauses that have no
    // corresponding facet request and move them inside the
    // baseQuery:
    List<Query> nonFacetClauses = new ArrayList<Query>();
    List<Query> facetClauses = new ArrayList<Query>();
    Map<String,Integer> dimToIndex = new LinkedHashMap<String,Integer>();
    for(int i=startClause;i<clauses.length;i++) {
      Query q = clauses[i].getQuery();
      String dim = dimsByIndex[i-startClause];
      if (!facetDims.contains(dim)) {
        nonFacetClauses.add(q);
      } else {
        facetClauses.add(q);
        dimToIndex.put(dim, dimToIndex.size());
      }
    }

    if (!nonFacetClauses.isEmpty()) {
      BooleanQuery newBaseQuery = new BooleanQuery(true);
      if (startClause == 1) {
        // Add original basaeQuery:
        newBaseQuery.add(clauses[0].getQuery(), BooleanClause.Occur.MUST);
      }
      for(Query q : nonFacetClauses) {
        newBaseQuery.add(q, BooleanClause.Occur.MUST);
      }

      return new DrillDownQuery(fsp.indexingParams, newBaseQuery, facetClauses, dimToIndex);
    } else {
      // No change:
      return in;
    }
  }

  /**
   * Search, collecting hits with a {@link Collector}, and
   * computing drill down and sideways counts.
   */
  @SuppressWarnings({"rawtypes","unchecked"})
  public DrillSidewaysResult search(DrillDownQuery query,
                                    Collector hitCollector, FacetSearchParams fsp) throws IOException {

    if (query.fip != fsp.indexingParams) {
      throw new IllegalArgumentException("DrillDownQuery's FacetIndexingParams should match FacetSearchParams'");
    }

    query = moveDrillDownOnlyClauses(query, fsp);

    Map<String,Integer> drillDownDims = query.getDims();

    if (drillDownDims.isEmpty()) {
      // Just do ordinary search when there are no drill-downs:
      FacetsCollector c = FacetsCollector.create(getDrillDownAccumulator(fsp));
      searcher.search(query, MultiCollector.wrap(hitCollector, c));
      return new DrillSidewaysResult(c.getFacetResults(), null);      
    }

    List<FacetRequest> ddRequests = new ArrayList<FacetRequest>();
    for(FacetRequest fr : fsp.facetRequests) {
      assert fr.categoryPath.length > 0;
      if (!drillDownDims.containsKey(fr.categoryPath.components[0])) {
        ddRequests.add(fr);
      }
    }
    FacetSearchParams fsp2;
    if (!ddRequests.isEmpty()) {
      fsp2 = new FacetSearchParams(fsp.indexingParams, ddRequests);
    } else {
      fsp2 = null;
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

    FacetsCollector drillDownCollector = fsp2 == null ? null : FacetsCollector.create(getDrillDownAccumulator(fsp2));

    FacetsCollector[] drillSidewaysCollectors = new FacetsCollector[drillDownDims.size()];

    int idx = 0;
    for(String dim : drillDownDims.keySet()) {
      List<FacetRequest> requests = new ArrayList<FacetRequest>();
      for(FacetRequest fr : fsp.facetRequests) {
        assert fr.categoryPath.length > 0;
        if (fr.categoryPath.components[0].equals(dim)) {
          requests.add(fr);
        }
      }
      // We already moved all drill-downs that didn't have a
      // FacetRequest, in moveDrillDownOnlyClauses above:
      assert !requests.isEmpty();
      drillSidewaysCollectors[idx++] = FacetsCollector.create(getDrillSidewaysAccumulator(dim, new FacetSearchParams(fsp.indexingParams, requests)));
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

    int numDims = drillDownDims.size();
    List<FacetResult>[] drillSidewaysResults = new List[numDims];
    List<FacetResult> drillDownResults = null;

    List<FacetResult> mergedResults = new ArrayList<FacetResult>();
    int[] requestUpto = new int[drillDownDims.size()];
    int ddUpto = 0;
    for(int i=0;i<fsp.facetRequests.size();i++) {
      FacetRequest fr = fsp.facetRequests.get(i);
      assert fr.categoryPath.length > 0;
      Integer dimIndex = drillDownDims.get(fr.categoryPath.components[0]);
      if (dimIndex == null) {
        // Pure drill down dim (the current query didn't
        // drill down on this dim):
        if (drillDownResults == null) {
          // Lazy init, in case all requests were against
          // drill-sideways dims:
          drillDownResults = drillDownCollector.getFacetResults();
          //System.out.println("get DD results");
        }
        //System.out.println("add dd results " + i);
        mergedResults.add(drillDownResults.get(ddUpto++));
      } else {
        // Drill sideways dim:
        int dim = dimIndex.intValue();
        List<FacetResult> sidewaysResult = drillSidewaysResults[dim];
        if (sidewaysResult == null) {
          // Lazy init, in case no facet request is against
          // a given drill down dim:
          sidewaysResult = drillSidewaysCollectors[dim].getFacetResults();
          drillSidewaysResults[dim] = sidewaysResult;
        }
        mergedResults.add(sidewaysResult.get(requestUpto[dim]));
        requestUpto[dim]++;
      }
    }

    return new DrillSidewaysResult(mergedResults, null);
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
                                    boolean doMaxScore, FacetSearchParams fsp) throws IOException {
    if (filter != null) {
      query = new DrillDownQuery(filter, query);
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
      DrillSidewaysResult r = search(query, hitCollector, fsp);
      return new DrillSidewaysResult(r.facetResults, hitCollector.topDocs());
    } else {
      return search(after, query, topN, fsp);
    }
  }

  /**
   * Search, sorting by score, and computing
   * drill down and sideways counts.
   */
  public DrillSidewaysResult search(ScoreDoc after,
                                    DrillDownQuery query, int topN, FacetSearchParams fsp) throws IOException {
    int limit = searcher.getIndexReader().maxDoc();
    if (limit == 0) {
      limit = 1; // the collector does not alow numHits = 0
    }
    topN = Math.min(topN, limit);
    TopScoreDocCollector hitCollector = TopScoreDocCollector.create(topN, after, true);
    DrillSidewaysResult r = search(query, hitCollector, fsp);
    return new DrillSidewaysResult(r.facetResults, hitCollector.topDocs());
  }

  /** Override this to use a custom drill-down {@link
   *  FacetsAccumulator}. */
  protected FacetsAccumulator getDrillDownAccumulator(FacetSearchParams fsp) throws IOException {
    if (taxoReader != null) {
      return FacetsAccumulator.create(fsp, searcher.getIndexReader(), taxoReader, null);
    } else {
      return FacetsAccumulator.create(fsp, state, null);
    }
  }

  /** Override this to use a custom drill-sideways {@link
   *  FacetsAccumulator}. */
  protected FacetsAccumulator getDrillSidewaysAccumulator(String dim, FacetSearchParams fsp) throws IOException {
    if (taxoReader != null) {
      return FacetsAccumulator.create(fsp, searcher.getIndexReader(), taxoReader, null);
    } else {
      return FacetsAccumulator.create(fsp, state, null);
    }
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

  /**
   * Represents the returned result from a drill sideways search. Note that if
   * you called
   * {@link DrillSideways#search(DrillDownQuery, Collector, FacetSearchParams)},
   * then {@link #hits} will be {@code null}.
   */
  public static class DrillSidewaysResult {
    /** Combined drill down & sideways results. */
    public final List<FacetResult> facetResults;

    /** Hits. */
    public final TopDocs hits;

    public DrillSidewaysResult(List<FacetResult> facetResults, TopDocs hits) {
      this.facetResults = facetResults;
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

