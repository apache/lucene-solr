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
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.facet.params.FacetSearchParams;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
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

  /** Create a new {@code DrillSideways} instance. */
  public DrillSideways(IndexSearcher searcher, TaxonomyReader taxoReader) {
    this.searcher = searcher;
    this.taxoReader = taxoReader;
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
    for(int i=startClause;i<clauses.length;i++) {
      Query q = clauses[i].getQuery();
      String dim = in.getDim(q);
      if (!facetDims.contains(dim)) {
        nonFacetClauses.add(q);
      } else {
        facetClauses.add(q);
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

      return new DrillDownQuery(fsp.indexingParams, newBaseQuery, facetClauses);
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
      // Just do ordinary search:
      FacetsCollector c = FacetsCollector.create(getDrillDownAccumulator(fsp));
      searcher.search(query, MultiCollector.wrap(hitCollector, c));
      return new DrillSidewaysResult(c.getFacetResults(), null);      
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

    Term[][] drillDownTerms = new Term[clauses.length-startClause][];
    for(int i=startClause;i<clauses.length;i++) {
      Query q = clauses[i].getQuery();
      assert q instanceof ConstantScoreQuery;
      q = ((ConstantScoreQuery) q).getQuery();
      assert q instanceof TermQuery || q instanceof BooleanQuery;
      if (q instanceof TermQuery) {
        drillDownTerms[i-startClause] = new Term[] {((TermQuery) q).getTerm()};
      } else {
        BooleanQuery q2 = (BooleanQuery) q;
        BooleanClause[] clauses2 = q2.getClauses();
        drillDownTerms[i-startClause] = new Term[clauses2.length];
        for(int j=0;j<clauses2.length;j++) {
          assert clauses2[j].getQuery() instanceof TermQuery;
          drillDownTerms[i-startClause][j] = ((TermQuery) clauses2[j].getQuery()).getTerm();
        }
      }
    }

    FacetsCollector drillDownCollector = FacetsCollector.create(getDrillDownAccumulator(fsp));

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
      if (requests.isEmpty()) {
        throw new IllegalArgumentException("could not find FacetRequest for drill-sideways dimension \"" + dim + "\"");
      }
      drillSidewaysCollectors[idx++] = FacetsCollector.create(getDrillSidewaysAccumulator(dim, new FacetSearchParams(fsp.indexingParams, requests)));
    }

    DrillSidewaysQuery dsq = new DrillSidewaysQuery(baseQuery, drillDownCollector, drillSidewaysCollectors, drillDownTerms);

    searcher.search(dsq, hitCollector);

    int numDims = drillDownDims.size();
    List<FacetResult>[] drillSidewaysResults = new List[numDims];
    List<FacetResult> drillDownResults = null;

    List<FacetResult> mergedResults = new ArrayList<FacetResult>();
    int[] requestUpto = new int[drillDownDims.size()];
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
        }
        mergedResults.add(drillDownResults.get(i));
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
      final TopFieldCollector hitCollector = TopFieldCollector.create(sort,
                                                                      Math.min(topN, searcher.getIndexReader().maxDoc()),
                                                                      after,
                                                                      true,
                                                                      doDocScores,
                                                                      doMaxScore,
                                                                      true);
      DrillSidewaysResult r = search(query, hitCollector, fsp);
      r.hits = hitCollector.topDocs();
      return r;
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
    TopScoreDocCollector hitCollector = TopScoreDocCollector.create(Math.min(topN, searcher.getIndexReader().maxDoc()), after, true);
    DrillSidewaysResult r = search(query, hitCollector, fsp);
    r.hits = hitCollector.topDocs();
    return r;
  }

  /** Override this to use a custom drill-down {@link
   *  FacetsAccumulator}. */
  protected FacetsAccumulator getDrillDownAccumulator(FacetSearchParams fsp) throws IOException {
    return FacetsAccumulator.create(fsp, searcher.getIndexReader(), taxoReader);
  }

  /** Override this to use a custom drill-sideways {@link
   *  FacetsAccumulator}. */
  protected FacetsAccumulator getDrillSidewaysAccumulator(String dim, FacetSearchParams fsp) throws IOException {
    return FacetsAccumulator.create(fsp, searcher.getIndexReader(), taxoReader);
  }

  /** Represents the returned result from a drill sideways
   *  search. */
  public static class DrillSidewaysResult {
    /** Combined drill down & sideways results. */
    public final List<FacetResult> facetResults;

    /** Hits. */
    public TopDocs hits;

    DrillSidewaysResult(List<FacetResult> facetResults, TopDocs hits) {
      this.facetResults = facetResults;
      this.hits = hits;
    }
  }
}

