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
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.facet.sortedset.SortedSetDocValuesFacetCounts;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesFacetField;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesReaderState;
import org.apache.lucene.facet.taxonomy.FastTaxonomyFacetCounts;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.search.FilterCollector;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MultiCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
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
    Map<String,Facets> drillSidewaysFacets = new HashMap<>();

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
  public DrillSidewaysResult search(DrillDownQuery query, Collector hitCollector) throws IOException {

    Map<String,Integer> drillDownDims = query.getDims();

    FacetsCollector drillDownCollector = new FacetsCollector();
    
    if (drillDownDims.isEmpty()) {
      // There are no drill-down dims, so there is no
      // drill-sideways to compute:
      searcher.search(query, MultiCollector.wrap(hitCollector, drillDownCollector));
      return new DrillSidewaysResult(buildFacetsResult(drillDownCollector, null, null), null);
    }

    Query baseQuery = query.getBaseQuery();
    if (baseQuery == null) {
      // TODO: we could optimize this pure-browse case by
      // making a custom scorer instead:
      baseQuery = new MatchAllDocsQuery();
    }
    Query[] drillDownQueries = query.getDrillDownQueries();

    FacetsCollector[] drillSidewaysCollectors = new FacetsCollector[drillDownDims.size()];
    for (int i = 0; i < drillSidewaysCollectors.length; i++) {
      drillSidewaysCollectors[i] = new FacetsCollector();
    }
    
    DrillSidewaysQuery dsq = new DrillSidewaysQuery(baseQuery, drillDownCollector, drillSidewaysCollectors, drillDownQueries, scoreSubDocsAtOnce());
    if (hitCollector.needsScores() == false) {
      // this is a borrible hack in order to make sure IndexSearcher will not
      // attempt to cache the DrillSidewaysQuery
      hitCollector = new FilterCollector(hitCollector) {
        @Override
        public boolean needsScores() {
          return true;
        }
      };
    }
    searcher.search(dsq, hitCollector);

    return new DrillSidewaysResult(buildFacetsResult(drillDownCollector, drillSidewaysCollectors, drillDownDims.keySet().toArray(new String[drillDownDims.size()])), null);
  }

  /**
   * Search, sorting by {@link Sort}, and computing
   * drill down and sideways counts.
   */
  public DrillSidewaysResult search(DrillDownQuery query,
                                    Query filter, FieldDoc after, int topN, Sort sort, boolean doDocScores,
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
                                                                      doMaxScore);
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
    TopScoreDocCollector hitCollector = TopScoreDocCollector.create(topN, after);
    DrillSidewaysResult r = search(query, hitCollector);
    return new DrillSidewaysResult(r.facets, hitCollector.topDocs());
  }

  /** Override this and return true if your collector
   *  (e.g., {@code ToParentBlockJoinCollector}) expects all
   *  sub-scorers to be positioned on the document being
   *  collected.  This will cause some performance loss;
   *  default is false. */
  protected boolean scoreSubDocsAtOnce() {
    return false;
  }

  /** Result of a drill sideways search, including the
   *  {@link Facets} and {@link TopDocs}. */
  public static class DrillSidewaysResult {
    /** Combined drill down and sideways results. */
    public final Facets facets;

    /** Hits. */
    public final TopDocs hits;

    /** Sole constructor. */
    public DrillSidewaysResult(Facets facets, TopDocs hits) {
      this.facets = facets;
      this.hits = hits;
    }
  }
}

