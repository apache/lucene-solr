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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.lucene.facet.sortedset.SortedSetDocValuesFacetCounts;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesFacetField;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesReaderState;
import org.apache.lucene.facet.taxonomy.FastTaxonomyFacetCounts;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.FilterCollector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MultiCollector;
import org.apache.lucene.search.MultiCollectorManager;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.util.ThreadInterruptedException;

/**
 * Computes drill down and sideways counts for the provided
 * {@link DrillDownQuery}.  Drill sideways counts include
 * alternative values/aggregates for the drill-down
 * dimensions so that a dimension does not disappear after
 * the user drills down into it.
 * <p> Use one of the static search
 * methods to do the search, and then get the hits and facet
 * results from the returned {@link DrillSidewaysResult}.
 * <p><b>NOTE</b>: this allocates one {@link
 * FacetsCollector} for each drill-down, plus one.  If your
 * index has high number of facet labels then this will
 * multiply your memory usage.
 *
 * @lucene.experimental
 */
public class DrillSideways {

  /**
   * {@link IndexSearcher} passed to constructor.
   */
  protected final IndexSearcher searcher;

  /**
   * {@link TaxonomyReader} passed to constructor.
   */
  protected final TaxonomyReader taxoReader;

  /**
   * {@link SortedSetDocValuesReaderState} passed to
   * constructor; can be null.
   */
  protected final SortedSetDocValuesReaderState state;

  /**
   * {@link FacetsConfig} passed to constructor.
   */
  protected final FacetsConfig config;

  // These are only used for multi-threaded search
  private final ExecutorService executor;

  /**
   * Create a new {@code DrillSideways} instance.
   */
  public DrillSideways(IndexSearcher searcher, FacetsConfig config, TaxonomyReader taxoReader) {
    this(searcher, config, taxoReader, null);
  }

  /**
   * Create a new {@code DrillSideways} instance, assuming the categories were
   * indexed with {@link SortedSetDocValuesFacetField}.
   */
  public DrillSideways(IndexSearcher searcher, FacetsConfig config, SortedSetDocValuesReaderState state) {
    this(searcher, config, null, state);
  }

  /**
   * Create a new {@code DrillSideways} instance, where some
   * dimensions were indexed with {@link
   * SortedSetDocValuesFacetField} and others were indexed
   * with {@link FacetField}.
   */
  public DrillSideways(IndexSearcher searcher, FacetsConfig config, TaxonomyReader taxoReader,
          SortedSetDocValuesReaderState state) {
    this(searcher, config, taxoReader, state, null);
  }

  /**
   * Create a new {@code DrillSideways} instance, where some
   * dimensions were indexed with {@link
   * SortedSetDocValuesFacetField} and others were indexed
   * with {@link FacetField}.
   * <p>
   * Use this constructor to use the concurrent implementation and/or the CollectorManager
   */
  public DrillSideways(IndexSearcher searcher, FacetsConfig config, TaxonomyReader taxoReader,
          SortedSetDocValuesReaderState state, ExecutorService executor) {
    this.searcher = searcher;
    this.config = config;
    this.taxoReader = taxoReader;
    this.state = state;
    this.executor = executor;
  }

  /**
   * Subclass can override to customize per-dim Facets
   * impl.
   */
  protected Facets buildFacetsResult(FacetsCollector drillDowns, FacetsCollector[] drillSideways,
          String[] drillSidewaysDims) throws IOException {

    Facets drillDownFacets;
    Map<String, Facets> drillSidewaysFacets = new HashMap<>();

    if (taxoReader != null) {
      drillDownFacets = new FastTaxonomyFacetCounts(taxoReader, config, drillDowns);
      if (drillSideways != null) {
        for (int i = 0; i < drillSideways.length; i++) {
          drillSidewaysFacets.put(drillSidewaysDims[i],
                  new FastTaxonomyFacetCounts(taxoReader, config, drillSideways[i]));
        }
      }
    } else {
      drillDownFacets = new SortedSetDocValuesFacetCounts(state, drillDowns);
      if (drillSideways != null) {
        for (int i = 0; i < drillSideways.length; i++) {
          drillSidewaysFacets.put(drillSidewaysDims[i], new SortedSetDocValuesFacetCounts(state, drillSideways[i]));
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

    Map<String, Integer> drillDownDims = query.getDims();

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

    DrillSidewaysQuery dsq =
            new DrillSidewaysQuery(baseQuery, drillDownCollector, drillSidewaysCollectors, drillDownQueries,
                    scoreSubDocsAtOnce());
    if (hitCollector.scoreMode().needsScores() == false) {
      // this is a horrible hack in order to make sure IndexSearcher will not
      // attempt to cache the DrillSidewaysQuery
      hitCollector = new FilterCollector(hitCollector) {
        @Override
        public ScoreMode scoreMode() {
          return ScoreMode.COMPLETE;
        }
      };
    }
    searcher.search(dsq, hitCollector);

    return new DrillSidewaysResult(buildFacetsResult(drillDownCollector, drillSidewaysCollectors,
            drillDownDims.keySet().toArray(new String[drillDownDims.size()])), null);
  }

  /**
   * Search, sorting by {@link Sort}, and computing
   * drill down and sideways counts.
   */
  public DrillSidewaysResult search(DrillDownQuery query, Query filter, FieldDoc after, int topN, Sort sort,
          boolean doDocScores) throws IOException {
    if (filter != null) {
      query = new DrillDownQuery(config, filter, query);
    }
    if (sort != null) {
      int limit = searcher.getIndexReader().maxDoc();
      if (limit == 0) {
        limit = 1; // the collector does not alow numHits = 0
      }
      final int fTopN = Math.min(topN, limit);

      if (executor != null) { // We have an executor, let use the multi-threaded version

        final CollectorManager<TopFieldCollector, TopFieldDocs> collectorManager =
                new CollectorManager<TopFieldCollector, TopFieldDocs>() {

                  @Override
                  public TopFieldCollector newCollector() throws IOException {
                    return TopFieldCollector.create(sort, fTopN, after, Integer.MAX_VALUE);
                  }

                  @Override
                  public TopFieldDocs reduce(Collection<TopFieldCollector> collectors) throws IOException {
                    final TopFieldDocs[] topFieldDocs = new TopFieldDocs[collectors.size()];
                    int pos = 0;
                    for (TopFieldCollector collector : collectors)
                      topFieldDocs[pos++] = collector.topDocs();
                    return TopDocs.merge(sort, topN, topFieldDocs);
                  }

                };
        ConcurrentDrillSidewaysResult<TopFieldDocs> r = search(query, collectorManager);
        TopFieldDocs topDocs = r.collectorResult;
        if (doDocScores) {
          TopFieldCollector.populateScores(topDocs.scoreDocs, searcher, query);
        }
        return new DrillSidewaysResult(r.facets, topDocs);

      } else {

        final TopFieldCollector hitCollector =
                TopFieldCollector.create(sort, fTopN, after, Integer.MAX_VALUE);
        DrillSidewaysResult r = search(query, hitCollector);
        TopFieldDocs topDocs = hitCollector.topDocs();
        if (doDocScores) {
          TopFieldCollector.populateScores(topDocs.scoreDocs, searcher, query);
        }
        return new DrillSidewaysResult(r.facets, topDocs);
      }
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
  public DrillSidewaysResult search(ScoreDoc after, DrillDownQuery query, int topN) throws IOException {
    int limit = searcher.getIndexReader().maxDoc();
    if (limit == 0) {
      limit = 1; // the collector does not alow numHits = 0
    }
    final int fTopN = Math.min(topN, limit);

    if (executor != null) {  // We have an executor, let use the multi-threaded version

      final CollectorManager<TopScoreDocCollector, TopDocs> collectorManager =
              new CollectorManager<TopScoreDocCollector, TopDocs>() {

                @Override
                public TopScoreDocCollector newCollector() throws IOException {
                  return TopScoreDocCollector.create(fTopN, after, Integer.MAX_VALUE);
                }

                @Override
                public TopDocs reduce(Collection<TopScoreDocCollector> collectors) throws IOException {
                  final TopDocs[] topDocs = new TopDocs[collectors.size()];
                  int pos = 0;
                  for (TopScoreDocCollector collector : collectors)
                    topDocs[pos++] = collector.topDocs();
                  return TopDocs.merge(topN, topDocs);
                }

              };
      ConcurrentDrillSidewaysResult<TopDocs> r = search(query, collectorManager);
      return new DrillSidewaysResult(r.facets, r.collectorResult);

    } else {

      TopScoreDocCollector hitCollector = TopScoreDocCollector.create(topN, after, Integer.MAX_VALUE);
      DrillSidewaysResult r = search(query, hitCollector);
      return new DrillSidewaysResult(r.facets, hitCollector.topDocs());
    }
  }

  /**
   * Override this and return true if your collector
   * (e.g., {@code ToParentBlockJoinCollector}) expects all
   * sub-scorers to be positioned on the document being
   * collected.  This will cause some performance loss;
   * default is false.
   */
  protected boolean scoreSubDocsAtOnce() {
    return false;
  }

  /**
   * Result of a drill sideways search, including the
   * {@link Facets} and {@link TopDocs}.
   */
  public static class DrillSidewaysResult {
    /**
     * Combined drill down and sideways results.
     */
    public final Facets facets;

    /**
     * Hits.
     */
    public final TopDocs hits;

    /**
     * Sole constructor.
     */
    public DrillSidewaysResult(Facets facets, TopDocs hits) {
      this.facets = facets;
      this.hits = hits;
    }
  }

  private static class CallableCollector implements Callable<CallableResult> {

    private final int pos;
    private final IndexSearcher searcher;
    private final Query query;
    private final CollectorManager<?, ?> collectorManager;

    private CallableCollector(int pos, IndexSearcher searcher, Query query, CollectorManager<?, ?> collectorManager) {
      this.pos = pos;
      this.searcher = searcher;
      this.query = query;
      this.collectorManager = collectorManager;
    }

    @Override
    public CallableResult call() throws Exception {
      return new CallableResult(pos, searcher.search(query, collectorManager));
    }
  }

  private static class CallableResult {

    private final int pos;
    private final Object result;

    private CallableResult(int pos, Object result) {
      this.pos = pos;
      this.result = result;
    }
  }

  private DrillDownQuery getDrillDownQuery(final DrillDownQuery query, Query[] queries,
          final String excludedDimension) {
    final DrillDownQuery ddl = new DrillDownQuery(config, query.getBaseQuery());
    query.getDims().forEach((dim, pos) -> {
      if (!dim.equals(excludedDimension))
        ddl.add(dim, queries[pos]);
    });
    return ddl.getDims().size() == queries.length ? null : ddl;
  }

  /** Runs a search, using a {@link CollectorManager} to gather and merge search results */
  @SuppressWarnings("unchecked")
  public <R> ConcurrentDrillSidewaysResult<R> search(final DrillDownQuery query,
          final CollectorManager<?, R> hitCollectorManager) throws IOException {

    final Map<String, Integer> drillDownDims = query.getDims();
    final List<CallableCollector> callableCollectors = new ArrayList<>(drillDownDims.size() + 1);

    // Add the main DrillDownQuery
    callableCollectors.add(new CallableCollector(-1, searcher, query,
            new MultiCollectorManager(new FacetsCollectorManager(), hitCollectorManager)));
    int i = 0;
    final Query[] filters = query.getDrillDownQueries();
    for (String dim : drillDownDims.keySet())
      callableCollectors.add(new CallableCollector(i++, searcher, getDrillDownQuery(query, filters, dim),
              new FacetsCollectorManager()));

    final FacetsCollector mainFacetsCollector;
    final FacetsCollector[] facetsCollectors = new FacetsCollector[drillDownDims.size()];
    final R collectorResult;

    try {
      // Run the query pool
      final List<Future<CallableResult>> futures = executor.invokeAll(callableCollectors);

      // Extract the results
      final Object[] mainResults = (Object[]) futures.get(0).get().result;
      mainFacetsCollector = (FacetsCollector) mainResults[0];
      collectorResult = (R) mainResults[1];
      for (i = 1; i < futures.size(); i++) {
        final CallableResult result = futures.get(i).get();
        facetsCollectors[result.pos] = (FacetsCollector) result.result;
      }
      // Fill the null results with the mainFacetsCollector
      for (i = 0; i < facetsCollectors.length; i++)
        if (facetsCollectors[i] == null)
          facetsCollectors[i] = mainFacetsCollector;

    } catch (InterruptedException e) {
      throw new ThreadInterruptedException(e);
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }

    // build the facets and return the result
    return new ConcurrentDrillSidewaysResult<>(buildFacetsResult(mainFacetsCollector, facetsCollectors,
            drillDownDims.keySet().toArray(new String[drillDownDims.size()])), null, collectorResult);
  }

  /**
   * Result of a concurrent drill sideways search, including the
   * {@link Facets} and {@link TopDocs}.
   */
  public static class ConcurrentDrillSidewaysResult<R> extends DrillSidewaysResult {

    /** The merged search results */
    public final R collectorResult;

    /**
     * Sole constructor.
     */
    ConcurrentDrillSidewaysResult(Facets facets, TopDocs hits, R collectorResult) {
      super(facets, hits);
      this.collectorResult = collectorResult;
    }
  }
}

