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
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MultiCollector;
import org.apache.lucene.search.MultiCollectorManager;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.util.ThreadInterruptedException;

/**
 * Computes drill down and sideways counts for the provided {@link DrillDownQuery}. Drill sideways
 * counts include alternative values/aggregates for the drill-down dimensions so that a dimension
 * does not disappear after the user drills down into it.
 *
 * <p>Use one of the static search methods to do the search, and then get the hits and facet results
 * from the returned {@link DrillSidewaysResult}.
 *
 * <p>There is both a "standard" and "concurrent" implementation for drill sideways. The concurrent
 * approach is enabled by providing an {@code ExecutorService} to the ctor. The concurrent
 * implementation may be a little faster but does duplicate work (which grows linearly with the
 * number of drill down dimensions specified on the provided {@link DrillDownQuery}). The duplicate
 * work may impact the overall throughput of a system. The standard approach may be a little slower
 * but avoids duplicate computations and query processing. Note that both approaches are compatible
 * with concurrent searching across segments (i.e., if using an {@link IndexSearcher} constructed
 * with an {@code Executor}).
 *
 * <p><b>NOTE</b>: this allocates one {@link FacetsCollector} for each drill-down, plus one. If your
 * index has high number of facet labels then this will multiply your memory usage.
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

  /**
   * (optional) {@link ExecutorService} used for "concurrent" drill sideways if desired.
   */
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
   * Create a new {@code DrillSideways} instance, where some dimensions were indexed with {@link
   * SortedSetDocValuesFacetField} and others were indexed with {@link FacetField}.
   *
   * <p>Use this constructor to use the concurrent implementation
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
   * Subclass can override to customize drill down facets collector. Returning {@code null} is valid
   * if no drill down facet collection is needed.
   */
  protected FacetsCollector createDrillDownFacetsCollector() {
    return new FacetsCollector();
  }

  /**
   * Subclass can override to customize drill down facets collector. Returning {@code null} is valid
   * if no drill down facet collection is needed.
   */
  protected FacetsCollectorManager createDrillDownFacetsCollectorManager() {
    return new FacetsCollectorManager();
  }

  /**
   * Subclass can override to customize per-dim Facets
   * impl.
   */
  protected Facets buildFacetsResult(FacetsCollector drillDowns, FacetsCollector[] drillSideways,
          String[] drillSidewaysDims) throws IOException {

    Facets drillDownFacets = null;
    Map<String, Facets> drillSidewaysFacets = new HashMap<>();

    if (taxoReader != null) {
      if (drillDowns != null) {
        drillDownFacets = new FastTaxonomyFacetCounts(taxoReader, config, drillDowns);
      }
      if (drillSideways != null) {
        for (int i = 0; i < drillSideways.length; i++) {
          drillSidewaysFacets.put(drillSidewaysDims[i],
                  new FastTaxonomyFacetCounts(taxoReader, config, drillSideways[i]));
        }
      }
    } else {
      if (drillDowns != null) {
        drillDownFacets = new SortedSetDocValuesFacetCounts(state, drillDowns);
      }
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
   * Search, collecting hits with a {@link Collector}, and computing drill down and sideways counts.
   *
   * <p>Note that "concurrent" drill sideways will not be invoked here, even if an {@link
   * ExecutorService} was supplied to the ctor, since {@code Collector}s are not thread-safe. If
   * interested in concurrent drill sideways, please use one of the other static {@code search}
   * methods.
   */
  public DrillSidewaysResult search(DrillDownQuery query, Collector hitCollector) throws IOException {

    Map<String, Integer> drillDownDims = query.getDims();

    if (drillDownDims.isEmpty()) {
      // There are no drill-down dims, so there is no
      // drill-sideways to compute:
      FacetsCollector drillDownCollector = createDrillDownFacetsCollector();
      if (drillDownCollector != null) {
        // Make sure we still populate a facet collector for the base query if desired:
        searcher.search(query, MultiCollector.wrap(hitCollector, drillDownCollector));
      } else {
        searcher.search(query, hitCollector);
      }
      return new DrillSidewaysResult(
          buildFacetsResult(drillDownCollector, null, null), null, drillDownCollector, null, null);
    }

    Query baseQuery = query.getBaseQuery();
    if (baseQuery == null) {
      // TODO: we could optimize this pure-browse case by
      // making a custom scorer instead:
      baseQuery = new MatchAllDocsQuery();
    }
    Query[] drillDownQueries = query.getDrillDownQueries();

    int numDims = drillDownDims.size();

    FacetsCollectorManager drillDownCollectorManager = createDrillDownFacetsCollectorManager();

    FacetsCollectorManager[] drillSidewaysFacetsCollectorManagers =
        new FacetsCollectorManager[numDims];
    for (int i = 0; i < numDims; i++) {
      drillSidewaysFacetsCollectorManagers[i] = new FacetsCollectorManager();
    }

    DrillSidewaysQuery dsq =
        new DrillSidewaysQuery(
            baseQuery,
            drillDownCollectorManager,
            drillSidewaysFacetsCollectorManagers,
            drillDownQueries,
            scoreSubDocsAtOnce());

    searcher.search(dsq, hitCollector);

    FacetsCollector drillDownCollector;
    if (drillDownCollectorManager != null) {
      drillDownCollector = drillDownCollectorManager.reduce(dsq.managedDrillDownCollectors);
    } else {
      drillDownCollector = null;
    }

    FacetsCollector[] drillSidewaysCollectors = new FacetsCollector[numDims];
    int numSlices = dsq.managedDrillSidewaysCollectors.size();

    for (int dim = 0; dim < numDims; dim++) {
      List<FacetsCollector> facetsCollectorsForDim = new ArrayList<>(numSlices);

      for (int slice = 0; slice < numSlices; slice++) {
        facetsCollectorsForDim.add(dsq.managedDrillSidewaysCollectors.get(slice)[dim]);
      }

      drillSidewaysCollectors[dim] =
          drillSidewaysFacetsCollectorManagers[dim].reduce(facetsCollectorsForDim);
    }

    String[] drillSidewaysDims = drillDownDims.keySet().toArray(new String[0]);

    return new DrillSidewaysResult(
        buildFacetsResult(drillDownCollector, drillSidewaysCollectors, drillSidewaysDims),
        null,
        drillDownCollector,
        drillSidewaysCollectors,
        drillSidewaysDims);
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
        ConcurrentDrillSidewaysResult<TopFieldDocs> r = searchConcurrently(query, collectorManager);
        TopFieldDocs topDocs = r.collectorResult;
        if (doDocScores) {
          TopFieldCollector.populateScores(topDocs.scoreDocs, searcher, query);
        }
        return new DrillSidewaysResult(
            r.facets,
            topDocs,
            r.drillDownFacetsCollector,
            r.drillSidewaysFacetsCollector,
            r.drillSidewaysDims);

      } else {

        final TopFieldCollector hitCollector =
                TopFieldCollector.create(sort, fTopN, after, Integer.MAX_VALUE);
        DrillSidewaysResult r = search(query, hitCollector);
        TopFieldDocs topDocs = hitCollector.topDocs();
        if (doDocScores) {
          TopFieldCollector.populateScores(topDocs.scoreDocs, searcher, query);
        }
        return new DrillSidewaysResult(
            r.facets,
            topDocs,
            r.drillDownFacetsCollector,
            r.drillSidewaysFacetsCollector,
            r.drillSidewaysDims);
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
      ConcurrentDrillSidewaysResult<TopDocs> r = searchConcurrently(query, collectorManager);
      return new DrillSidewaysResult(
          r.facets,
          r.collectorResult,
          r.drillDownFacetsCollector,
          r.drillSidewaysFacetsCollector,
          r.drillSidewaysDims);

    } else {

      TopScoreDocCollector hitCollector = TopScoreDocCollector.create(topN, after, Integer.MAX_VALUE);
      DrillSidewaysResult r = search(query, hitCollector);
      return new DrillSidewaysResult(
          r.facets,
          hitCollector.topDocs(),
          r.drillDownFacetsCollector,
          r.drillSidewaysFacetsCollector,
          r.drillSidewaysDims);
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
     * FacetsCollector populated based on hits that match the full DrillDownQuery, treating all
     * drill down dimensions as required clauses. Useful for advanced use-cases that want to compute
     * Facets results separate from the provided Facets in this result.
     */
    public final FacetsCollector drillDownFacetsCollector;

    /**
     * FacetsCollectors populated for each drill sideways dimension. Each collector exposes the hits
     * that match on all DrillDownQuery dimensions, but treating their corresponding sideways
     * dimension as optional. This array provides a FacetsCollector for each drill down dimension
     * present in the original DrillDownQuery, and the associated dimension for each FacetsCollector
     * can be determined using the parallel {@link DrillSidewaysResult#drillSidewaysDims} array.
     * Useful for advanced use-cases that want to compute Facets results separate from the provided
     * Facets in this result.
     */
    public final FacetsCollector[] drillSidewaysFacetsCollector;

    /**
     * Dimensions that correspond to to the {@link DrillSidewaysResult#drillSidewaysFacetsCollector}
     */
    public final String[] drillSidewaysDims;

    /** Sole constructor. */
    public DrillSidewaysResult(
        Facets facets,
        TopDocs hits,
        FacetsCollector drillDownFacetsCollector,
        FacetsCollector[] drillSidewaysFacetsCollector,
        String[] drillSidewaysDims) {
      this.facets = facets;
      this.hits = hits;
      this.drillDownFacetsCollector = drillDownFacetsCollector;
      this.drillSidewaysFacetsCollector = drillSidewaysFacetsCollector;
      this.drillSidewaysDims = drillSidewaysDims;
    }

    /**
     * Constructor to maintain backwards-compatibility with 8.x.
     * @deprecated Please see alternate constructor
     */
    @Deprecated
    public DrillSidewaysResult(Facets facets, TopDocs hits) {
      this(facets, hits, null, null, null);
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
  public <R> ConcurrentDrillSidewaysResult<R> search(
      final DrillDownQuery query, final CollectorManager<?, R> hitCollectorManager)
      throws IOException {
    if (executor != null) {
      return searchConcurrently(query, hitCollectorManager);
    } else {
      return searchSequentially(query, hitCollectorManager);
    }
  }

  @SuppressWarnings("unchecked")
  private <R> ConcurrentDrillSidewaysResult<R> searchSequentially(
      final DrillDownQuery query, final CollectorManager<?, R> hitCollectorManager)
      throws IOException {

    Map<String, Integer> drillDownDims = query.getDims();

    if (drillDownDims.isEmpty()) {
      // There are no drill-down dims, so there is no
      // drill-sideways to compute:
      FacetsCollectorManager drillDownCollectorManager = createDrillDownFacetsCollectorManager();
      FacetsCollector mainFacetsCollector;
      R collectorResult;
      if (drillDownCollectorManager != null) {
        Object[] mainResults =
            searcher.search(
                query, new MultiCollectorManager(drillDownCollectorManager, hitCollectorManager));
        // Extract the results:
        mainFacetsCollector = (FacetsCollector) mainResults[0];
        collectorResult = (R) mainResults[1];
      } else {
        mainFacetsCollector = null;
        collectorResult = searcher.search(query, hitCollectorManager);
      }

      return new ConcurrentDrillSidewaysResult<>(
          buildFacetsResult(mainFacetsCollector, null, null),
          null,
          collectorResult,
          mainFacetsCollector,
          null,
          null);
    }

    Query baseQuery = query.getBaseQuery();
    if (baseQuery == null) {
      // TODO: we could optimize this pure-browse case by
      // making a custom scorer instead:
      baseQuery = new MatchAllDocsQuery();
    }
    Query[] drillDownQueries = query.getDrillDownQueries();

    int numDims = drillDownDims.size();

    FacetsCollectorManager drillDownCollectorManager = createDrillDownFacetsCollectorManager();

    FacetsCollectorManager[] drillSidewaysFacetsCollectorManagers =
        new FacetsCollectorManager[numDims];
    for (int i = 0; i < numDims; i++) {
      drillSidewaysFacetsCollectorManagers[i] = new FacetsCollectorManager();
    }

    DrillSidewaysQuery dsq =
        new DrillSidewaysQuery(
            baseQuery,
            drillDownCollectorManager,
            drillSidewaysFacetsCollectorManagers,
            drillDownQueries,
            scoreSubDocsAtOnce());

    R collectorResult = searcher.search(dsq, hitCollectorManager);

    FacetsCollector drillDownCollector;
    if (drillDownCollectorManager != null) {
      drillDownCollector = drillDownCollectorManager.reduce(dsq.managedDrillDownCollectors);
    } else {
      drillDownCollector = null;
    }

    FacetsCollector[] drillSidewaysCollectors = new FacetsCollector[numDims];
    int numSlices = dsq.managedDrillSidewaysCollectors.size();

    for (int dim = 0; dim < numDims; dim++) {
      List<FacetsCollector> facetsCollectorsForDim = new ArrayList<>(numSlices);

      for (int slice = 0; slice < numSlices; slice++) {
        facetsCollectorsForDim.add(dsq.managedDrillSidewaysCollectors.get(slice)[dim]);
      }

      drillSidewaysCollectors[dim] =
          drillSidewaysFacetsCollectorManagers[dim].reduce(facetsCollectorsForDim);
    }

    String[] drillSidewaysDims = drillDownDims.keySet().toArray(new String[0]);

    return new ConcurrentDrillSidewaysResult<>(
        buildFacetsResult(drillDownCollector, drillSidewaysCollectors, drillSidewaysDims),
        null,
        collectorResult,
        drillDownCollector,
        drillSidewaysCollectors,
        drillSidewaysDims);
  }

  @SuppressWarnings("unchecked")
  private <R> ConcurrentDrillSidewaysResult<R> searchConcurrently(
      final DrillDownQuery query, final CollectorManager<?, R> hitCollectorManager)
      throws IOException {

    final Map<String, Integer> drillDownDims = query.getDims();
    final List<CallableCollector> callableCollectors = new ArrayList<>(drillDownDims.size() + 1);

    // Add the main DrillDownQuery
    FacetsCollectorManager drillDownFacetsCollectorManager =
        createDrillDownFacetsCollectorManager();
    CollectorManager<?, ?> mainCollectorManager;
    if (drillDownFacetsCollectorManager != null) {
      // Make sure we populate a facet collector corresponding to the base query if desired:
      mainCollectorManager =
          new MultiCollectorManager(drillDownFacetsCollectorManager, hitCollectorManager);
    } else {
      mainCollectorManager = hitCollectorManager;
    }
    callableCollectors.add(new CallableCollector(-1, searcher, query, mainCollectorManager));
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
      if (drillDownFacetsCollectorManager != null) {
        // If we populated a facets collector for the main query, make sure to unpack it properly
        final Object[] mainResults = (Object[]) futures.get(0).get().result;
        mainFacetsCollector = (FacetsCollector) mainResults[0];
        collectorResult = (R) mainResults[1];
      } else {
        mainFacetsCollector = null;
        collectorResult = (R) futures.get(0).get().result;
      }
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

    String[] drillSidewaysDims = drillDownDims.keySet().toArray(new String[0]);

    // build the facets and return the result
    return new ConcurrentDrillSidewaysResult<>(
        buildFacetsResult(mainFacetsCollector, facetsCollectors, drillSidewaysDims),
        null,
        collectorResult,
        mainFacetsCollector,
        facetsCollectors,
        drillSidewaysDims);
  }

  /**
   * Result of a concurrent drill sideways search, including the
   * {@link Facets} and {@link TopDocs}.
   */
  public static class ConcurrentDrillSidewaysResult<R> extends DrillSidewaysResult {

    /** The merged search results */
    public final R collectorResult;

    /** Sole constructor. */
    ConcurrentDrillSidewaysResult(
        Facets facets,
        TopDocs hits,
        R collectorResult,
        FacetsCollector drillDownFacetsCollector,
        FacetsCollector[] drillSidewaysFacetsCollector,
        String[] drillSidewaysDims) {
      super(
          facets, hits, drillDownFacetsCollector, drillSidewaysFacetsCollector, drillSidewaysDims);
      this.collectorResult = collectorResult;
    }

    /**
     * Constructor to maintain backwards-compatibility with 8.x.
     * @deprecated Please see alternate constructor
     */
    @Deprecated
    ConcurrentDrillSidewaysResult(Facets facets, TopDocs hits, R collectorResult) {
      super(facets, hits, null, null, null);
      this.collectorResult = collectorResult;
    }
  }
}

