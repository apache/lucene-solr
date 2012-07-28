package org.apache.lucene.facet.search;

import java.io.IOException;
import java.util.List;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Scorer;

import org.apache.lucene.facet.search.params.FacetRequest;
import org.apache.lucene.facet.search.params.FacetSearchParams;
import org.apache.lucene.facet.search.results.FacetResult;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;

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

/**
 * Collector for facet accumulation. *
 * 
 * @lucene.experimental
 */
public class FacetsCollector extends Collector {

  protected final FacetsAccumulator facetsAccumulator;
  private ScoredDocIdCollector scoreDocIdCollector;
  private List<FacetResult> results;
  private Object resultsGuard;

  /**
   * Create a collector for accumulating facets while collecting documents
   * during search.
   * 
   * @param facetSearchParams
   *          faceted search parameters defining which facets are required and
   *          how.
   * @param indexReader
   *          searched index.
   * @param taxonomyReader
   *          taxonomy containing the facets.
   */
  public FacetsCollector(FacetSearchParams facetSearchParams,
                          IndexReader indexReader, TaxonomyReader taxonomyReader) {
    facetsAccumulator = initFacetsAccumulator(facetSearchParams, indexReader, taxonomyReader);
    scoreDocIdCollector = initScoredDocCollector(facetSearchParams, indexReader, taxonomyReader);
    resultsGuard = new Object();
  }

  /**
   * Create a {@link ScoredDocIdCollector} to be used as the first phase of
   * the facet collection. If all facetRequests are do not require the
   * document score, a ScoredDocIdCollector which does not store the document
   * scores would be returned. Otherwise a SDIC which does store the documents
   * will be returned, having an initial allocated space for 1000 such
   * documents' scores.
   */
  protected ScoredDocIdCollector initScoredDocCollector(
      FacetSearchParams facetSearchParams, IndexReader indexReader,
      TaxonomyReader taxonomyReader) {
    boolean scoresNeeded = false;
    for (FacetRequest frq : facetSearchParams.getFacetRequests()) {
      if (frq.requireDocumentScore()) {
        scoresNeeded = true;
        break;
      }
    }
    return ScoredDocIdCollector.create(indexReader.maxDoc(), scoresNeeded);
  }

  /**
   * Create the {@link FacetsAccumulator} to be used. Default is 
   * {@link StandardFacetsAccumulator}. Called once at the constructor of the collector.
   * 
   * @param facetSearchParams
   *            The search params.
   * @param indexReader
   *            A reader to the index to search in.
   * @param taxonomyReader
   *            A reader to the active taxonomy.
   * @return The {@link FacetsAccumulator} to use.
   */
  protected FacetsAccumulator initFacetsAccumulator(FacetSearchParams facetSearchParams,
                                                    IndexReader indexReader,
                                                    TaxonomyReader taxonomyReader) {
    return new StandardFacetsAccumulator(facetSearchParams, indexReader, taxonomyReader);
  }

  /**
   * Return accumulated facets results (according to faceted search parameters) 
   * for collected documents.
   * @throws IOException on error
   */
  public List<FacetResult> getFacetResults() throws IOException {
    synchronized (resultsGuard) { // over protection 
      if (results == null) {
        // lazy creation but just once
        results = facetsAccumulator.accumulate(scoreDocIdCollector.getScoredDocIDs());
        scoreDocIdCollector = null;
      }
      return results;
    }
  }

  @Override
  public boolean acceptsDocsOutOfOrder() {
    return false;
  }

  @Override
  public void collect(int doc) throws IOException {
    scoreDocIdCollector.collect(doc);
  }

  @Override
  public void setNextReader(AtomicReaderContext context) throws IOException {
    scoreDocIdCollector.setNextReader(context);
  }

  @Override
  public void setScorer(Scorer scorer) throws IOException {
    scoreDocIdCollector.setScorer(scorer);
  }

}
