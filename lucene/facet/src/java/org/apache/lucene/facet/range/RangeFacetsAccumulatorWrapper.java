package org.apache.lucene.facet.range;

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
import java.util.List;
import java.util.Set;

import org.apache.lucene.facet.params.CategoryListParams;
import org.apache.lucene.facet.params.FacetSearchParams;
import org.apache.lucene.facet.search.FacetArrays;
import org.apache.lucene.facet.search.FacetRequest;
import org.apache.lucene.facet.search.FacetResult;
import org.apache.lucene.facet.search.FacetResultsHandler;
import org.apache.lucene.facet.search.FacetsAccumulator;
import org.apache.lucene.facet.search.FacetsAggregator;
import org.apache.lucene.facet.search.FacetsCollector.MatchingDocs;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.index.IndexReader;

/** Takes multiple facet requests and if necessary splits
 *  them between the normal {@link FacetsAccumulator} and a
 *  {@link RangeAccumulator} */
public class RangeFacetsAccumulatorWrapper extends FacetsAccumulator {
  // TODO: somehow handle SortedSetDVAccumulator as
  // well... but it's tricky because SSDV just uses an
  // "ordinary" flat CountFacetRequest so we can't switch
  // based on that.
  private final FacetsAccumulator accumulator;
  private final RangeAccumulator rangeAccumulator;

  public static FacetsAccumulator create(FacetSearchParams fsp, IndexReader indexReader, TaxonomyReader taxoReader) {
    return create(fsp, indexReader, taxoReader, new FacetArrays(taxoReader.getSize()));
  }

  public static FacetsAccumulator create(FacetSearchParams fsp, IndexReader indexReader, TaxonomyReader taxoReader, FacetArrays arrays) {
    List<FacetRequest> rangeRequests = new ArrayList<FacetRequest>();
    List<FacetRequest> nonRangeRequests = new ArrayList<FacetRequest>();
    for(FacetRequest fr : fsp.facetRequests) {
      if (fr instanceof RangeFacetRequest) {
        rangeRequests.add(fr);
      } else {
        nonRangeRequests.add(fr);
      }
    }

    if (rangeRequests.isEmpty()) {
      return new FacetsAccumulator(fsp, indexReader, taxoReader, arrays);
    } else if (nonRangeRequests.isEmpty()) {
      return new RangeAccumulator(fsp, indexReader);
    } else {
      FacetsAccumulator accumulator = new FacetsAccumulator(new FacetSearchParams(fsp.indexingParams, nonRangeRequests), indexReader, taxoReader, arrays);
      RangeAccumulator rangeAccumulator = new RangeAccumulator(new FacetSearchParams(fsp.indexingParams, rangeRequests), indexReader);
      return new RangeFacetsAccumulatorWrapper(accumulator, rangeAccumulator, fsp);
    }
  }

  private RangeFacetsAccumulatorWrapper(FacetsAccumulator accumulator, RangeAccumulator rangeAccumulator, FacetSearchParams fsp) {
    super(fsp, accumulator.indexReader, accumulator.taxonomyReader);
    this.accumulator = accumulator;
    this.rangeAccumulator = rangeAccumulator;
  }

  @Override
  public FacetsAggregator getAggregator() {
    throw new UnsupportedOperationException();
  }

  @Override
  protected FacetResultsHandler createFacetResultsHandler(FacetRequest fr) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected Set<CategoryListParams> getCategoryLists() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean requiresDocScores() {
    return accumulator.requiresDocScores();
  }

  public List<FacetResult> accumulate(List<MatchingDocs> matchingDocs) throws IOException {
    List<FacetResult> results = accumulator.accumulate(matchingDocs);
    List<FacetResult> rangeResults = rangeAccumulator.accumulate(matchingDocs);

    int aUpto = 0;
    int raUpto = 0;
    List<FacetResult> merged = new ArrayList<FacetResult>();
    for(FacetRequest fr : searchParams.facetRequests) {
      if (fr instanceof RangeFacetRequest) {
        merged.add(rangeResults.get(raUpto++));
      } else {
        merged.add(results.get(aUpto++));
      }
    }

    return merged;
  }
}
