package org.apache.lucene.server;

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
import org.apache.lucene.facet.range.Range;
import org.apache.lucene.facet.range.RangeAccumulator;
import org.apache.lucene.facet.range.RangeFacetRequest;
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
 *  them between the normal accumulator and a
 *  RangeAccumulator */
public class MultiFacetsAccumulator extends FacetsAccumulator {
  private final FacetsAccumulator a;
  private final RangeAccumulator ra;
  private final FacetSearchParams fspOrig;

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
      return FacetsAccumulator.create(fsp, indexReader, taxoReader, arrays);
    } else if (nonRangeRequests.isEmpty()) {
      return new RangeAccumulator(rangeRequests);
    } else {
      FacetsAccumulator a = FacetsAccumulator.create(new FacetSearchParams(fsp.indexingParams, nonRangeRequests), indexReader, taxoReader, arrays);
      RangeAccumulator ra = new RangeAccumulator(rangeRequests);
      return new MultiFacetsAccumulator(a, ra, fsp);
    }
  }

  private MultiFacetsAccumulator(FacetsAccumulator a, RangeAccumulator ra, FacetSearchParams fspOrig) {
    super(fspOrig);
    this.a = a;
    this.ra = ra;
    this.fspOrig = fspOrig;
  }

  @Override
  public boolean requiresDocScores() {
    return a.requiresDocScores();
  }

  public List<FacetResult> accumulate(List<MatchingDocs> matchingDocs) throws IOException {
    List<FacetResult> results = a.accumulate(matchingDocs);
    List<FacetResult> rangeResults = ra.accumulate(matchingDocs);

    int aUpto = 0;
    int raUpto = 0;
    List<FacetResult> merged = new ArrayList<FacetResult>();
    for(FacetRequest fr : fspOrig.facetRequests) {
      if (fr instanceof RangeFacetRequest) {
        merged.add(rangeResults.get(raUpto++));
      } else {
        merged.add(results.get(aUpto++));
      }
    }

    return merged;
  }
}
