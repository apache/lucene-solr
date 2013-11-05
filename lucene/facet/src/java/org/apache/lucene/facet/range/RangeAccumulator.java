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
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.facet.params.FacetSearchParams;
import org.apache.lucene.facet.search.FacetRequest;
import org.apache.lucene.facet.search.FacetResult;
import org.apache.lucene.facet.search.FacetResultNode;
import org.apache.lucene.facet.search.FacetsAccumulator;
import org.apache.lucene.facet.search.FacetsCollector.MatchingDocs;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.queries.function.FunctionValues;

/**
 * Uses {@link RangeFacetRequest#getValues(AtomicReaderContext)} and accumulates
 * counts for provided ranges.
 */
public class RangeAccumulator extends FacetsAccumulator {

  public RangeAccumulator(FacetRequest... facetRequests) {
    this(Arrays.asList(facetRequests));
  }
  
  public RangeAccumulator(List<FacetRequest> facetRequests) {
    super(new FacetSearchParams(facetRequests));
    for (FacetRequest fr : facetRequests) {
      if (!(fr instanceof RangeFacetRequest)) {
        throw new IllegalArgumentException("this accumulator only supports RangeFacetRequest; got " + fr);
      }

      if (fr.categoryPath.length != 1) {
        throw new IllegalArgumentException("only flat (dimension only) CategoryPath is allowed");
      }
    }
  }

  @Override
  public List<FacetResult> accumulate(List<MatchingDocs> matchingDocs) throws IOException {

    // TODO: test if this is faster (in the past it was
    // faster to do MachingDocs on the inside) ... see
    // patches on LUCENE-4965):
    List<FacetResult> results = new ArrayList<FacetResult>();
    for (FacetRequest req : searchParams.facetRequests) {
      RangeFacetRequest<?> rangeFR = (RangeFacetRequest<?>) req;
      int[] counts = new int[rangeFR.ranges.length];
      for (MatchingDocs hits : matchingDocs) {
        FunctionValues fv = rangeFR.getValues(hits.context);
        final int length = hits.bits.length();
        int doc = 0;
        while (doc < length && (doc = hits.bits.nextSetBit(doc)) != -1) {
          // Skip missing docs:
          if (!fv.exists(doc)) {
            ++doc;
            continue;
          }
          
          long v = fv.longVal(doc);

          // TODO: if all ranges are non-overlapping, we
          // should instead do a bin-search up front
          // (really, a specialized case of the interval
          // tree)
          // TODO: use interval tree instead of linear search:
          for (int j = 0; j < rangeFR.ranges.length; j++) {
            if (rangeFR.ranges[j].accept(v)) {
              counts[j]++;
            }
          }

          doc++;
        }
      }
      
      List<FacetResultNode> nodes = new ArrayList<FacetResultNode>(rangeFR.ranges.length);
      for (int j = 0; j < rangeFR.ranges.length; j++) {
        nodes.add(new RangeFacetResultNode(rangeFR.label, rangeFR.ranges[j], counts[j]));
      }
      
      FacetResultNode rootNode = new FacetResultNode(-1, 0);
      rootNode.label = rangeFR.categoryPath;
      rootNode.subResults = nodes;

      results.add(new FacetResult(req, rootNode, nodes.size()));
    }
    
    return results;
  }

  @Override
  public boolean requiresDocScores() {
    return false;
  }
}
