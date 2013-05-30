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

import org.apache.lucene.facet.params.FacetSearchParams;
import org.apache.lucene.facet.search.FacetRequest;
import org.apache.lucene.facet.search.FacetResult;
import org.apache.lucene.facet.search.FacetResultNode;
import org.apache.lucene.facet.search.FacetsAccumulator;
import org.apache.lucene.facet.search.FacetsAggregator;
import org.apache.lucene.facet.search.FacetsCollector.MatchingDocs;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.NumericDocValues;

/** Uses a {@link NumericDocValues} and accumulates
 *  counts for provided ranges.  This is dynamic (does not
 *  use the taxonomy index or anything from the index
 *  except the NumericDocValuesField). */

public class RangeAccumulator extends FacetsAccumulator {

  static class RangeSet {
    final Range[] ranges;
    final String field;

    public RangeSet(Range[] ranges, String field) {
      this.ranges = ranges;
      this.field = field;
    }
  }

  final List<RangeSet> requests = new ArrayList<RangeSet>();

  public RangeAccumulator(FacetSearchParams fsp, IndexReader reader) {
    super(fsp, reader, null, null);

    for(FacetRequest fr : fsp.facetRequests) {

      if (!(fr instanceof RangeFacetRequest)) {
        throw new IllegalArgumentException("only RangeFacetRequest is supported; got " + fsp.facetRequests.get(0).getClass());
      }

      if (fr.categoryPath.length != 1) {
        throw new IllegalArgumentException("only flat (dimension only) CategoryPath is allowed");
      }

      RangeFacetRequest<?> rfr = (RangeFacetRequest) fr;

      requests.add(new RangeSet(rfr.ranges, rfr.categoryPath.components[0]));
    }
  }

  @Override
  public FacetsAggregator getAggregator() {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<FacetResult> accumulate(List<MatchingDocs> matchingDocs) throws IOException {

    // TODO: test if this is faster (in the past it was
    // faster to do MachingDocs on the inside) ... see
    // patches on LUCENE-4965):
    List<FacetResult> results = new ArrayList<FacetResult>();
    for(int i=0;i<requests.size();i++) {
      RangeSet ranges = requests.get(i);

      int[] counts = new int[ranges.ranges.length];
      for(MatchingDocs hits : matchingDocs) {
        NumericDocValues ndv = hits.context.reader().getNumericDocValues(ranges.field);
        final int length = hits.bits.length();
        int doc = 0;
        while (doc < length && (doc = hits.bits.nextSetBit(doc)) != -1) {
          long v = ndv.get(doc);
          // TODO: use interval tree instead of linear search:
          for(int j=0;j<ranges.ranges.length;j++) {
            if (ranges.ranges[j].accept(v)) {
              counts[j]++;
            }
          }

          doc++;
        }
      }

      List<FacetResultNode> nodes = new ArrayList<FacetResultNode>(ranges.ranges.length);
      for(int j=0;j<ranges.ranges.length;j++) {
        nodes.add(new RangeFacetResultNode(ranges.field, ranges.ranges[j], counts[j]));
      }

      FacetResultNode rootNode = new FacetResultNode(-1, 0);
      rootNode.label = new CategoryPath(ranges.field);
      rootNode.subResults = nodes;

      results.add(new FacetResult(searchParams.facetRequests.get(i), rootNode, nodes.size()));
    }

    return results;
  }

  @Override
  public boolean requiresDocScores() {
    return false;
  }
}
