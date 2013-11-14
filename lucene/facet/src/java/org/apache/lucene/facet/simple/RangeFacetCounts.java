package org.apache.lucene.facet.simple;

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
import java.util.Collections;
import java.util.List;

import org.apache.lucene.facet.range.Range;
import org.apache.lucene.facet.simple.SimpleFacetsCollector.MatchingDocs;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.LongFieldSource;

/**
 * Uses {@link RangeFacetRequest#getValues(AtomicReaderContext)} and accumulates
 * counts for provided ranges.
 */
public class RangeFacetCounts extends Facets {
  private final Range[] ranges;
  private final int[] counts;
  private int totCount;

  public RangeFacetCounts(String field, SimpleFacetsCollector hits, Range... ranges) throws IOException {
    this(new LongFieldSource(field), hits, ranges);
  }

  public RangeFacetCounts(ValueSource valueSource, SimpleFacetsCollector hits, Range... ranges) throws IOException {
    this.ranges = ranges;
    counts = new int[ranges.length];
    count(valueSource, hits.getMatchingDocs());
  }

  private void count(ValueSource valueSource, List<MatchingDocs> matchingDocs) throws IOException {

    // TODO: test if this is faster (in the past it was
    // faster to do MatchingDocs on the inside) ... see
    // patches on LUCENE-4965):
    for (MatchingDocs hits : matchingDocs) {
      FunctionValues fv = valueSource.getValues(Collections.emptyMap(), hits.context);
      final int length = hits.bits.length();
      int doc = 0;
      totCount += hits.totalHits;
      while (doc < length && (doc = hits.bits.nextSetBit(doc)) != -1) {
        // Skip missing docs:
        if (fv.exists(doc)) {
          
          long v = fv.longVal(doc);

          // TODO: if all ranges are non-overlapping, we
          // should instead do a bin-search up front
          // (really, a specialized case of the interval
          // tree)
          // TODO: use interval tree instead of linear search:
          for (int j = 0; j < ranges.length; j++) {
            if (ranges[j].accept(v)) {
              counts[j]++;
            }
          }
        }

        doc++;
      }
    }
  }

  // nocommit all args are ... unused ... this doesn't "fit"
  // very well:

  @Override
  public SimpleFacetResult getTopChildren(int topN, String dim, String... path) {
    LabelAndValue[] labelValues = new LabelAndValue[counts.length];
    for(int i=0;i<counts.length;i++) {
      // nocommit can we add the range into this?
      labelValues[i] = new LabelAndValue(ranges[i].label, counts[i]);
    }

    return new SimpleFacetResult(null, totCount, labelValues);
  }

  @Override
  public Number getSpecificValue(String dim, String... path) throws IOException {
    // nocommit we could impl this?
    throw new UnsupportedOperationException();
  }

  @Override
  public List<SimpleFacetResult> getAllDims(int topN) throws IOException {
    return Collections.singletonList(getTopChildren(topN, null));
  }
}
