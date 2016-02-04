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
package org.apache.lucene.search.join;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.LongBitSet;
import org.apache.lucene.util.LongValues;

import java.io.IOException;

/**
 * A collector that collects all ordinals from a specified field matching the query.
 *
 * @lucene.experimental
 */
final class GlobalOrdinalsCollector implements Collector {

  final String field;
  final LongBitSet collectedOrds;
  final MultiDocValues.OrdinalMap ordinalMap;

  GlobalOrdinalsCollector(String field, MultiDocValues.OrdinalMap ordinalMap, long valueCount) {
    this.field = field;
    this.ordinalMap = ordinalMap;
    this.collectedOrds = new LongBitSet(valueCount);
  }

  public LongBitSet getCollectorOrdinals() {
    return collectedOrds;
  }

  @Override
  public boolean needsScores() {
    return false;
  }

  @Override
  public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
    SortedDocValues docTermOrds = DocValues.getSorted(context.reader(), field);
    if (ordinalMap != null) {
      LongValues segmentOrdToGlobalOrdLookup = ordinalMap.getGlobalOrds(context.ord);
      return new OrdinalMapCollector(docTermOrds, segmentOrdToGlobalOrdLookup);
    } else {
      return new SegmentOrdinalCollector(docTermOrds);
    }
  }

  final class OrdinalMapCollector implements LeafCollector {

    private final SortedDocValues docTermOrds;
    private final LongValues segmentOrdToGlobalOrdLookup;

    OrdinalMapCollector(SortedDocValues docTermOrds, LongValues segmentOrdToGlobalOrdLookup) {
      this.docTermOrds = docTermOrds;
      this.segmentOrdToGlobalOrdLookup = segmentOrdToGlobalOrdLookup;
    }

    @Override
    public void collect(int doc) throws IOException {
      final long segmentOrd = docTermOrds.getOrd(doc);
      if (segmentOrd != -1) {
        final long globalOrd = segmentOrdToGlobalOrdLookup.get(segmentOrd);
        collectedOrds.set(globalOrd);
      }
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
    }
  }

  final class SegmentOrdinalCollector implements LeafCollector {

    private final SortedDocValues docTermOrds;

    SegmentOrdinalCollector(SortedDocValues docTermOrds) {
      this.docTermOrds = docTermOrds;
    }

    @Override
    public void collect(int doc) throws IOException {
      final long segmentOrd = docTermOrds.getOrd(doc);
      if (segmentOrd != -1) {
        collectedOrds.set(segmentOrd);
      }
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
    }
  }

}
