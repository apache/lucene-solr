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
package org.apache.solr.search.facet;

import java.io.IOException;
import java.util.Map;
import java.util.function.IntFunction;
import org.apache.lucene.index.IndexReader.CacheKey;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.util.LongValues;
import static org.apache.solr.request.TermFacetCache.mergeCachedSegmentCounts;

import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.request.TermFacetCache;
import org.apache.solr.request.TermFacetCache.CacheUpdater;
import org.apache.solr.request.TermFacetCache.FacetCacheKey;
import org.apache.solr.request.TermFacetCache.SegmentCacheEntry;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.QueryResultKey;
import org.apache.solr.search.SolrCache;
import org.apache.solr.search.facet.SlotAcc.CountSlotAcc;
import org.apache.solr.search.facet.SlotAcc.SweepCoordinationPoint;
import org.apache.solr.search.facet.SlotAcc.SweepCoordinator;

final class CacheUpdateCountSlotAcc extends CountSlotAcc implements CacheUpdater, SweepCoordinationPoint {

  //nocommit: probably make topLevelCounts a long[]?
  private final int[] topLevelCounts;
  private final Map<CacheKey, SegmentCacheEntry> cachedSegments;
  private final ByteBuffersDataOutput dataOutput;
  private final CacheKey topLevelCacheKey;
  private final SolrCache<FacetCacheKey, Map<CacheKey, SegmentCacheEntry>> facetCache;
  private final FacetCacheKey facetCacheKey;
  private final boolean includesMissingCount;
  private CacheKey leafCacheKey;
  private SegmentCacheEntry cached;
  private final SweepCoordinator sweepCoordinator;

  static SweepCountAccStruct create(FacetFieldProcessor p, int numSlots, Map<CacheKey, SegmentCacheEntry> cachedSegments,
      CacheKey topLevelCacheKey, SolrCache<FacetCacheKey, Map<CacheKey, SegmentCacheEntry>> facetCache,
      FacetCacheKey facetCacheKey, boolean includesMissingCount, QueryResultKey qKey, boolean isBase, DocSet docs, CacheState cacheState) {
    CacheUpdateCountSlotAcc count = new CacheUpdateCountSlotAcc(p, numSlots, cachedSegments, topLevelCacheKey, facetCache, facetCacheKey,
        includesMissingCount, qKey, isBase, docs, cacheState);
    return isBase ? count.sweepCoordinator.base : new SweepCountAccStruct(docs, isBase, count, qKey, cacheState, cachedSegments, count);
  }

  private CacheUpdateCountSlotAcc(FacetFieldProcessor p, int numSlots, Map<CacheKey, SegmentCacheEntry> cachedSegments,
      CacheKey topLevelCacheKey, SolrCache<FacetCacheKey, Map<CacheKey, SegmentCacheEntry>> facetCache,
      FacetCacheKey facetCacheKey, boolean includesMissingCount,
      QueryResultKey qKey, boolean isBase, DocSet docs, CacheState cacheState) {
    super(p.fcontext);
    this.topLevelCounts = new int[numSlots];
    this.cachedSegments = cachedSegments;
    final int initialBackingByteArraySize = numSlots; // probably ok as a rough initial size estimate
    this.dataOutput = new ByteBuffersDataOutput(initialBackingByteArraySize);
    this.topLevelCacheKey = topLevelCacheKey;
    this.facetCache = facetCache;
    this.facetCacheKey = facetCacheKey;
    this.includesMissingCount = includesMissingCount;
    if (!isBase) {
      this.sweepCoordinator = null;
    } else {
      SweepCountAccStruct struct = new SweepCountAccStruct(docs, isBase, this, qKey, cacheState, cachedSegments, this);
      this.sweepCoordinator = new SweepCoordinator(p, struct);
    }
  }

  @Override
  public SweepCoordinator getSweepCoordinator() {
    return sweepCoordinator;
  }

  /**
   * Always populates the bucket with the current count for that slot. If the count is positive, or if
   * <code>processEmpty==true</code>, then this method also populates the values from mapped "output" accumulators.
   *
   * @see SweepCoordinator#setSweepValues(SimpleOrderedMap, int)
   */
  @Override
  public void setValues(SimpleOrderedMap<Object> bucket, int slotNum) throws IOException {
    super.setValues(bucket, slotNum);
    if (sweepCoordinator != null && (0 < getCount(slotNum) || fcontext.processor.freq.processEmpty)) {
      sweepCoordinator.setSweepValues(bucket, slotNum);
    }
  }

  @Override
  public void setNextReader(LeafReaderContext ctx) throws IOException {
    leafCacheKey = ctx.reader().getReaderCacheHelper().getKey();
    cached = cachedSegments.get(leafCacheKey);
  }

  @Override
  public boolean incrementFromCachedSegment(LongValues toGlobal) {
    if (cached == null) {
      return false;
    } else {
      mergeCachedSegmentCounts(topLevelCounts, cached.counts, toGlobal);
      return true;
    }
  }

  @Override
  public void updateLeaf(int[] leafCounts) {
    if (cached != null) {
      // we don't need to update this leaf
      return;
    }
    dataOutput.reset();
    cachedSegments.put(leafCacheKey, new SegmentCacheEntry(TermFacetCache.encodeCounts(leafCounts, dataOutput)));
  }

  @Override
  public void updateTopLevel() {
    cachedSegments.put(topLevelCacheKey, new SegmentCacheEntry(topLevelCounts, includesMissingCount));
    facetCache.put(facetCacheKey, cachedSegments);
  }

  @Override
  public long getCount(int slot) {
    return topLevelCounts[slot];
  }

  @Override
  public int compare(int slotA, int slotB) {
    return Integer.compare(topLevelCounts[slotA], topLevelCounts[slotB]);
  }

  @Override
  public Object getValue(int slotNum) throws IOException {
    return (long) topLevelCounts[slotNum];
  }

  @Override
  public void incrementCount(int slot, long increment) {
    if (cached != null) {
      return;
    }
    topLevelCounts[slot] += increment;
  }

  @Override
  public void reset() throws IOException {
    throw new UnsupportedOperationException("reset not supported for cached count");
  }

  @Override
  public void resize(Resizer resizer) {
    throw new UnsupportedOperationException("resize not supported for cached count");
  }

  @Override
  public void collect(int doc, int slot, IntFunction<SlotContext> slotContext) throws IOException {
    throw new UnsupportedOperationException("collect not supported for cached count");
  }
}
