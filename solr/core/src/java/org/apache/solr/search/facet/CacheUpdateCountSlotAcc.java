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
import org.apache.solr.request.TermFacetCache;
import org.apache.solr.request.TermFacetCache.CacheUpdater;
import org.apache.solr.request.TermFacetCache.FacetCacheKey;
import org.apache.solr.request.TermFacetCache.SegmentCacheEntry;
import org.apache.solr.search.SolrCache;

final class CacheUpdateCountSlotAcc extends CountSlotAcc implements CacheUpdater {

  private final int[] topLevelCounts;
  private final Map<CacheKey, SegmentCacheEntry> cachedSegments;
  private final ByteBuffersDataOutput dataOutput;
  private final CacheKey topLevelCacheKey;
  private final SolrCache<FacetCacheKey, Map<CacheKey, SegmentCacheEntry>> facetCache;
  private final FacetCacheKey facetCacheKey;
  private final boolean includesMissingCount;
  private CacheKey leafCacheKey;
  private SegmentCacheEntry cached;

  CacheUpdateCountSlotAcc(FacetContext fcontext, int numSlots, Map<CacheKey, SegmentCacheEntry> cachedSegments,
      CacheKey topLevelCacheKey, SolrCache<FacetCacheKey, Map<CacheKey, SegmentCacheEntry>> facetCache,
      FacetCacheKey facetCacheKey, boolean includesMissingCount) {
    super(fcontext);
    this.topLevelCounts = new int[numSlots];
    this.cachedSegments = cachedSegments;
    final int initialBackingByteArraySize = numSlots; // probably ok as a rough initial size estimate
    this.dataOutput = new ByteBuffersDataOutput(initialBackingByteArraySize);
    this.topLevelCacheKey = topLevelCacheKey;
    this.facetCache = facetCache;
    this.facetCacheKey = facetCacheKey;
    this.includesMissingCount = includesMissingCount;
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
  public int getCount(int slot) {
    return topLevelCounts[slot];
  }

  @Override
  public int compare(int slotA, int slotB) {
    return Integer.compare(topLevelCounts[slotA], topLevelCounts[slotB]);
  }

  @Override
  public Object getValue(int slotNum) throws IOException {
    return topLevelCounts[slotNum];
  }

  @Override
  public void incrementCount(int slot, int increment) {
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
