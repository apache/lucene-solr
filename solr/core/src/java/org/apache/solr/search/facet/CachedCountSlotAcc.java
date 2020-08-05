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
import java.util.function.IntFunction;
import org.apache.lucene.util.LongValues;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.request.TermFacetCache.CacheUpdater;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.QueryResultKey;
import org.apache.solr.search.facet.SlotAcc.CountSlotAcc;
import org.apache.solr.search.facet.SlotAcc.SweepCoordinationPoint;
import org.apache.solr.search.facet.SlotAcc.SweepCoordinator;

//nocommit: is this implementing CacheUpdater to signal something? It's not actually updating a cache.
final class CachedCountSlotAcc extends CountSlotAcc implements CacheUpdater, SweepCoordinationPoint {

  //nocommit: probably should make this a long[]?
  private final int[] topLevelCounts;
  private final SweepCoordinator sweepCoordinator;

  static SweepCountAccStruct create(QueryResultKey qKey, DocSet docs, boolean isBase, FacetFieldProcessor p, int[] topLevelCounts) {
    CachedCountSlotAcc count = new CachedCountSlotAcc(qKey, docs, isBase, p, topLevelCounts);
    return isBase ? count.sweepCoordinator.base : new SweepCountAccStruct(docs, isBase, count, qKey, CacheState.CACHED, null, count);
  }

  private CachedCountSlotAcc(QueryResultKey qKey, DocSet docs, boolean isBase, FacetFieldProcessor p, int[] topLevelCounts) {
    super(p.fcontext);
    this.topLevelCounts = topLevelCounts;
    if (!isBase) {
      this.sweepCoordinator = null;
    } else {
      SweepCountAccStruct struct = new SweepCountAccStruct(docs, isBase, this, qKey, CacheState.CACHED, null, this);
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
  public boolean incrementFromCachedSegment(LongValues toGlobal) {
    throw new RuntimeException("TODO?");//return true;
  }

  @Override
  public void updateLeaf(int[] leafCounts) {
    throw new RuntimeException("TODO?");//NoOp
  }

  @Override
  public void updateTopLevel() {
    throw new RuntimeException("TODO?");//NoOp
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
    return topLevelCounts[slotNum];
  }

  @Override
  public void incrementCount(int slot, long increment) {
    //NoOp
  }

  @Override
  public void collect(int doc, int slot, IntFunction<SlotContext> slotContext) throws IOException {
    //NoOp
  }

  @Override
  public void reset() throws IOException {
    //NoOp
  }

  @Override
  public void resize(Resizer resizer) {
    //NoOp
  }
}
