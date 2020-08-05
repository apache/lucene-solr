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
import java.util.List;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.LongValues;
import org.apache.solr.search.facet.FacetFieldProcessor.FilterCtStruct;
import org.apache.solr.search.facet.SlotAcc.CountSlotAcc;
import org.apache.solr.search.facet.SlotAcc.SweepCountAccStruct;
import org.apache.solr.request.TermFacetCache.CacheUpdater;

abstract class SweepDISI extends DocIdSetIterator implements SweepCountAware {

  public final int size;
  final CountSlotAcc[] countAccs;
  final CacheUpdater[] cacheUpdaters;

  public SweepDISI(int size, CountSlotAcc[] countAccs, CacheUpdater[] cacheUpdaters) {
    this.size = size;
    this.countAccs = countAccs;
    this.cacheUpdaters = cacheUpdaters;
  }

  private static boolean addAcc(SweepCountAccStruct entry, DocIdSetIterator[] subIterators, CountSlotAcc[] activeCountAccs, CacheUpdater[] cacheUpdaters, LeafReaderContext subCtx, int idx) throws IOException {
    final DocIdSet docIdSet = entry.docSet.getTopFilter().getDocIdSet(subCtx, null);
    if (docIdSet == null || (subIterators[idx] = docIdSet.iterator()) == null) {
      return false;
    }
    activeCountAccs[idx] = entry.countAcc;
    cacheUpdaters[idx] = entry.cacheUpdater;
    return true;
  }

  static Object blah(DocIdSetIterator[] subIterators, CountSlotAcc[] activeCountAccs, CacheUpdater[] cacheUpdaters, int subIdx, LeafReaderContext subCtx) {
    final SweepDISI disi;
    int activeCt = 0;
    final boolean hasBase;
    boolean hasCacheUpdater = false;
    LongValues toGlobal = ordinalMap == null ? null : ordinalMap.getGlobalOrds(subIdx);
    for (int i = 0; ; ) {
      FilterCtStruct filterEntry = filters[i];
      final CacheUpdater cacheUpdater = filterEntry.cacheUpdater;
      if (cacheUpdater != null && cacheUpdater.incrementFromCachedSegment(toGlobal) && (maySkipBaseSetCollection || !filterEntry.isBase)) {
        if (++i == filters.length) {
          hasBase = false;
          break;
        }
      } else {
        DocIdSet docIdSet = filterEntry.filter.getDocIdSet(subCtx, null);
        subIterators[activeCt] = docIdSet.iterator();
        activeCountAccs[activeCt] = filterEntry.countAcc;
        hasCacheUpdater |= (cacheUpdaters[activeCt++] = cacheUpdater) != null;
        if (++i == filters.length) {
          hasBase = filterEntry.isBase;
          break;
        }
      }
    }
    updateTopLevelCache |= hasCacheUpdater;
    switch (activeCt) {
      case 0:
        continue;
      case 1:
        disi = new SingletonDISI(subIterators[0], activeCountAccs, hasCacheUpdater ? cacheUpdaters : null, hasBase); // solr docsets already exclude any deleted docs
        break;
      default:
        disi = new UnionDISI(subIterators, activeCountAccs, hasCacheUpdater ? cacheUpdaters : null, activeCt, hasBase);
        break;
    }
  }
  static SweepDISI newInstance(SweepCountAccStruct base, List<SweepCountAccStruct> others, DocIdSetIterator[] subIterators, CountSlotAcc[] activeCountAccs, CacheUpdater[] cacheUpdaters, LeafReaderContext subCtx) throws IOException {
    int activeCt = 0;
    final int baseIdx;
    if (base == null || !addAcc(base, subIterators, activeCountAccs, cacheUpdaters, subCtx, activeCt)) {
      baseIdx = -1;
    } else {
      baseIdx = activeCt++;
    }
    for (SweepCountAccStruct entry : others) {
      if (addAcc(entry, subIterators, activeCountAccs, cacheUpdaters, subCtx, activeCt)) {
        activeCt++;
      }
    }
    switch (activeCt) {
      case 0:
        return null;
      case 1:
        return new SingletonDISI(subIterators[0], activeCountAccs, cacheUpdaters, baseIdx >= 0); // solr docsets already exclude any deleted docs
      default:
        return new UnionDISI(subIterators, activeCountAccs, cacheUpdaters, activeCt, baseIdx);
    }
  }

  public abstract boolean hasBase();

  public abstract boolean hasCacheUpdater();

  @Override
  public int docID() {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public int advance(int target) throws IOException {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public long cost() {
    throw new UnsupportedOperationException("Not supported.");
  }

}
