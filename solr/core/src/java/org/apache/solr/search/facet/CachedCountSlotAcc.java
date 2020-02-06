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
import org.apache.solr.request.TermFacetCache.CacheUpdater;

final class CachedCountSlotAcc extends CountSlotAcc implements CacheUpdater {

  private final int[] topLevelCounts;

  CachedCountSlotAcc(FacetContext fcontext, int[] topLevelCounts) {
    super(fcontext);
    this.topLevelCounts = topLevelCounts;
  }

  @Override
  public boolean incrementFromCachedSegment(LongValues toGlobal) {
    return true;
  }

  @Override
  public void updateLeaf(int[] leafCounts) {
    //NoOp
  }

  @Override
  public void updateTopLevel() {
    //NoOp
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
