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
import java.util.Arrays;
import java.util.Objects;
import java.util.function.IntFunction;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BitSet;

import static org.apache.solr.search.join.BlockJoinParentQParser.getCachedFilter;

public class UniqueBlockQueryAgg extends AggValueSource {

  protected static final class UniqueBlockQuerySlotAcc extends UniqueSinglevaluedSlotAcc {

    protected int[] lastSeenValuesPerSlot;
    private Query query;
    private BitSet parentBitSet;

    protected UniqueBlockQuerySlotAcc(FacetContext fcontext, int numSlots, Query query)
        throws IOException { //
      super(fcontext, null, /*numSlots suppressing inherited accumulator */ 0, null);
      this.query = query;
      counts = new int[numSlots];
      lastSeenValuesPerSlot = new int[numSlots];
      Arrays.fill(lastSeenValuesPerSlot, Integer.MIN_VALUE);
    }

    @Override
    public void setNextReader(LeafReaderContext readerContext) throws IOException {
      this.parentBitSet = getCachedFilter(fcontext.req, query).getFilter().getBitSet(readerContext);
    }

    @Override
    public void collect(int doc, int slotNum, IntFunction<SlotContext> slotContext) {
      int ord = parentBitSet.nextSetBit(doc);
      collectOrdToSlot(slotNum, ord);
    }

    @Override
    protected void collectOrdToSlot(int slotNum, int ord) {
      if (lastSeenValuesPerSlot[slotNum] != ord) {
        counts[slotNum] += 1;
        lastSeenValuesPerSlot[slotNum] = ord;
      }
    }

    @Override
    public void reset() {
      Arrays.fill(counts, 0);
      Arrays.fill(lastSeenValuesPerSlot, Integer.MIN_VALUE);
    }

    @Override
    public void resize(Resizer resizer) {
      lastSeenValuesPerSlot = resizer.resize(lastSeenValuesPerSlot, Integer.MIN_VALUE);
      super.resize(resizer);
    }

    @Override
    public Object getValue(int slot) {
      return counts[slot];
    }
  }

  private final static String NAME = "uniqueBlockQuery";

  final protected Query query;

  public UniqueBlockQueryAgg(Query query) {
    super(NAME);
    this.query = query;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), query);
  }

  @Override
  public String description() {
    return name + "(query" + query + ")";
  }

  @Override
  public SlotAcc createSlotAcc(FacetContext fcontext, int numDocs, int numSlots) throws IOException {
    return new UniqueBlockQuerySlotAcc(fcontext, numSlots, query);
  }

  @Override
  public FacetMerger createFacetMerger(Object prototype) {
    return new FacetLongMerger();
  }
}
