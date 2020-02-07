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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BitSet;

import static org.apache.solr.search.join.BlockJoinParentQParser.getCachedFilter;

public class UniqueBlockQueryAgg extends UniqueBlockAgg {

  private static final class UniqueBlockQuerySlotAcc extends UniqueBlockSlotAcc {

    private Query query;
    private BitSet parentBitSet;

    private UniqueBlockQuerySlotAcc(FacetContext fcontext, Query query, int numSlots)
        throws IOException { //
      super(fcontext, null, numSlots);
      this.query = query;
    }

    @Override
    public void setNextReader(LeafReaderContext readerContext) throws IOException {
      this.parentBitSet = getCachedFilter(fcontext.req, query).getFilter().getBitSet(readerContext);
    }

    @Override
    public void collect(int doc, int slotNum, IntFunction<SlotContext> slotContext) {
      if (parentBitSet != null) {
        int ord = parentBitSet.nextSetBit(doc);
        if (ord != DocIdSetIterator.NO_MORE_DOCS) {
          collectOrdToSlot(slotNum, ord);
        } 
      }
    }
  }

  final private Query query;

  public UniqueBlockQueryAgg(Query query) {
    super(null);
    this.query = query;
    arg = query.toString();
  }

  @Override
  public SlotAcc createSlotAcc(FacetContext fcontext, int numDocs, int numSlots) throws IOException {
    return new UniqueBlockQuerySlotAcc(fcontext, query, numSlots);
  }
}
