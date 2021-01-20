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
import java.util.Iterator;
import java.util.List;

import org.apache.solr.search.DocIterator;
import org.apache.solr.search.facet.SlotAcc.CountSlotAcc;
import org.apache.solr.search.facet.SlotAcc.SweepCountAccStruct;

abstract class SweepDocIterator implements DocIterator, SweepCountAware {

  public final int size;

  public SweepDocIterator(int size) {
    this.size = size;
  }

  static class SweepIteratorAndCounts {
    final SweepDocIterator iter;
    final CountSlotAcc[] countAccs;
    public SweepIteratorAndCounts(SweepDocIterator iter, CountSlotAcc[] countAccs) {
      this.iter = iter;
      this.countAccs = countAccs;
    }
  }

  static SweepIteratorAndCounts newInstance(SweepCountAccStruct base, List<SweepCountAccStruct> others) throws IOException {
    final int activeCt;
    SweepCountAccStruct entry;
    if (base == null) {
      activeCt = others.size();
      entry = others.get(0);
    } else {
      activeCt = others.size() + 1;
      entry = base;
    }
    if (activeCt == 1) {
      final CountSlotAcc[] countAccs = new CountSlotAcc[] {entry.countAcc};
      return new SweepIteratorAndCounts(new SingletonDocIterator(entry.docSet.iterator(), base != null), countAccs);
    } else {
      final DocIterator[] subIterators = new DocIterator[activeCt];
      final CountSlotAcc[] countAccs = new CountSlotAcc[activeCt];
      Iterator<SweepCountAccStruct> othersIter = others.iterator();
      int i = 0;
      for (;;) {
        subIterators[i] = entry.docSet.iterator();
        countAccs[i] = entry.countAcc;
        if (++i == activeCt) {
          break;
        }
        entry = othersIter.next();
      }
      return new SweepIteratorAndCounts(new UnionDocIterator(subIterators, base == null ? -1 : 0), countAccs);
    }
  }

  @Override
  public float score() {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public Integer next() {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public abstract int registerCounts(SegCounter segCounts); // override to not throw IOException

}
