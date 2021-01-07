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

package org.apache.lucene.misc.search;

import java.io.IOException;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.misc.CollectorMemoryTracker;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.util.FixedBitSet;

/** Bitset collector which supports memory tracking */
public class MemoryAccountingBitsetCollector extends SimpleCollector {

  final CollectorMemoryTracker tracker;
  FixedBitSet bitSet = new FixedBitSet(0);
  int length = 0;
  int docBase = 0;

  public MemoryAccountingBitsetCollector(CollectorMemoryTracker tracker) {
    this.tracker = tracker;
    tracker.updateBytes(bitSet.ramBytesUsed());
  }

  @Override
  protected void doSetNextReader(LeafReaderContext context) throws IOException {
    length += context.reader().maxDoc();
    FixedBitSet newBitSet = FixedBitSet.ensureCapacity(bitSet, length);
    if (newBitSet != bitSet) {
      tracker.updateBytes(newBitSet.ramBytesUsed() - bitSet.ramBytesUsed());
      bitSet = newBitSet;
    }
  }

  @Override
  public void collect(int doc) {
    bitSet.set(docBase + doc);
  }

  @Override
  public ScoreMode scoreMode() {
    return ScoreMode.COMPLETE_NO_SCORES;
  }
}
