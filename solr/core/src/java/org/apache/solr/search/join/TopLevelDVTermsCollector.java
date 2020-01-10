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

package org.apache.solr.search.join;

import java.io.IOException;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.util.LongBitSet;
import org.apache.solr.search.DelegatingCollector;

/**
 * Collects all documents with a field value matching a set value in an ordinal bitset.
 *
 * Implementation is similar to {@link org.apache.lucene.search.join.TermsCollector}, but uses top-level ordinals
 * explicitly and has wider visibility.
 */
public class TopLevelDVTermsCollector extends DelegatingCollector {
  private LeafCollector leafCollector;
  private int docBase;
  private SortedSetDocValues topLevelDocValues;
  private LongBitSet topLevelDocValuesBitSet;
  private long firstOrd;
  private long lastOrd;

  public TopLevelDVTermsCollector(SortedSetDocValues topLevelDocValues, LongBitSet topLevelDocValuesBitSet, long firstOrd, long lastOrd) {
    this.topLevelDocValues = topLevelDocValues;
    this.topLevelDocValuesBitSet = topLevelDocValuesBitSet;
    this.firstOrd = firstOrd;
    this.lastOrd = lastOrd;
  }

  @Override
  public void setScorer(Scorable scorer) throws IOException {
    leafCollector.setScorer(scorer);
  }

  @Override
  public void doSetNextReader(LeafReaderContext context) throws IOException {
    this.leafCollector = delegate.getLeafCollector(context);
    this.docBase = context.docBase;
  }

  @Override
  public void collect(int doc) throws IOException {
    final int globalDoc = doc + docBase;

    if (topLevelDocValues.advanceExact(globalDoc)) {
      while (true) {
        final long ord = topLevelDocValues.nextOrd();
        if (ord == SortedSetDocValues.NO_MORE_ORDS) break;
        if (ord > lastOrd) break;
        if (ord < firstOrd) continue;
        if (topLevelDocValuesBitSet.get(ord)) {
          leafCollector.collect(doc);
          break;
        }
      }
    }
  }
}
