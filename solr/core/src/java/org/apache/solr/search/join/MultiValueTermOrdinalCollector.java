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
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.util.LongBitSet;

/**
 * Populates a bitset of (top-level) ordinals based on field values in a multi-valued field.
 */
public class MultiValueTermOrdinalCollector extends SimpleCollector {

  private int docBase;
  private SortedSetDocValues topLevelDocValues;
  private final String fieldName;
  // Records all ordinals found during collection
  private final LongBitSet topLevelDocValuesBitSet;

  public MultiValueTermOrdinalCollector(String fieldName, SortedSetDocValues topLevelDocValues, LongBitSet topLevelDocValuesBitSet) {
    this.fieldName = fieldName;
    this.topLevelDocValues = topLevelDocValues;
    this.topLevelDocValuesBitSet = topLevelDocValuesBitSet;
  }

  public ScoreMode scoreMode() {
    return ScoreMode.COMPLETE_NO_SCORES;
  }

  @Override
  public void doSetNextReader(LeafReaderContext context) throws IOException {
    this.docBase = context.docBase;
  }

  @Override
  public void collect(int doc) throws IOException {
    final int globalDoc = docBase + doc;

    if (topLevelDocValues.advanceExact(globalDoc)) {
      long ord = SortedSetDocValues.NO_MORE_ORDS;
      while ((ord = topLevelDocValues.nextOrd()) != SortedSetDocValues.NO_MORE_ORDS) {
        topLevelDocValuesBitSet.set(ord);
      }
    }
  }
}
