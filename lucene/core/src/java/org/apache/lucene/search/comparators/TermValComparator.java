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

package org.apache.lucene.search.comparators;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;

import java.io.IOException;

/**
 * Comparator that sorts by field's natural Term sort order.
 * All comparisons are done using BytesRef.compareTo,
 * which is slow for medium to large result sets,
 * but possibly for very small results sets.
 */
public class TermValComparator extends FieldComparator<BytesRef> {
  private final String field;
  private final BytesRef[] values;
  private final BytesRefBuilder[] tempBRs;
  private final int missingSortCmp;

  private BytesRef topValue;
  private BytesRef bottomValue;

  public TermValComparator(int numHits, String field, boolean sortMissingLast) {
    this.field = field;
    this.values = new BytesRef[numHits];
    this.tempBRs = new BytesRefBuilder[numHits];
    this.missingSortCmp = sortMissingLast ? 1 : -1;
  }

  @Override
  public int compare(int slot1, int slot2) {
    final BytesRef val1 = values[slot1];
    final BytesRef val2 = values[slot2];
    return compareValues(val1, val2);
  }

  @Override
  public void setTopValue(BytesRef value) {
    // null is fine: it means the last doc of the prior
    // search was missing this value
    topValue = value;
  }

  @Override
  public BytesRef value(int slot) {
    return values[slot];
  }

  @Override
  public LeafFieldComparator getLeafComparator(LeafReaderContext context) throws IOException {
    return new TermValLeafComparator(context);
  }

  @Override
  public int compareValues(BytesRef val1, BytesRef val2) {
    // missing always sorts first:
    if (val1 == null) {
      if (val2 == null) {
        return 0;
      }
      return missingSortCmp;
    } else if (val2 == null) {
      return -missingSortCmp;
    }
    return val1.compareTo(val2);
  }

  /**
   * Leaf comparator for {@link TermValComparator}
   */
  public class TermValLeafComparator implements LeafFieldComparator {
    private final BinaryDocValues docTerms;

    public TermValLeafComparator(LeafReaderContext context) throws IOException {
      docTerms = DocValues.getBinary(context.reader(), field);
    }

    @Override
    public void setBottom(int slot) {
      bottomValue = values[slot];
    }

    @Override
    public int compareBottom(int doc) throws IOException {
      return compareValues(bottomValue, getValueForDoc(doc));
    }

    @Override
    public int compareTop(int doc) throws IOException {
      return compareValues(topValue, getValueForDoc(doc));
    }

    @Override
    public void copy(int slot, int doc) throws IOException {
      final BytesRef comparableBytes = getValueForDoc(doc);
      if (comparableBytes == null) {
        values[slot] = null;
      } else {
        if (tempBRs[slot] == null) {
          tempBRs[slot] = new BytesRefBuilder();
        }
        tempBRs[slot].copyBytes(comparableBytes);
        values[slot] = tempBRs[slot].get();
      }
    }

    @Override
    public void setScorer(Scorable scorer) {}

    private BytesRef getValueForDoc(int doc) throws IOException {
      if (docTerms.advanceExact(doc)) {
        return docTerms.binaryValue();
      } else {
        return null;
      }
    }
  }
}
