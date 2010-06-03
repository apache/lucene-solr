/**
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

package org.apache.solr.search;

import org.apache.lucene.search.*;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;


public class MissingStringLastComparatorSource extends FieldComparatorSource {
  public static final String bigString="\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffffNULL_VAL";

  private final String missingValueProxy;

  public MissingStringLastComparatorSource() {
    this(bigString);
  }

  /** Creates a {@link FieldComparatorSource} that uses <tt>missingValueProxy</tt> as the value to return from ScoreDocComparator.sortValue()
   * which is only used my multisearchers to determine how to collate results from their searchers.
   *
   * @param missingValueProxy   The value returned when sortValue() is called for a document missing the sort field.
   * This value is *not* normally used for sorting, but used to create
   */
  public MissingStringLastComparatorSource(String missingValueProxy) {
    this.missingValueProxy=missingValueProxy;
  }

  public FieldComparator newComparator(String fieldname, int numHits, int sortPos, boolean reversed) throws IOException {
    return new MissingLastOrdComparator(numHits, fieldname, sortPos, reversed, missingValueProxy);
  }

}


// Copied from Lucene and modified since the Lucene version couldn't
// be extended or have it's values accessed.
 class MissingLastOrdComparator extends FieldComparator {
    private static final int NULL_ORD = Integer.MAX_VALUE;
    private final BytesRef nullVal; 

    private final int[] ords;
    private final BytesRef[] values;
    private final int[] readerGen;

    private int currentReaderGen = -1;
    private FieldCache.DocTermsIndex termsIndex;
    private final String field;

    private int bottomSlot = -1;
    private int bottomOrd;
    private BytesRef bottomValue;
    private final boolean reversed;
    private final int sortPos;
    private final BytesRef tempBR = new BytesRef();

   public MissingLastOrdComparator(int numHits, String field, int sortPos, boolean reversed, String nullVal) {
      ords = new int[numHits];
      values = new BytesRef[numHits];
      readerGen = new int[numHits];
      this.sortPos = sortPos;
      this.reversed = reversed;
      this.field = field;
      this.nullVal = nullVal == null ? null : new BytesRef(nullVal);
    }

    @Override
    public int compare(int slot1, int slot2) {
      if (readerGen[slot1] == readerGen[slot2]) {
        int cmp = ords[slot1] - ords[slot2];
        if (cmp != 0) {
          return cmp;
        }
      }

      final BytesRef val1 = values[slot1];
      final BytesRef val2 = values[slot2];

      if (val1 == null) {
        if (val2 == null) {
          return 0;
        }
        return 1;
      } else if (val2 == null) {
        return -1;
      }
      return val1.compareTo(val2);
    }

    public int compareBottom(int doc) {
      assert bottomSlot != -1;
      int order = termsIndex.getOrd(doc);
      int ord = (order == 0) ? NULL_ORD : order;
      final int cmp = bottomOrd - ord;
      if (cmp != 0) {
        return cmp;
      }

      // take care of the case where both vals are null
      if (bottomOrd == NULL_ORD) return 0;
      return bottomValue.compareTo(termsIndex.lookup(order, tempBR));
    }

    private void convert(int slot) {
      readerGen[slot] = currentReaderGen;
      int index = 0;
      BytesRef value = values[slot];
      if (value == null) {
        // should already be done
        assert ords[slot] == NULL_ORD;
        return;
      }

      if (sortPos == 0 && bottomSlot != -1 && bottomSlot != slot) {
        // Since we are the primary sort, the entries in the
        // queue are bounded by bottomOrd:
        assert bottomOrd < termsIndex.numOrd();
        if (reversed) {
          index = binarySearch(tempBR, termsIndex, value, bottomOrd, termsIndex.numOrd()-1);
        } else {
          index = binarySearch(tempBR, termsIndex, value, 0, bottomOrd);
        }
      } else {
        // Full binary search
        index = binarySearch(tempBR, termsIndex, value);
      }

      if (index < 0) {
        index = -index - 2;
      }
      ords[slot] = index;
    }

    @Override
    public void copy(int slot, int doc) {
      final int ord = termsIndex.getOrd(doc);
      assert ord >= 0;
      if (ord == 0) {
        ords[slot] = NULL_ORD;
        values[slot] = null;
      } else {
        ords[slot] = ord;
        if (values[slot] == null) {
          values[slot] = new BytesRef();
        }
        termsIndex.lookup(ord, values[slot]);
      }
      readerGen[slot] = currentReaderGen;
    }

    @Override
    public void setNextReader(IndexReader reader, int docBase) throws IOException {
      termsIndex = FieldCache.DEFAULT.getTermsIndex(reader, field);
      currentReaderGen++;
      assert termsIndex.numOrd() > 0;
      if (bottomSlot != -1) {
        convert(bottomSlot);
        bottomOrd = ords[bottomSlot];
      }
    }

    @Override
    public void setBottom(final int bottom) {
      bottomSlot = bottom;
      if (readerGen[bottom] != currentReaderGen) {
        convert(bottomSlot);
      }
      bottomOrd = ords[bottom];
      assert bottomOrd >= 0;
      // assert bottomOrd < lookup.length;
      bottomValue = values[bottom];
    }

    @Override
    public Comparable value(int slot) {
      Comparable v = values[slot];
      return v==null ? nullVal : v;
    }

    public BytesRef[] getValues() {
      return values;
    }

    public int getBottomSlot() {
      return bottomSlot;
    }

    public String getField() {
      return field;
    }
  }
