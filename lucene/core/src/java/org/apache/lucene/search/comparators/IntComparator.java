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

import org.apache.lucene.document.IntPoint;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.LeafFieldComparator;

import java.io.IOException;

/**
 * Comparator based on {@link Integer#compare} for {@code numHits}.
 * This comparator provides a skipping functionality – an iterator that can skip over non-competitive documents.
 */
public class IntComparator extends NumericComparator<Integer> {
  private final int[] values;
  protected int topValue;
  protected int bottom;

  public IntComparator(int numHits, String field, Integer missingValue, boolean reverse, int sortPos) {
    super(field, missingValue != null ? missingValue : 0, reverse, sortPos, Integer.BYTES);
    values = new int[numHits];
  }

  @Override
  public int compare(int slot1, int slot2) {
    return Integer.compare(values[slot1], values[slot2]);
  }

  @Override
  public void setTopValue(Integer value) {
    super.setTopValue(value);
    topValue = value;
  }

  @Override
  public Integer value(int slot) {
    return Integer.valueOf(values[slot]);
  }

  @Override
  public LeafFieldComparator getLeafComparator(LeafReaderContext context) throws IOException {
    return new IntLeafComparator(context);
  }

  /**
   * Leaf comparator for {@link IntComparator} that provides skipping functionality
   */
  public class IntLeafComparator extends NumericLeafComparator {

    public IntLeafComparator(LeafReaderContext context) throws IOException {
      super(context);
    }

    private int getValueForDoc(int doc) throws IOException {
      if (docValues.advanceExact(doc)) {
        return (int) docValues.longValue();
      } else {
        return missingValue;
      }
    }

    @Override
    public void setBottom(int slot) throws IOException {
      bottom = values[slot];
      super.setBottom(slot);
    }

    @Override
    public int compareBottom(int doc) throws IOException {
      return Integer.compare(bottom, getValueForDoc(doc));
    }

    @Override
    public int compareTop(int doc) throws IOException {
      return Integer.compare(topValue, getValueForDoc(doc));
    }

    @Override
    public void copy(int slot, int doc) throws IOException {
      values[slot] = getValueForDoc(doc);
      super.copy(slot, doc);
    }

    @Override
    protected boolean isMissingValueCompetitive() {
      int result = Integer.compare(missingValue, bottom);
      // in reverse (desc) sort missingValue is competitive when it's greater or equal to bottom,
      // in asc sort missingValue is competitive when it's smaller or equal to bottom
      return reverse ? (result >= 0) : (result <= 0);
    }

    @Override
    protected void encodeBottom(byte[] packedValue) {
      IntPoint.encodeDimension(bottom, packedValue, 0);
    }

    @Override
    protected void encodeTop(byte[] packedValue) {
      IntPoint.encodeDimension(topValue, packedValue, 0);
    }
  }

}
