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

import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.LeafFieldComparator;

import java.io.IOException;

/**
 * Comparator based on {@link Float#compare} for {@code numHits}.
 * This comparator provides a skipping functionality – an iterator that can skip over non-competitive documents.
 */
public class FloatComparator extends NumericComparator<Float> {
  private final float[] values;
  protected float topValue;
  protected float bottom;

  public FloatComparator(int numHits, String field, Float missingValue, boolean reverse, int sortPos) {
    super(field, missingValue != null ? missingValue : 0.0f, reverse, sortPos, Float.BYTES);
    values = new float[numHits];
  }

  @Override
  public int compare(int slot1, int slot2) {
    return Float.compare(values[slot1], values[slot2]);
  }

  @Override
  public void setTopValue(Float value) {
    super.setTopValue(value);
    topValue = value;
  }

  @Override
  public Float value(int slot) {
    return Float.valueOf(values[slot]);
  }

  @Override
  public LeafFieldComparator getLeafComparator(LeafReaderContext context) throws IOException {
    return new FloatLeafComparator(context);
  }

  /**
   * Leaf comparator for {@link FloatComparator} that provides skipping functionality
   */
  public class FloatLeafComparator extends NumericLeafComparator {

    public FloatLeafComparator(LeafReaderContext context) throws IOException {
      super(context);
    }

    private float getValueForDoc(int doc) throws IOException {
      if (docValues.advanceExact(doc)) {
        return Float.intBitsToFloat((int) docValues.longValue());
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
      return Float.compare(bottom, getValueForDoc(doc));
    }

    @Override
    public int compareTop(int doc) throws IOException {
      return Float.compare(topValue, getValueForDoc(doc));
    }

    @Override
    public void copy(int slot, int doc) throws IOException {
      values[slot] = getValueForDoc(doc);
      super.copy(slot, doc);
    }

    @Override
    protected boolean isMissingValueCompetitive() {
      int result = Float.compare(missingValue, bottom);
      return reverse ? (result >= 0) : (result <= 0);
    }

    @Override
    protected void encodeBottom(byte[] packedValue) {
      FloatPoint.encodeDimension(bottom, packedValue, 0);
    }

    @Override
    protected void encodeTop(byte[] packedValue) {
      FloatPoint.encodeDimension(topValue, packedValue, 0);
    }
  }

}
