package org.apache.lucene.search;

import java.io.IOException;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.util.Bits;

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

/**
 * Base FieldComparator class for numeric types
 */
public abstract class NumericComparator<T extends Number> extends FieldComparator<T> {
  private final long[] values;
  private final long missingValue;
  private long bottom;
  private long topValue;
  protected final String field;
  protected Bits docsWithField;
  protected NumericDocValues currentReaderValues;
    
  public NumericComparator(int numHits, String field, long missingValue) {
    this.field = field;
    this.values = new long[numHits];
    this.missingValue = missingValue;
  }

  @Override
  public int compare(int slot1, int slot2) {
    return Long.compare(values[slot1], values[slot2]);
  }

  private long getDocValue(int doc) {
    long v = currentReaderValues.get(doc);
    // Test for v == 0 to save Bits.get method call for
    // the common case (doc has value and value is non-zero):
    if (docsWithField != null && v == 0 && !docsWithField.get(doc)) {
      v = missingValue;
    }
    return v;
  }

  @Override
  public int compareBottom(int doc) {
    return Long.compare(bottom, getDocValue(doc));
  }

  @Override
  public void copy(int slot, int doc) {
    values[slot] = getDocValue(doc);
  }
    
  @Override
  public void setBottom(final int bottom) {
    this.bottom = values[bottom];
  }

  @Override
  public void setTopValue(T value) {
    topValue = valueToLong(value);
  }

  @Override
  public FieldComparator<T> setNextReader(LeafReaderContext context) throws IOException {
    currentReaderValues = getNumericDocValues(context, field);
    docsWithField = DocValues.getDocsWithField(context.reader(), field);
    // optimization to remove unneeded checks on the bit interface:
    if (docsWithField instanceof Bits.MatchAllBits) {
      docsWithField = null;
    }
    return this;
  }
    
  @Override
  public int compareTop(int doc) {
    return Long.compare(topValue, getDocValue(doc));
  }

  protected abstract T longToValue(long value);

  protected abstract long valueToLong(T value);

  @Override
  public T value(int slot) {
    return longToValue(values[slot]);
  }

  /** Retrieves the NumericDocValues for the field in this segment */
  protected NumericDocValues getNumericDocValues(LeafReaderContext context, String field) throws IOException {
    return DocValues.getNumeric(context.reader(), field);
  }
}

