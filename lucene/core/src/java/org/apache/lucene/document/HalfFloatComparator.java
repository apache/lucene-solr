package org.apache.lucene.document;

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

import org.apache.lucene.search.FieldComparator;

// nocommit can't we do all numeric comparators this way?  if we fix all numeric dv to write sortable versions?
class HalfFloatComparator extends FieldComparator.NumericComparator<Float> {
  private final short[] values;
  private final short missingShortValue;
  private short bottom;
  private short topValue;

  /** 
   * Creates a new comparator based on {@link Float#compare} for {@code numHits}.
   * When a document has no value for the field, {@code missingValue} is substituted. 
   */
  public HalfFloatComparator(int numHits, String field, Float missingValue) {
    super(field, missingValue);
    values = new short[numHits];
    missingShortValue = (short) Document.sortableHalfFloatBits(HalfFloat.floatToIntBits(missingValue));
  }
    
  @Override
  public int compare(int slot1, int slot2) {
    return (int) values[slot1] - (int) values[slot2];
  }

  @Override
  public int compareBottom(int doc) {
    // TODO: are there sneaky non-branch ways to compute sign of float?
    short v = (short) currentReaderValues.get(doc);
    // Test for v == 0 to save Bits.get method call for
    // the common case (doc has value and value is non-zero):
    if (docsWithField != null && v == 0 && !docsWithField.get(doc)) {
      v = missingShortValue;
    }

    return (int) bottom - (int) v;
  }

  @Override
  public void copy(int slot, int doc) {
    short v =  (short) currentReaderValues.get(doc);
    // Test for v == 0 to save Bits.get method call for
    // the common case (doc has value and value is non-zero):
    if (docsWithField != null && v == 0 && !docsWithField.get(doc)) {
      v = missingShortValue;
    }

    values[slot] = v;
  }
    
  @Override
  public void setBottom(final int bottom) {
    this.bottom = values[bottom];
  }

  @Override
  public void setTopValue(Float value) {
    topValue = (short) Document.sortableHalfFloatBits(HalfFloat.floatToIntBits(value));
  }

  @Override
  public Float value(int slot) {
    return Document.sortableShortToFloat(values[slot]);
  }

  @Override
  public int compareTop(int doc) {
    short docValue = (short) currentReaderValues.get(doc);
    // Test for docValue == 0 to save Bits.get method call for
    // the common case (doc has value and value is non-zero):
    if (docsWithField != null && docValue == 0 && !docsWithField.get(doc)) {
      docValue = missingShortValue;
    }
    return (int) topValue - (int) docValue;
  }
}
