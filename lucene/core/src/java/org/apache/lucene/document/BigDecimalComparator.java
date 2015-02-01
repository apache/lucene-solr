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

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;

// TODO: this doesn't use the ord; we could index BinaryDV instead for the single valued case?
class BigDecimalComparator extends FieldComparator<BigDecimal> {
  private final String field;
  private final BytesRef[] values;
  private final int byteWidth;
  private final BytesRef missingValue;
  private final int scale;
  private BytesRef bottom;
  private BytesRef topValue;
  private Bits docsWithField;
  private SortedDocValues currentReaderValues;

  public BigDecimalComparator(int numHits, String field, int byteWidth, boolean missingLast, int scale) {
    this.scale = scale;
    this.field = field;
    this.byteWidth = byteWidth;
    values = new BytesRef[numHits];
    missingValue = new BytesRef(byteWidth);
    missingValue.length = byteWidth;
    if (missingLast) {
      Arrays.fill(missingValue.bytes, (byte) 0xff);
    }
    for(int i=0;i<numHits;i++) {
      values[i] = new BytesRef(byteWidth);
      values[i].length = byteWidth;
    }
  }

  @Override
  public int compare(int slot1, int slot2) {
    return values[slot1].compareTo(values[slot2]);
  }

  private BytesRef getDocValue(int doc) {
    BytesRef v;
    if (docsWithField != null && docsWithField.get(doc) == false) {
      v = missingValue;
    } else {
      v = currentReaderValues.get(doc);
    }
    assert v.length == byteWidth: v.length + " vs " +  byteWidth;
    return v;
  }

  @Override
  public int compareBottom(int doc) {
    return bottom.compareTo(getDocValue(doc));
  }

  @Override
  public void copy(int slot, int doc) {
    BytesRef v = getDocValue(doc);
    System.arraycopy(v.bytes, v.offset, values[slot].bytes, 0, byteWidth);
  }
    
  @Override
  public void setBottom(final int bottom) {
    this.bottom = values[bottom];
  }

  @Override
  public void setTopValue(BigDecimal value) {
    topValue = NumericUtils.bigIntToBytes(value.unscaledValue(), byteWidth);
  }

  @Override
  public BigDecimal value(int slot) {
    return new BigDecimal(NumericUtils.bytesToBigInt(values[slot]), scale);
  }

  @Override
  public int compareTop(int doc) {
    BytesRef v = getDocValue(doc);
    return topValue.compareTo(v);
  }

  @Override
  public FieldComparator<BigDecimal> setNextReader(LeafReaderContext context) throws IOException {
    currentReaderValues = getDocValues(context);
    assert currentReaderValues != null;
    docsWithField = DocValues.getDocsWithField(context.reader(), field);
    assert docsWithField != null;
    // optimization to remove unneeded checks on the bit interface:
    if (docsWithField instanceof Bits.MatchAllBits) {
      docsWithField = null;
    }
    return this;
  }

  protected SortedDocValues getDocValues(LeafReaderContext context) throws IOException {
    return context.reader().getSortedDocValues(field);
  }
}
