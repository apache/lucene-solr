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
package org.apache.solr.schema;


import java.io.IOException;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.FilterNumericDocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.queries.function.valuesource.SortedSetFieldSource;
import org.apache.solr.schema.NumericSortedSetSortField;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedSetSelector;
import org.apache.lucene.search.SortedSetSelector.Type;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.apache.solr.legacy.LegacyNumericUtils;

public class NumericSortedSetFieldSource extends SortedSetFieldSource {

  private final NumberType numberType;
  private final SchemaField sf;

  public NumericSortedSetFieldSource(SchemaField sf, Type selector, NumberType numberType) {
    super(sf.name, selector);
    this.numberType = validateType(numberType);
    this.sf = sf;
  }

  public NumericSortedSetFieldSource(SchemaField sf, NumberType numericType) {
    super(sf.name);
    this.numberType = validateType(numericType);
    this.sf = sf;
  }

  private static NumberType validateType(NumberType type) {
    switch (type) {
      // whitelisted types
      case LONG:
      case INTEGER:
      case DOUBLE:
      case FLOAT:
        return type;
      default:
        throw new IllegalStateException(NumericSortedSetFieldSource.class+" should not be instantiated with type \""+type+'"');
    }
  }

  @Override
  public SortField getSortField(boolean reverse) {
    SortField ret = new NumericSortedSetSortField(field, numberType, reverse, selector);
    FieldType.applySetMissingValue(sf, ret, numberType.sortMissingLow, numberType.sortMissingHigh);
    return ret;
  }

  public static NumericDocValues sortedSetAsNumericDocValues(LeafReader reader, String field, SortedSetSelector.Type selector, SortField.Type numericType) throws IOException {
    SortedSetDocValues sortedSet = DocValues.getSortedSet(reader, field);
    SortedDocValues singleValuedView = SortedSetSelector.wrap(sortedSet, selector);
    return new SortedSetNumericDocValues(singleValuedView, numericType);
  }

  public static final class SortedSetNumericDocValues extends NumericDocValues {

    private final SortedDocValues backing;
    private final SortField.Type numericType;

    public SortedSetNumericDocValues(SortedDocValues backing, SortField.Type numericType) {
      this.backing = backing;
      this.numericType = numericType;
    }
    @Override
    public long longValue() throws IOException {
      BytesRef bytes = backing.binaryValue();
      assert bytes.length > 0;
      switch (numericType) {
        case INT:
          return LegacyNumericUtils.prefixCodedToInt(bytes);
        case LONG:
          return LegacyNumericUtils.prefixCodedToLong(bytes);
        case FLOAT:
          return NumericUtils.sortableFloatBits(LegacyNumericUtils.prefixCodedToInt(bytes));
        case DOUBLE:
          return NumericUtils.sortableDoubleBits(LegacyNumericUtils.prefixCodedToLong(bytes));
        default:
          throw new AssertionError();
      }
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
      return backing.advanceExact(target);
    }

    @Override
    public int docID() {
      return backing.docID();
    }

    @Override
    public int nextDoc() throws IOException {
      return backing.nextDoc();
    }

    @Override
    public int advance(int target) throws IOException {
      return backing.advance(target);
    }

    @Override
    public long cost() {
      return backing.cost();
    }
  }
}
