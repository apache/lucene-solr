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

  private final SortField.Type numericType;

  public NumericSortedSetFieldSource(String field, Type selector, SortField.Type numericType) {
    super(field, selector);
    this.numericType = validateType(numericType);
  }

  public NumericSortedSetFieldSource(String field, SortField.Type numericType) {
    super(field);
    this.numericType = validateType(numericType);
  }

  private static SortField.Type validateType(SortField.Type type) {
    switch (type) {
      // whitelisted types
      case LONG:
      case INT:
      case DOUBLE:
      case FLOAT:
        return type;
      default:
        throw new IllegalStateException(NumericSortedSetFieldSource.class+" should not be instantiated with type \""+type+'"');
    }
  }

  @Override
  public SortField getSortField(boolean reverse) {
    return new NumericSortedSetSortField(field, numericType, reverse, selector);
  }

  public static NumericDocValues sortedSetAsNumericDocValues(LeafReader reader, String field, SortedSetSelector.Type selector, SortField.Type numericType) throws IOException {
    SortedSetDocValues sortedSet = DocValues.getSortedSet(reader, field);
    SortedDocValues singleValuedView = SortedSetSelector.wrap(sortedSet, selector);
    NumericDocValues view = new SortedSetNumericDocValues(singleValuedView);
    switch (numericType) {
      case FLOAT:
        return new FilterNumericDocValues(view) {
          @Override
          public long longValue() throws IOException {
            return NumericUtils.sortableFloatBits((int) in.longValue());
          }
        };
      case DOUBLE:
        return new FilterNumericDocValues(view) {
          @Override
          public long longValue() throws IOException {
            return NumericUtils.sortableDoubleBits(in.longValue());
          }
        };
      default:
        return view;
    }
  }

  public static final class SortedSetNumericDocValues extends NumericDocValues {

    private final SortedDocValues backing;

    public SortedSetNumericDocValues(SortedDocValues backing) {
      this.backing = backing;
    }
    @Override
    public long longValue() throws IOException {
      BytesRef bytes = backing.binaryValue();
      assert bytes.length > 0;
      return LegacyNumericUtils.prefixCodedToLong(bytes);
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
