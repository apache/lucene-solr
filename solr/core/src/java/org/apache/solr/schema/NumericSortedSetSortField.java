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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedSetSelector;

/**
 * SortField wrapping numeric {@link SortedSetDocValues}.
 * <p>
 * A SortedSetDocValues contains multiple values for a field, so sorting with
 * this technique "selects" a value as the representative sort value for the document.
 * <p>
 * By default, the minimum value in the list is selected as the sort value, but
 * this can be customized.
 * <p>
 * Like sorting by string, this also supports sorting missing values as first or last,
 * via {@link #setMissingValue(Object)}.
 * @see SortedSetSelector
 */
public class NumericSortedSetSortField extends SortField {

  private final SortedSetSelector.Type selector;
  private final NumberType numberType;

  /**
   * Creates a sort, by the minimum value in the set
   * for the document.
   * @param field Name of field to sort by.  Must not be null.
   * @param type Type of values
   */
  public NumericSortedSetSortField(String field, NumberType type) {
    this(field, type, false);
  }

  /**
   * Creates a sort, possibly in reverse, by the minimum value in the set
   * for the document.
   * @param field Name of field to sort by.  Must not be null.
   * @param type Type of values
   * @param reverse True if natural order should be reversed.
   */
  public NumericSortedSetSortField(String field, NumberType type, boolean reverse) {
    this(field, type, reverse, SortedSetSelector.Type.MIN);
  }

  /**
   * Creates a sort, possibly in reverse, specifying how the sort value from
   * the document's set is selected.
   * @param field Name of field to sort by.  Must not be null.
   * @param numericType Type of values
   * @param reverse True if natural order should be reversed.
   * @param selector custom selector type for choosing the sort value from the set.
   */
  public NumericSortedSetSortField(String field, NumberType numericType, boolean reverse, SortedSetSelector.Type selector) {
    super(field, SortField.Type.CUSTOM, reverse);
    if (selector == null) {
      throw new NullPointerException();
    }
    if (numericType == null) {
      throw new NullPointerException();
    }
    this.selector = selector;
    this.numberType = numericType;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + selector.hashCode();
    result = prime * result + numberType.hashCode();
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (!super.equals(obj)) return false;
    if (getClass() != obj.getClass()) return false;
    NumericSortedSetSortField other = (NumericSortedSetSortField) obj;
    if (selector != other.selector) return false;
    if (numberType != other.numberType) return false;
    return true;
  }

  @Override
  public String toString() {
    StringBuilder buffer = new StringBuilder();
    buffer.append("<numericsortedset" + ": \"").append(getField()).append("\">");
    if (getReverse()) buffer.append('!');
    if (missingValue != null) {
      buffer.append(" missingValue=");
      buffer.append(missingValue);
    }
    buffer.append(" selector=");
    buffer.append(selector);
    buffer.append(" type=");
    buffer.append(numberType);

    return buffer.toString();
  }

  @Override
  public void setMissingValue(Object missingValue) {
    if (missingValue == SortField.STRING_FIRST) {
      this.missingValue = numberType.sortMissingLow;
    } else if (missingValue == SortField.STRING_LAST) {
      this.missingValue = numberType.sortMissingHigh;
    } else {
      this.missingValue = missingValue;
    }
  }

  @Override
  public FieldComparator<?> getComparator(int numHits, int sortPos) {
    switch(numberType) {
      case INTEGER:
        return new FieldComparator.IntComparator(numHits, getField(), (Integer) missingValue) {
          @Override
          protected NumericDocValues getNumericDocValues(LeafReaderContext context, String field) throws IOException {
            return NumericSortedSetFieldSource.sortedSetAsNumericDocValues(context.reader(), field, selector, numberType.sortType);
          }
        };
      case FLOAT:
        return new FieldComparator.FloatComparator(numHits, getField(), (Float) missingValue) {
          @Override
          protected NumericDocValues getNumericDocValues(LeafReaderContext context, String field) throws IOException {
            return NumericSortedSetFieldSource.sortedSetAsNumericDocValues(context.reader(), field, selector, numberType.sortType);
          }
        };
      case LONG:
        return new FieldComparator.LongComparator(numHits, getField(), (Long) missingValue) {
          @Override
          protected NumericDocValues getNumericDocValues(LeafReaderContext context, String field) throws IOException {
            return NumericSortedSetFieldSource.sortedSetAsNumericDocValues(context.reader(), field, selector, numberType.sortType);
          }
        };
      case DOUBLE:
        return new FieldComparator.DoubleComparator(numHits, getField(), (Double) missingValue) {
          @Override
          protected NumericDocValues getNumericDocValues(LeafReaderContext context, String field) throws IOException {
            return NumericSortedSetFieldSource.sortedSetAsNumericDocValues(context.reader(), field, selector, numberType.sortType);
          }
        };
      default:
        throw new AssertionError();
    }
  }
}
