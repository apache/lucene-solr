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
package org.apache.lucene.queries.function.valuesource;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSelector;
import org.apache.lucene.search.SortedNumericSelector.Type;
import org.apache.lucene.search.SortedNumericSortField;

/**
 * Obtains double field values from {@link
 * org.apache.lucene.index.LeafReader#getSortedNumericDocValues} and using a {@link
 * org.apache.lucene.search.SortedNumericSelector} it gives a single-valued ValueSource view of a
 * field.
 */
public class MultiValuedDoubleFieldSource extends DoubleFieldSource {

  protected final SortedNumericSelector.Type selector;

  public MultiValuedDoubleFieldSource(String field, Type selector) {
    super(field);
    this.selector = selector;
    Objects.requireNonNull(field, "Field is required to create a MultiValuedDoubleFieldSource");
    Objects.requireNonNull(
        selector, "SortedNumericSelector is required to create a MultiValuedDoubleFieldSource");
  }

  @Override
  public SortField getSortField(boolean reverse) {
    return new SortedNumericSortField(field, SortField.Type.DOUBLE, reverse, selector);
  }

  @Override
  public String description() {
    return "double(" + field + ',' + selector.name() + ')';
  }

  @Override
  protected NumericDocValues getNumericDocValues(
      Map<Object, Object> context, LeafReaderContext readerContext) throws IOException {
    SortedNumericDocValues sortedDv = DocValues.getSortedNumeric(readerContext.reader(), field);
    return SortedNumericSelector.wrap(sortedDv, selector, SortField.Type.DOUBLE);
  }

  @Override
  public boolean equals(Object o) {
    if (o.getClass() != MultiValuedDoubleFieldSource.class) return false;
    MultiValuedDoubleFieldSource other = (MultiValuedDoubleFieldSource) o;
    if (this.selector != other.selector) return false;
    return this.field.equals(other.field);
  }

  @Override
  public int hashCode() {
    int h = super.hashCode();
    h += selector.hashCode();
    return h;
  }
}
