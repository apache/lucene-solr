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
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.docvalues.DocTermsIndexDocValues;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedSetSelector;
import org.apache.lucene.search.SortedSetSortField;

/**
 * Retrieves {@link FunctionValues} instances for multi-valued string based fields.
 *
 * <p>A SortedSetDocValues contains multiple values for a field, so this technique "selects" a value
 * as the representative value for the document.
 *
 * @see SortedSetSelector
 */
public class SortedSetFieldSource extends FieldCacheSource {
  protected final SortedSetSelector.Type selector;

  public SortedSetFieldSource(String field) {
    this(field, SortedSetSelector.Type.MIN);
  }

  public SortedSetFieldSource(String field, SortedSetSelector.Type selector) {
    super(field);
    this.selector = selector;
  }

  @Override
  public SortField getSortField(boolean reverse) {
    return new SortedSetSortField(this.field, reverse, this.selector);
  }

  @Override
  public FunctionValues getValues(Map<Object, Object> context, LeafReaderContext readerContext)
      throws IOException {
    SortedSetDocValues sortedSet = DocValues.getSortedSet(readerContext.reader(), field);
    SortedDocValues view = SortedSetSelector.wrap(sortedSet, selector);
    return new DocTermsIndexDocValues(this, view) {
      @Override
      protected String toTerm(String readableValue) {
        return readableValue;
      }

      @Override
      public Object objectVal(int doc) throws IOException {
        return strVal(doc);
      }
    };
  }

  @Override
  public String description() {
    return "sortedset(" + field + ",selector=" + selector + ')';
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((selector == null) ? 0 : selector.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (!super.equals(obj)) return false;
    if (getClass() != obj.getClass()) return false;
    SortedSetFieldSource other = (SortedSetFieldSource) obj;
    if (selector != other.selector) return false;
    return true;
  }
}
