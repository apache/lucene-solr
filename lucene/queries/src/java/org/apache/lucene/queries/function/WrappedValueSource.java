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
package org.apache.lucene.queries.function;

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LongValuesSource;
import org.apache.lucene.search.SortField;

/**
 * Wraps a ValueSource that derives values directly from a particular SchemaField.
 * This allows a relatively clean way to set missingValue on ValueSource.getSortField,
 * and also clearly marks SortFields that are directly associated with a SchemaField
 * (e.g., for purposes of marshaling/unmarshaling sort values).
 */
public class WrappedValueSource extends ValueSource {

  private final ValueSource backing;

  public WrappedValueSource(ValueSource backing) {
    this.backing = backing;
  }

  public ValueSource unwrap() {
    return backing;
  }

  @Override
  public void createWeight(Map context, IndexSearcher searcher) throws IOException {
    backing.createWeight(context, searcher);
  }

  @Override
  public LongValuesSource asLongValuesSource() {
    return backing.asLongValuesSource();
  }

  @Override
  public DoubleValuesSource asDoubleValuesSource() {
    return backing.asDoubleValuesSource();
  }

  @Override
  public SortField getSortField(boolean reverse) {
    return backing.getSortField(reverse);
  }

  @Override
  public FunctionValues getValues(Map context, LeafReaderContext readerContext) throws IOException {
    return backing.getValues(context, readerContext);
  }

  @Override
  public boolean equals(Object o) {
    if (o == null) {
      return false;
    } else if (!(o instanceof ValueSource)) {
      return false;
    } else {
      return backing.equals(unwrap((ValueSource)o));
    }
  }

  @Override
  public int hashCode() {
    return backing.hashCode();
  }

  @Override
  public String description() {
    return "<wrapped "+backing.description()+">";
  }

  public static ValueSource unwrap(ValueSource vs) {
    if (vs == null) {
      return null;
    } else if (vs instanceof WrappedValueSource) {
      return ((WrappedValueSource)vs).unwrap();
    } else {
      return vs;
    }
  }
}
