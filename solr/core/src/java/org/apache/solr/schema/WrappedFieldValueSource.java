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
import java.util.Map;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LongValuesSource;
import org.apache.lucene.search.SortField;

public class WrappedFieldValueSource extends ValueSource {

  private final SchemaField f;
  private final ValueSource backing;

  public WrappedFieldValueSource(SchemaField f, ValueSource backing) {
    this.f = f;
    this.backing = backing;
  }

  public ValueSource unwrap() {
    return backing;
  }

  public SchemaField getSchemaField() {
    return f;
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
    SortField ret = backing.getSortField(reverse);
    if (ret.getMissingValue() == null) {
      ret.setMissingValue(f.getSortField(reverse).getMissingValue());
    }
    return ret;
  }

  @Override
  public FunctionValues getValues(Map context, LeafReaderContext readerContext) throws IOException {
    return backing.getValues(context, readerContext);
  }

  @Override
  public boolean equals(Object o) {
    if (o == null) {
      return false;
    } else if (!(o instanceof WrappedFieldValueSource)) {
      return false;
    } else {
      WrappedFieldValueSource other = (WrappedFieldValueSource)o;
      return other.backing.equals(this.backing) && other.f.equals(this.f);
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
}
