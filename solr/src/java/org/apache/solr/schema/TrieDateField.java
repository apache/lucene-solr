/**
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

import org.apache.noggit.CharArr;
import org.apache.solr.search.function.ValueSource;
import org.apache.solr.search.QParser;
import org.apache.solr.response.TextResponseWriter;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.util.BytesRef;

import java.util.Map;
import java.util.Date;
import java.io.IOException;

public class TrieDateField extends DateField {

  final TrieField wrappedField = new TrieField() {{
    type = TrieTypes.DATE;
  }};

  @Override
  protected void init(IndexSchema schema, Map<String, String> args) {
    wrappedField.init(schema, args);
    analyzer = wrappedField.analyzer;
    queryAnalyzer = wrappedField.queryAnalyzer;
  }

  @Override
  public Date toObject(Fieldable f) {
    return (Date) wrappedField.toObject(f);
  }

  @Override
  public Object toObject(SchemaField sf, BytesRef term) {
    return wrappedField.toObject(sf, term);
  }

  @Override
  public SortField getSortField(SchemaField field, boolean top) {
    return wrappedField.getSortField(field, top);
  }

  @Override
  public ValueSource getValueSource(SchemaField field, QParser parser) {
    return wrappedField.getValueSource(field, parser);
  }

  /**
   * @return the precisionStep used to index values into the field
   */
  public int getPrecisionStep() {
    return wrappedField.getPrecisionStep();
  }


  @Override
  public void write(TextResponseWriter writer, String name, Fieldable f) throws IOException {
    wrappedField.write(writer, name, f);
  }

  @Override
  public boolean isTokenized() {
    return wrappedField.isTokenized();
  }

  @Override
  public boolean multiValuedFieldCache() {
    return wrappedField.multiValuedFieldCache();
  }

  @Override
  public String storedToReadable(Fieldable f) {
    return wrappedField.storedToReadable(f);
  }

  @Override
  public String readableToIndexed(String val) {  
    return wrappedField.readableToIndexed(val);
  }

  @Override
  public String toInternal(String val) {
    return wrappedField.toInternal(val);
  }

  @Override
  public String toExternal(Fieldable f) {
    return wrappedField.toExternal(f);
  }

  @Override
  public String indexedToReadable(String _indexedForm) {
    return wrappedField.indexedToReadable(_indexedForm);
  }

  @Override
  public void indexedToReadable(BytesRef input, CharArr out) {
    wrappedField.indexedToReadable(input, out);
  }

  @Override
  public String storedToIndexed(Fieldable f) {
    return wrappedField.storedToIndexed(f);
  }

  @Override
  public Fieldable createField(SchemaField field, Object value, float boost) {
    return wrappedField.createField(field, value, boost);
  }

  @Override
  public Query getRangeQuery(QParser parser, SchemaField field, String min, String max, boolean minInclusive, boolean maxInclusive) {
    return wrappedField.getRangeQuery(parser, field, min, max, minInclusive, maxInclusive);
  }
  
  @Override
  public Query getRangeQuery(QParser parser, SchemaField sf, Date min, Date max, boolean minInclusive, boolean maxInclusive) {
    return NumericRangeQuery.newLongRange(sf.getName(), wrappedField.precisionStep,
              min == null ? null : min.getTime(),
              max == null ? null : max.getTime(),
              minInclusive, maxInclusive);
  }
}
