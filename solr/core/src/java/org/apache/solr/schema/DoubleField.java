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

import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.DoubleFieldSource;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.SortField;
import org.apache.solr.response.TextResponseWriter;
import org.apache.solr.search.QParser;

import java.io.IOException;
import java.util.Map;

/**
 * A legacy numeric field type that encodes "Double" values as simple Strings.
 * This class should not be used except by people with existing indexes that
 * contain numeric values indexed as Strings.  
 * New schemas should use {@link TrieDoubleField}.
 *
 * <p>
 * Field values will sort numerically, but Range Queries (and other features 
 * that rely on numeric ranges) will not work as expected: values will be 
 * evaluated in unicode String order, not numeric order.
 * </p>
 * 
 * @see TrieDoubleField
 */
public class DoubleField extends FieldType {
  @Override
  protected void init(IndexSchema schema, Map<String, String> args) {
    restrictProps(SORT_MISSING_FIRST | SORT_MISSING_LAST);
  }

  /////////////////////////////////////////////////////////////
  @Override
  public SortField getSortField(SchemaField field, boolean reverse) {
    field.checkSortability();
    return new SortField(field.name, SortField.Type.DOUBLE, reverse);
  }

  @Override
  public ValueSource getValueSource(SchemaField field, QParser qparser) {
    field.checkFieldCacheSource(qparser);
    return new DoubleFieldSource(field.name);
  }

  @Override
  public void write(TextResponseWriter writer, String name, IndexableField f) throws IOException {
    String s = f.stringValue();

    // these values may be from a legacy lucene index, which may
    // not be properly formatted in some output formats, or may
    // incorrectly have a zero length.

    if (s.length()==0) {
      // zero length value means someone mistakenly indexed the value
      // instead of simply leaving it out.  Write a null value instead of a numeric.
      writer.writeNull(name);
      return;
    }

    try {
      double val = Double.parseDouble(s);
      writer.writeDouble(name, val);
    } catch (NumberFormatException e){
      // can't parse - write out the contents as a string so nothing is lost and
      // clients don't get a parse error.
      writer.writeStr(name, s, true);
    }
  }


  @Override
  public Double toObject(IndexableField f) {
    return Double.valueOf(toExternal(f));
  }
}
