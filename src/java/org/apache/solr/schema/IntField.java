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

import org.apache.lucene.search.SortField;
import org.apache.solr.search.function.ValueSource;
import org.apache.solr.search.function.IntFieldSource;
import org.apache.lucene.document.Fieldable;
import org.apache.solr.request.XMLWriter;
import org.apache.solr.request.TextResponseWriter;

import java.util.Map;
import java.io.IOException;
/**
 * @version $Id$
 */
public class IntField extends FieldType {
  protected void init(IndexSchema schema, Map<String,String> args) {
    restrictProps(SORT_MISSING_FIRST | SORT_MISSING_LAST);
  }

  public SortField getSortField(SchemaField field,boolean reverse) {
    return new SortField(field.name,SortField.INT, reverse);
  }

  public ValueSource getValueSource(SchemaField field) {
    return new IntFieldSource(field.name);
  }

  public void write(XMLWriter xmlWriter, String name, Fieldable f) throws IOException {
    xmlWriter.writeInt(name, f.stringValue());
  }

  public void write(TextResponseWriter writer, String name, Fieldable f) throws IOException {
    String s = f.stringValue();
    int len = s.length();
    // these values may be from a legacy lucene index, which may contain
    // integer values padded with zeros, or a zero length value.
    if (len>=2) {
      char ch = s.charAt(0);
      if ((ch=='0') || (ch=='-' && s.charAt(1)=='0')) {
        s = Integer.toString(Integer.parseInt(s));
      }
    } else if (len == 0) {
      // zero length value means someone mistakenly indexed the value
      // instead of simply leaving it out.  Write a null value instead
      // of an integer value in this case.
      writer.writeNull(name);
      return;
    }
    writer.writeInt(name, s);
  }

  @Override
  public Integer toObject(Fieldable f) {
    return Integer.valueOf( toExternal(f) );
  }
}
