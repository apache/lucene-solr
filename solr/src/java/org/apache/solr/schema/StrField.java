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
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.response.TextResponseWriter;
import org.apache.solr.search.function.ValueSource;
import org.apache.solr.search.QParser;

import java.util.Map;
import java.io.IOException;
/**
 * @version $Id$
 */
public class StrField extends FieldType {
  @Override
  protected void init(IndexSchema schema, Map<String,String> args) {
    super.init(schema, args);    
  }

  @Override
  public SortField getSortField(SchemaField field,boolean reverse) {
    return getStringSort(field,reverse);
  }

  @Override
  public void write(TextResponseWriter writer, String name, Fieldable f) throws IOException {
    writer.writeStr(name, f.stringValue(), true);
  }

  @Override
  public ValueSource getValueSource(SchemaField field, QParser parser) {
    field.checkFieldCacheSource(parser);
    return new StrFieldSource(field.getName());
  }

  @Override
  public Object toObject(SchemaField sf, BytesRef term) {
    return term.utf8ToString();
  }
}


