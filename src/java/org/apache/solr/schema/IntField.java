/**
 * Copyright 2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
import org.apache.lucene.search.function.ValueSource;
import org.apache.lucene.search.function.IntFieldSource;
import org.apache.lucene.document.Field;
import org.apache.solr.request.XMLWriter;

import java.util.Map;
import java.io.IOException;
/**
 * @author yonik
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

  public void write(XMLWriter xmlWriter, String name, Field f) throws IOException {
    xmlWriter.writeInt(name, f.stringValue());
  }
}
