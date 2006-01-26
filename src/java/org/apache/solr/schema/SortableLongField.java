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
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.search.function.ValueSource;
import org.apache.lucene.search.function.FieldCacheSource;
import org.apache.lucene.search.function.DocValues;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.solr.util.NumberUtils;
import org.apache.solr.request.XMLWriter;

import java.util.Map;
import java.io.IOException;
/**
 * @author yonik
 * @version $Id$
 */
public class SortableLongField extends FieldType {
  protected void init(IndexSchema schema, Map<String,String> args) {
  }

  public SortField getSortField(SchemaField field,boolean reverse) {
    return getStringSort(field,reverse);
  }

  public ValueSource getValueSource(SchemaField field) {
    return new SortableLongFieldSource(field.name);
  }

  public String toInternal(String val) {
    return NumberUtils.long2sortableStr(val);
  }

  public String indexedToReadable(String indexedForm) {
    return NumberUtils.SortableStr2long(indexedForm);
  }

  public String toExternal(Field f) {
    return indexedToReadable(f.stringValue());
  }

  public void write(XMLWriter xmlWriter, String name, Field f) throws IOException {
    String sval = f.stringValue();
    xmlWriter.writeLong(name, NumberUtils.SortableStr2long(sval,0,sval.length()));
  }
}





class SortableLongFieldSource extends FieldCacheSource {
  protected long defVal;

  public SortableLongFieldSource(String field) {
    this(field, 0);
  }

  public SortableLongFieldSource(String field, long defVal) {
    super(field);
    this.defVal = defVal;
  }

  public String description() {
    return "slong(" + field + ')';
  }

  public DocValues getValues(IndexReader reader) throws IOException {
    final FieldCache.StringIndex index = cache.getStringIndex(reader, field);
    final int[] order = index.order;
    final String[] lookup = index.lookup;
    final long def = defVal;

    return new DocValues() {
      public float floatVal(int doc) {
        return (float)longVal(doc);
      }

      public int intVal(int doc) {
        return (int)longVal(doc);
      }

      public long longVal(int doc) {
        int ord=order[doc];
        return ord==0 ? def  : NumberUtils.SortableStr2long(lookup[ord],0,5);
      }

      public double doubleVal(int doc) {
        return (double)longVal(doc);
      }

      public String strVal(int doc) {
        return Long.toString(longVal(doc));
      }

      public String toString(int doc) {
        return description() + '=' + longVal(doc);
      }
    };
  }

  public boolean equals(Object o) {
    return o instanceof SortableLongFieldSource
            && super.equals(o)
            && defVal == ((SortableLongFieldSource)o).defVal;
  }

  private static int hcode = SortableLongFieldSource.class.hashCode();
  public int hashCode() {
    return hcode + super.hashCode() + (int)defVal;
  };
}
