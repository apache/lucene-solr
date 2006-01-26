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
public class SortableIntField extends FieldType {
  protected void init(IndexSchema schema, Map<String,String> args) {
  }

  public SortField getSortField(SchemaField field,boolean reverse) {
    return getStringSort(field,reverse);
  }

  public ValueSource getValueSource(SchemaField field) {
    return new SortableIntFieldSource(field.name);
  }

  public String toInternal(String val) {
    // special case single digits?  years?, etc
    // stringCache?  general stringCache on a
    // global field level?
    return NumberUtils.int2sortableStr(val);
  }

  public String toExternal(Field f) {
    return indexedToReadable(f.stringValue());
  }

  public String indexedToReadable(String indexedForm) {
    return NumberUtils.SortableStr2int(indexedForm);
  }

  public void write(XMLWriter xmlWriter, String name, Field f) throws IOException {
    String sval = f.stringValue();
    // since writeInt an int instead of a String since that may be more efficient
    // in the future (saves the construction of one String)
    xmlWriter.writeInt(name, NumberUtils.SortableStr2int(sval,0,sval.length()));
  }
}



class SortableIntFieldSource extends FieldCacheSource {
  protected int defVal;

  public SortableIntFieldSource(String field) {
    this(field, 0);
  }

  public SortableIntFieldSource(String field, int defVal) {
    super(field);
    this.defVal = defVal;
  }

  public String description() {
    return "sint(" + field + ')';
  }

  public DocValues getValues(IndexReader reader) throws IOException {
    final FieldCache.StringIndex index = cache.getStringIndex(reader, field);
    final int[] order = index.order;
    final String[] lookup = index.lookup;
    final int def = defVal;

    return new DocValues() {
      public float floatVal(int doc) {
        return (float)intVal(doc);
      }

      public int intVal(int doc) {
        int ord=order[doc];
        return ord==0 ? def  : NumberUtils.SortableStr2int(lookup[ord],0,3);
      }

      public long longVal(int doc) {
        return (long)intVal(doc);
      }

      public double doubleVal(int doc) {
        return (double)intVal(doc);
      }

      public String strVal(int doc) {
        return Integer.toString(intVal(doc));
      }

      public String toString(int doc) {
        return description() + '=' + intVal(doc);
      }
    };
  }

  public boolean equals(Object o) {
    return o instanceof SortableIntFieldSource
            && super.equals(o)
            && defVal == ((SortableIntFieldSource)o).defVal;
  }

  private static int hcode = SortableIntFieldSource.class.hashCode();
  public int hashCode() {
    return hcode + super.hashCode() + defVal;
  };
}
