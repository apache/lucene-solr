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
public class SortableDoubleField extends FieldType {
  protected void init(IndexSchema schema, Map<String,String> args) {
  }

  public SortField getSortField(SchemaField field,boolean reverse) {
    return getStringSort(field,reverse);
  }

  public ValueSource getValueSource(SchemaField field) {
    return new SortableDoubleFieldSource(field.name);
  }

  public String toInternal(String val) {
    return NumberUtils.double2sortableStr(val);
  }

  public String toExternal(Field f) {
    return indexedToReadable(f.stringValue());
  }

  public String indexedToReadable(String indexedForm) {
    return NumberUtils.SortableStr2doubleStr(indexedForm);
  }

  public void write(XMLWriter xmlWriter, String name, Field f) throws IOException {
    String sval = f.stringValue();
    xmlWriter.writeDouble(name, NumberUtils.SortableStr2double(sval));
  }
}




class SortableDoubleFieldSource extends FieldCacheSource {
  protected double defVal;

  public SortableDoubleFieldSource(String field) {
    this(field, 0.0);
  }

  public SortableDoubleFieldSource(String field, double defVal) {
    super(field);
    this.defVal = defVal;
  }

  public String description() {
    return "sdouble(" + field + ')';
  }

  public DocValues getValues(IndexReader reader) throws IOException {
    final FieldCache.StringIndex index = cache.getStringIndex(reader, field);
    final int[] order = index.order;
    final String[] lookup = index.lookup;
    final double def = defVal;

    return new DocValues() {
      public float floatVal(int doc) {
        return (float)doubleVal(doc);
      }

      public int intVal(int doc) {
        return (int)doubleVal(doc);
      }

      public long longVal(int doc) {
        return (long)doubleVal(doc);
      }

      public double doubleVal(int doc) {
        int ord=order[doc];
        return ord==0 ? def  : NumberUtils.SortableStr2double(lookup[ord]);
      }

      public String strVal(int doc) {
        return Double.toString(doubleVal(doc));
      }

      public String toString(int doc) {
        return description() + '=' + doubleVal(doc);
      }
    };
  }

  public boolean equals(Object o) {
    return o instanceof SortableDoubleFieldSource
            && super.equals(o)
            && defVal == ((SortableDoubleFieldSource)o).defVal;
  }

  private static int hcode = SortableDoubleFieldSource.class.hashCode();
  public int hashCode() {
    long bits = Double.doubleToLongBits(defVal);
    int ibits = (int)(bits ^ (bits>>>32));  // mix upper bits into lower.
    return hcode + super.hashCode() + ibits;
  };
}


