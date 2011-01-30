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
import org.apache.solr.search.function.FieldCacheSource;
import org.apache.solr.search.function.DocValues;
import org.apache.solr.search.function.StringIndexDocValues;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.IndexReader;
import org.apache.solr.util.NumberUtils;
import org.apache.solr.response.TextResponseWriter;
import org.apache.solr.response.XMLWriter;

import java.util.Map;
import java.io.IOException;
/**
 * @version $Id$
 */
public class SortableFloatField extends FieldType {
  @Override
  protected void init(IndexSchema schema, Map<String,String> args) {
  }

  @Override
  public SortField getSortField(SchemaField field,boolean reverse) {
    return getStringSort(field,reverse);
  }

  @Override
  public ValueSource getValueSource(SchemaField field) {
    return new SortableFloatFieldSource(field.name);
  }

  @Override
  public String toInternal(String val) {
    return NumberUtils.float2sortableStr(val);
  }

  @Override
  public String toExternal(Fieldable f) {
    return indexedToReadable(f.stringValue());
  }

  @Override
  public Float toObject(Fieldable f) {
    return NumberUtils.SortableStr2float(f.stringValue());
  }
  
  @Override
  public String indexedToReadable(String indexedForm) {
    return NumberUtils.SortableStr2floatStr(indexedForm);
  }

  @Override
  public void write(XMLWriter xmlWriter, String name, Fieldable f) throws IOException {
    String sval = f.stringValue();
    xmlWriter.writeFloat(name, NumberUtils.SortableStr2float(sval));
  }

  @Override
  public void write(TextResponseWriter writer, String name, Fieldable f) throws IOException {
    String sval = f.stringValue();
    writer.writeFloat(name, NumberUtils.SortableStr2float(sval));
  }
}




class SortableFloatFieldSource extends FieldCacheSource {
  protected float defVal;

  public SortableFloatFieldSource(String field) {
    this(field, 0.0f);
  }

  public SortableFloatFieldSource(String field, float defVal) {
    super(field);
    this.defVal = defVal;
  }

    @Override
    public String description() {
    return "sfloat(" + field + ')';
  }

  @Override
  public DocValues getValues(Map context, IndexReader reader) throws IOException {
    final float def = defVal;

    return new StringIndexDocValues(this, reader, field) {
      @Override
      protected String toTerm(String readableValue) {
        return NumberUtils.float2sortableStr(readableValue);
      }

      @Override
      public float floatVal(int doc) {
        int ord=order[doc];
        return ord==0 ? def  : NumberUtils.SortableStr2float(lookup[ord]);
      }

      @Override
      public int intVal(int doc) {
        return (int)floatVal(doc);
      }

      @Override
      public long longVal(int doc) {
        return (long)floatVal(doc);
      }

      @Override
      public double doubleVal(int doc) {
        return (double)floatVal(doc);
      }

      @Override
      public String strVal(int doc) {
        return Float.toString(floatVal(doc));
      }

      @Override
      public String toString(int doc) {
        return description() + '=' + floatVal(doc);
      }
    };
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof SortableFloatFieldSource
            && super.equals(o)
            && defVal == ((SortableFloatFieldSource)o).defVal;
  }

  private static int hcode = SortableFloatFieldSource.class.hashCode();
  @Override
  public int hashCode() {
    return hcode + super.hashCode() + Float.floatToIntBits(defVal);
  };
}




