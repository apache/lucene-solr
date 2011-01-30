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
public class SortableIntField extends FieldType {
  @Override
  protected void init(IndexSchema schema, Map<String,String> args) {
  }

  @Override
  public SortField getSortField(SchemaField field,boolean reverse) {
    return getStringSort(field,reverse);
  }

  @Override
  public ValueSource getValueSource(SchemaField field) {
    return new SortableIntFieldSource(field.name);
  }

  @Override
  public String toInternal(String val) {
    // special case single digits?  years?, etc
    // stringCache?  general stringCache on a
    // global field level?
    return NumberUtils.int2sortableStr(val);
  }

  @Override
  public String toExternal(Fieldable f) {
    return indexedToReadable(f.stringValue());
  }

  @Override
  public String indexedToReadable(String indexedForm) {
    return NumberUtils.SortableStr2int(indexedForm);
  }

  @Override
  public Integer toObject(Fieldable f) {
    return NumberUtils.SortableStr2int(f.stringValue(), 0, 3);    
  }
  
  @Override
  public void write(XMLWriter xmlWriter, String name, Fieldable f) throws IOException {
    String sval = f.stringValue();
    // since writeInt an int instead of a String since that may be more efficient
    // in the future (saves the construction of one String)
    xmlWriter.writeInt(name, NumberUtils.SortableStr2int(sval,0,sval.length()));
  }

  @Override
  public void write(TextResponseWriter writer, String name, Fieldable f) throws IOException {
    String sval = f.stringValue();
    writer.writeInt(name, NumberUtils.SortableStr2int(sval,0,sval.length()));
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

  @Override
  public String description() {
    return "sint(" + field + ')';
  }

  @Override
  public DocValues getValues(Map context, IndexReader reader) throws IOException {
    final int def = defVal;

    return new StringIndexDocValues(this, reader, field) {
      @Override
      protected String toTerm(String readableValue) {
        return NumberUtils.int2sortableStr(readableValue);
      }

      @Override
      public float floatVal(int doc) {
        return (float)intVal(doc);
      }

      @Override
      public int intVal(int doc) {
        int ord=order[doc];
        return ord==0 ? def  : NumberUtils.SortableStr2int(lookup[ord],0,3);
      }

      @Override
      public long longVal(int doc) {
        return (long)intVal(doc);
      }

      @Override
      public double doubleVal(int doc) {
        return (double)intVal(doc);
      }

      @Override
      public String strVal(int doc) {
        return Integer.toString(intVal(doc));
      }

      @Override
      public String toString(int doc) {
        return description() + '=' + intVal(doc);
      }
    };
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof SortableIntFieldSource
            && super.equals(o)
            && defVal == ((SortableIntFieldSource)o).defVal;
  }

  private static int hcode = SortableIntFieldSource.class.hashCode();
  @Override
  public int hashCode() {
    return hcode + super.hashCode() + defVal;
  };
}
