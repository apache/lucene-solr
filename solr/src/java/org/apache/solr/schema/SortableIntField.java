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
import org.apache.lucene.util.BytesRef;
import org.apache.noggit.CharArr;
import org.apache.solr.search.MutableValueInt;
import org.apache.solr.search.MutableValue;
import org.apache.solr.search.QParser;
import org.apache.solr.search.function.ValueSource;
import org.apache.solr.search.function.FieldCacheSource;
import org.apache.solr.search.function.DocValues;
import org.apache.solr.search.function.StringIndexDocValues;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.IndexReader.AtomicReaderContext;
import org.apache.solr.util.ByteUtils;
import org.apache.solr.util.NumberUtils;
import org.apache.solr.response.TextResponseWriter;

import java.util.Map;
import java.io.IOException;
/**
 * @version $Id$
 * 
 * @deprecated use {@link IntField} or {@link TrieIntField} - will be removed in 5.x
 */
@Deprecated
public class SortableIntField extends FieldType {
  @Override
  protected void init(IndexSchema schema, Map<String,String> args) {
  }

  @Override
  public SortField getSortField(SchemaField field,boolean reverse) {
    return getStringSort(field,reverse);
  }

  @Override
  public ValueSource getValueSource(SchemaField field, QParser qparser) {
    field.checkFieldCacheSource(qparser);
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
  public void indexedToReadable(BytesRef input, CharArr out) {
    // TODO: this could be more efficient, but the sortable types should be deprecated instead
    out.write( indexedToReadable(ByteUtils.UTF8toUTF16(input)) );
  }

  @Override
  public Integer toObject(Fieldable f) {
    return NumberUtils.SortableStr2int(f.stringValue(), 0, 3);    
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
  public DocValues getValues(Map context, AtomicReaderContext readerContext) throws IOException {
    final int def = defVal;

    return new StringIndexDocValues(this, readerContext, field) {
      private final BytesRef spare = new BytesRef();

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
        int ord=termsIndex.getOrd(doc);
        return ord==0 ? def  : NumberUtils.SortableStr2int(termsIndex.lookup(ord, spare),0,3);
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

      @Override
      public Object objectVal(int doc) {
        int ord=termsIndex.getOrd(doc);
        return ord==0 ? null  : NumberUtils.SortableStr2int(termsIndex.lookup(ord, spare));
      }

      @Override
      public ValueFiller getValueFiller() {
        return new ValueFiller() {
          private final MutableValueInt mval = new MutableValueInt();

          @Override
          public MutableValue getValue() {
            return mval;
          }

          @Override
          public void fillValue(int doc) {
            int ord=termsIndex.getOrd(doc);
            if (ord == 0) {
              mval.value = def;
              mval.exists = false;
            } else {
              mval.value = NumberUtils.SortableStr2int(termsIndex.lookup(ord, spare),0,3);
              mval.exists = true;
            }
          }
        };
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
