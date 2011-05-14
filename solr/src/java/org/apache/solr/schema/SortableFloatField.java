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
import org.apache.solr.search.MutableValueFloat;
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
 * @deprecated use {@link FloatField} or {@link TrieFloatField} - will be removed in 5.x
 */
@Deprecated
public class SortableFloatField extends FieldType {
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
  public void indexedToReadable(BytesRef input, CharArr out) {
    // TODO: this could be more efficient, but the sortable types should be deprecated instead
    out.write( indexedToReadable(ByteUtils.UTF8toUTF16(input)) );
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
  public DocValues getValues(Map context, AtomicReaderContext readerContext) throws IOException {
    final float def = defVal;

    return new StringIndexDocValues(this, readerContext, field) {
      private final BytesRef spare = new BytesRef();

      @Override
      protected String toTerm(String readableValue) {
        return NumberUtils.float2sortableStr(readableValue);
      }

      @Override
      public float floatVal(int doc) {
        int ord=termsIndex.getOrd(doc);
        return ord==0 ? def  : NumberUtils.SortableStr2float(termsIndex.lookup(ord, spare));
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

      @Override
      public Object objectVal(int doc) {
        int ord=termsIndex.getOrd(doc);
        return ord==0 ? null  : NumberUtils.SortableStr2float(termsIndex.lookup(ord, spare));
      }

      @Override
      public ValueFiller getValueFiller() {
        return new ValueFiller() {
          private final MutableValueFloat mval = new MutableValueFloat();

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
              mval.value = NumberUtils.SortableStr2float(termsIndex.lookup(ord, spare));
              mval.exists = true;
            }
          }
        };
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




