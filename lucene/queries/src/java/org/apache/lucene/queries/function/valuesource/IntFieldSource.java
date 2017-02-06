/*
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
package org.apache.lucene.queries.function.valuesource;

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.docvalues.IntDocValues;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortField.Type;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.mutable.MutableValue;
import org.apache.lucene.util.mutable.MutableValueInt;

/**
 * Obtains int field values from {@link org.apache.lucene.index.LeafReader#getNumericDocValues} and makes those
 * values available as other numeric types, casting as needed.
 */
public class IntFieldSource extends FieldCacheSource {

  public IntFieldSource(String field) {
    super(field);
  }

  @Override
  public String description() {
    return "int(" + field + ')';
  }

  @Override
  public SortField getSortField(boolean reverse) {
    return new SortField(field, Type.INT, reverse);
  }
  
  @Override
  public FunctionValues getValues(Map context, LeafReaderContext readerContext) throws IOException {
    final NumericDocValues arr = getNumericDocValues(context, readerContext);;
    final Bits valid = DocValues.getDocsWithField(readerContext.reader(), field);
    
    return new IntDocValues(this) {

      @Override
      public int intVal(int doc) {
        return (int) arr.get(doc);
      }

      @Override
      public String strVal(int doc) {
        return Integer.toString(intVal(doc));
      }

      @Override
      public boolean exists(int doc) {
        return arr.get(doc) != 0 || valid.get(doc);
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
            mval.value = intVal(doc);
            mval.exists = mval.value != 0 || valid.get(doc);
          }
        };
      }
    };
  }

  protected NumericDocValues getNumericDocValues(Map context, LeafReaderContext readerContext) throws IOException {
    return DocValues.getNumeric(readerContext.reader(), field);
  }

  @Override
  public boolean equals(Object o) {
    if (o.getClass() !=  IntFieldSource.class) return false;
    IntFieldSource other = (IntFieldSource)o;
    return super.equals(other);
  }

  @Override
  public int hashCode() {
    int h = Integer.class.hashCode();
    h += super.hashCode();
    return h;
  }
}
