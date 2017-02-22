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
import org.apache.lucene.queries.function.docvalues.LongDocValues;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortField.Type;
import org.apache.lucene.util.mutable.MutableValue;
import org.apache.lucene.util.mutable.MutableValueLong;

/**
 * Obtains long field values from {@link org.apache.lucene.index.LeafReader#getNumericDocValues} and makes those
 * values available as other numeric types, casting as needed.
 */
public class LongFieldSource extends FieldCacheSource {

  public LongFieldSource(String field) {
    super(field);
  }

  @Override
  public String description() {
    return "long(" + field + ')';
  }

  public long externalToLong(String extVal) {
    return Long.parseLong(extVal);
  }

  public Object longToObject(long val) {
    return val;
  }

  public String longToString(long val) {
    return longToObject(val).toString();
  }

  @Override
  public SortField getSortField(boolean reverse) {
    return new SortField(field, Type.LONG, reverse);
  }
  
  @Override
  public FunctionValues getValues(Map context, LeafReaderContext readerContext) throws IOException {
    
    final NumericDocValues arr = getNumericDocValues(context, readerContext);

    return new LongDocValues(this) {
      int lastDocID;

      private long getValueForDoc(int doc) throws IOException {
        if (doc < lastDocID) {
          throw new IllegalArgumentException("docs were sent out-of-order: lastDocID=" + lastDocID + " vs docID=" + doc);
        }
        lastDocID = doc;
        int curDocID = arr.docID();
        if (doc > curDocID) {
          curDocID = arr.advance(doc);
        }
        if (doc == curDocID) {
          return arr.longValue();
        } else {
          return 0;
        }
      }
      
      @Override
      public long longVal(int doc) throws IOException {
        return getValueForDoc(doc);
      }

      @Override
      public boolean exists(int doc) throws IOException {
        getValueForDoc(doc);
        return arr.docID() == doc;
      }

      @Override
      public Object objectVal(int doc) throws IOException {
        long value = getValueForDoc(doc);
        if (arr.docID() == doc) {
          return longToObject(value);
        } else {
          return null;
        }
      }

      @Override
      public String strVal(int doc) throws IOException {
        long value = getValueForDoc(doc);
        if (arr.docID() == doc) {
          return longToString(value);
        } else {
          return null;
        }
      }

      @Override
      protected long externalToLong(String extVal) {
        return LongFieldSource.this.externalToLong(extVal);
      }

      @Override
      public ValueFiller getValueFiller() {
        return new ValueFiller() {
          private final MutableValueLong mval = newMutableValueLong();

          @Override
          public MutableValue getValue() {
            return mval;
          }

          @Override
          public void fillValue(int doc) throws IOException {
            mval.value = getValueForDoc(doc);
            mval.exists = arr.docID() == doc;
          }
        };
      }

    };
  }

  protected NumericDocValues getNumericDocValues(Map context, LeafReaderContext readerContext) throws IOException {
    return DocValues.getNumeric(readerContext.reader(), field);
  }

  protected MutableValueLong newMutableValueLong() {
    return new MutableValueLong();  
  }

  @Override
  public boolean equals(Object o) {
    if (o.getClass() != this.getClass()) return false;
    LongFieldSource other = (LongFieldSource) o;
    return super.equals(other);
  }

  @Override
  public int hashCode() {
    int h = getClass().hashCode();
    h += super.hashCode();
    return h;
  }
}
