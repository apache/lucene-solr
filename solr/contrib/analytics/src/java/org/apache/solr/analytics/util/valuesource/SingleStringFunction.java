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

package org.apache.solr.analytics.util.valuesource;

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.docvalues.StrDocValues;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.mutable.MutableValue;
import org.apache.lucene.util.mutable.MutableValueStr;

/**
 * Abstract {@link ValueSource} implementation which wraps one ValueSource
 * and applies an extendible string function to its values.
 */
public abstract class SingleStringFunction extends ValueSource {
  protected final ValueSource source;
  
  public SingleStringFunction(ValueSource source) {
    this.source = source;
  }

  @Override
  public String description() {
    return name()+"("+source.description()+")";
  }

  abstract String name();
  abstract CharSequence func(int doc, FunctionValues vals);

  @Override
  public FunctionValues getValues(Map context, LeafReaderContext readerContext) throws IOException {
    final FunctionValues vals =  source.getValues(context, readerContext);
    return new StrDocValues(this) {
      @Override
      public String strVal(int doc) {
        CharSequence cs = func(doc, vals);
        return cs != null ? cs.toString() : null;
      }
      
      @Override
      public boolean bytesVal(int doc, BytesRefBuilder bytes) {
        CharSequence cs = func(doc, vals);
        if( cs != null ){
          bytes.copyChars(func(doc,vals));
          return true;
        } else {
          bytes.clear();
          return false;
        }
      }

      @Override
      public Object objectVal(int doc) {
        return strVal(doc);
      }
      
      @Override
      public boolean exists(int doc) {
        return vals.exists(doc);
      }

      @Override
      public String toString(int doc) {
        return name() + '(' + strVal(doc) + ')';
      }

      @Override
      public ValueFiller getValueFiller() {
        return new ValueFiller() {
          private final MutableValueStr mval = new MutableValueStr();

          @Override
          public MutableValue getValue() {
            return mval;
          }

          @Override
          public void fillValue(int doc) {
            mval.exists = bytesVal(doc, mval.value);
          }
        };
      }
    };
  }

  @Override
  public boolean equals(Object o) {
    if (getClass() != o.getClass()) return false;
    SingleStringFunction other = (SingleStringFunction)o;
    return this.source.equals(other.source);
  }

  @Override
  public int hashCode() {
    return source.hashCode()+name().hashCode();
  }

}
