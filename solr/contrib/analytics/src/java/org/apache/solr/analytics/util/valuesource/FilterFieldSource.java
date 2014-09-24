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
import java.util.Date;
import java.util.Map;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.util.mutable.MutableValue;
import org.apache.solr.analytics.util.AnalyticsParams;
import org.apache.solr.schema.TrieDateField;

/**
 * <code>DefaultIsMissingFieldSource</code> wraps a field source to return missing values 
 * if the value is equal to the default value.
 */
public class FilterFieldSource extends ValueSource {
  public final static String NAME = AnalyticsParams.FILTER;
  public final Object missValue;
  protected final ValueSource source;
  
  public FilterFieldSource(ValueSource source, Object missValue) {
    this.source = source;
    this.missValue = missValue;
  }

  protected String name() {
    return NAME;
  }

  @SuppressWarnings("deprecation")
  @Override
  public String description() {
    if (missValue.getClass().equals(Date.class)) {
      return name()+"("+source.description()+","+TrieDateField.formatExternal((Date)missValue)+")";
    } else {
      return name()+"("+source.description()+","+missValue.toString()+")";
    }
  }

  @Override
  public FunctionValues getValues(Map context, LeafReaderContext readerContext) throws IOException {
    final FunctionValues vals =  source.getValues(context, readerContext);
    return new FunctionValues() {

      @Override
      public byte byteVal(int doc) {
        return vals.byteVal(doc);
      }

      @Override
      public short shortVal(int doc) {
        return vals.shortVal(doc);
      }

      @Override
      public float floatVal(int doc) {
        return vals.floatVal(doc);
      }

      @Override
      public int intVal(int doc) {
        return vals.intVal(doc);
      }

      @Override
      public long longVal(int doc) {
        return vals.longVal(doc);
      }

      @Override
      public double doubleVal(int doc) {
        return vals.doubleVal(doc);
      }

      @Override
      public String strVal(int doc) {
        return vals.strVal(doc);
      }

      @Override
      public Object objectVal(int doc) {
        return exists(doc)? vals.objectVal(doc) : null;
      }

      @Override
      public boolean exists(int doc) {
        Object other = vals.objectVal(doc);
        return other!=null&&!missValue.equals(other);
      }

      @Override
      public String toString(int doc) {
        return NAME + '(' + vals.toString(doc) + ')';
      }

      @Override
      public ValueFiller getValueFiller() {
        return new ValueFiller() {
          private final ValueFiller delegateFiller = vals.getValueFiller();
          private final MutableValue mval = delegateFiller.getValue();

          @Override
          public MutableValue getValue() {
            return mval;
          }

          @Override
          public void fillValue(int doc) {
            delegateFiller.fillValue(doc);
            mval.exists = exists(doc);
          }
        };
      }
    };
  }
  
  public ValueSource getRootSource() {
    if (source instanceof FilterFieldSource) {
      return ((FilterFieldSource)source).getRootSource();
    } else {
      return source;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (getClass() != o.getClass()) return false;
    FilterFieldSource other = (FilterFieldSource)o;
    return this.source.equals(other.source) && this.missValue.equals(other.missValue);
  }

  @Override
  public int hashCode() {
    return source.hashCode()+name().hashCode();
  }

}
