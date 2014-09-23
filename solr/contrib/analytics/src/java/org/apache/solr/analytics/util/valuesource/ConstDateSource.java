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
import java.text.ParseException;
import java.util.Date;
import java.util.Map;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.docvalues.FloatDocValues;
import org.apache.lucene.util.mutable.MutableValue;
import org.apache.lucene.util.mutable.MutableValueDate;
import org.apache.solr.analytics.util.AnalyticsParams;
import org.apache.solr.schema.TrieDateField;

/**
 * <code>ConstDateSource</code> returns a constant date for all documents
 */
public class ConstDateSource extends ConstDoubleSource {
  public final static String NAME = AnalyticsParams.CONSTANT_DATE;

  public ConstDateSource(Date constant) throws ParseException {
    super(constant.getTime());
  }

  public ConstDateSource(Long constant) {
    super(constant);
  }

  @SuppressWarnings("deprecation")
  @Override
  public String description() {
    return name()+"(" + TrieDateField.formatExternal(new Date(getLong())) + ")";
  }

  protected String name() {
    return NAME;
  }
  
  @Override
  public FunctionValues getValues(Map context, LeafReaderContext readerContext) throws IOException {
    return new FloatDocValues(this) {
      @Override
      public float floatVal(int doc) {
        return getFloat();
      }
      @Override
      public int intVal(int doc) {
        return getInt();
      }
      @Override
      public long longVal(int doc) {
        return getLong();
      }
      @Override
      public double doubleVal(int doc) {
        return getDouble();
      }
      @Override
      public String toString(int doc) {
        return description();
      }
      @Override
      public Object objectVal(int doc) {
        return new Date(longVal(doc));
      }
      @SuppressWarnings("deprecation")
      @Override
      public String strVal(int doc) {
        return TrieDateField.formatExternal(new Date(longVal(doc)));
      }
      @Override
      public boolean boolVal(int doc) {
        return getFloat() != 0.0f;
      }

      @Override
      public ValueFiller getValueFiller() {
        return new ValueFiller() {
          private final MutableValueDate mval = new MutableValueDate();

          @Override
          public MutableValue getValue() {
            return mval;
          }

          @Override
          public void fillValue(int doc) {
            mval.value = longVal(doc);
            mval.exists = true;
          }
        };
      }
    };
  }

}
