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
package org.apache.lucene.queries.function.docvalues;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.ValueSourceScorer;
import org.apache.lucene.util.mutable.MutableValue;
import org.apache.lucene.util.mutable.MutableValueInt;

/**
 * Abstract {@link FunctionValues} implementation which supports retrieving int values.
 * Implementations can control how the int values are loaded through {@link #intVal(int)}
 */
public abstract class IntDocValues extends FunctionValues {
  protected final ValueSource vs;

  public IntDocValues(ValueSource vs) {
    this.vs = vs;
  }

  @Override
  public byte byteVal(int doc) {
    return (byte)intVal(doc);
  }

  @Override
  public short shortVal(int doc) {
    return (short)intVal(doc);
  }

  @Override
  public float floatVal(int doc) {
    return (float)intVal(doc);
  }

  @Override
  public abstract int intVal(int doc);

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
  public Object objectVal(int doc) {
    return exists(doc) ? intVal(doc) : null;
  }

  @Override
  public String toString(int doc) {
    return vs.description() + '=' + strVal(doc);
  }
  
  @Override
  public ValueSourceScorer getRangeScorer(LeafReaderContext readerContext, String lowerVal, String upperVal, boolean includeLower, boolean includeUpper) {
    int lower,upper;

    // instead of using separate comparison functions, adjust the endpoints.

    if (lowerVal==null) {
      lower = Integer.MIN_VALUE;
    } else {
      lower = Integer.parseInt(lowerVal);
      if (!includeLower && lower < Integer.MAX_VALUE) lower++;
    }

     if (upperVal==null) {
      upper = Integer.MAX_VALUE;
    } else {
      upper = Integer.parseInt(upperVal);
      if (!includeUpper && upper > Integer.MIN_VALUE) upper--;
    }

    final int ll = lower;
    final int uu = upper;

    return new ValueSourceScorer(readerContext, this) {
      @Override
      public boolean matches(int doc) {
        if (!exists(doc)) return false;
        int val = intVal(doc);
        return val >= ll && val <= uu;
      }
    };
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
        mval.exists = exists(doc);
      }
    };
  }
}
