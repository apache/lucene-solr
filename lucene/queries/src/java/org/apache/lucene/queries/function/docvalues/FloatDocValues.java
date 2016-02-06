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

import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.util.mutable.MutableValue;
import org.apache.lucene.util.mutable.MutableValueFloat;

/**
 * Abstract {@link FunctionValues} implementation which supports retrieving float values.
 * Implementations can control how the float values are loaded through {@link #floatVal(int)}}
 */
public abstract class FloatDocValues extends FunctionValues {
  protected final ValueSource vs;

  public FloatDocValues(ValueSource vs) {
    this.vs = vs;
  }

  @Override
  public byte byteVal(int doc) {
    return (byte)floatVal(doc);
  }

  @Override
  public short shortVal(int doc) {
    return (short)floatVal(doc);
  }

  @Override
  public abstract float floatVal(int doc);

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
  public Object objectVal(int doc) {
    return exists(doc) ? floatVal(doc) : null;
  }

  @Override
  public String toString(int doc) {
    return vs.description() + '=' + strVal(doc);
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
        mval.value = floatVal(doc);
        mval.exists = exists(doc);
      }
    };
  }
}
