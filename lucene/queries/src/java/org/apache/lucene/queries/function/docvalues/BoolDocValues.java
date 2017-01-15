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

import java.io.IOException;

import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.util.mutable.MutableValue;
import org.apache.lucene.util.mutable.MutableValueBool;

/**
 * Abstract {@link FunctionValues} implementation which supports retrieving boolean values.
 * Implementations can control how the boolean values are loaded through {@link #boolVal(int)}}
 */
public abstract class BoolDocValues extends FunctionValues {
  protected final ValueSource vs;

  public BoolDocValues(ValueSource vs) {
    this.vs = vs;
  }

  @Override
  public abstract boolean boolVal(int doc) throws IOException;

  @Override
  public byte byteVal(int doc) throws IOException {
    return boolVal(doc) ? (byte)1 : (byte)0;
  }

  @Override
  public short shortVal(int doc) throws IOException {
    return boolVal(doc) ? (short)1 : (short)0;
  }

  @Override
  public float floatVal(int doc) throws IOException {
    return boolVal(doc) ? (float)1 : (float)0;
  }

  @Override
  public int intVal(int doc) throws IOException {
    return boolVal(doc) ? 1 : 0;
  }

  @Override
  public long longVal(int doc) throws IOException {
    return boolVal(doc) ? (long)1 : (long)0;
  }

  @Override
  public double doubleVal(int doc) throws IOException {
    return boolVal(doc) ? (double)1 : (double)0;
  }

  @Override
  public String strVal(int doc) throws IOException {
    return Boolean.toString(boolVal(doc));
  }

  @Override
  public Object objectVal(int doc) throws IOException {
    return exists(doc) ? boolVal(doc) : null;
  }

  @Override
  public String toString(int doc) throws IOException {
    return vs.description() + '=' + strVal(doc);
  }

  @Override
  public ValueFiller getValueFiller() {
    return new ValueFiller() {
      private final MutableValueBool mval = new MutableValueBool();

      @Override
      public MutableValue getValue() {
        return mval;
      }

      @Override
      public void fillValue(int doc) throws IOException {
        mval.value = boolVal(doc);
        mval.exists = exists(doc);
      }
    };
  }
}
