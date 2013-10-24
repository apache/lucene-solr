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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.docvalues.DoubleDocValues;

import java.io.IOException;
import java.util.Map;

/**
 * Function that returns a constant double value for every document.
 */
public class DoubleConstValueSource extends ConstNumberSource {
  final double constant;
  private final float fv;
  private final long lv;

  public DoubleConstValueSource(double constant) {
    this.constant = constant;
    this.fv = (float)constant;
    this.lv = (long)constant;
  }

  @Override
  public String description() {
    return "const(" + constant + ")";
  }

  @Override
  public FunctionValues getValues(Map context, AtomicReaderContext readerContext) throws IOException {
    return new DoubleDocValues(this) {
      @Override
      public float floatVal(int doc) {
        return fv;
      }

      @Override
      public int intVal(int doc) {
        return (int) lv;
      }

      @Override
      public long longVal(int doc) {
        return lv;
      }

      @Override
      public double doubleVal(int doc) {
        return constant;
      }

      @Override
      public String strVal(int doc) {
        return Double.toString(constant);
      }

      @Override
      public Object objectVal(int doc) {
        return constant;
      }

      @Override
      public String toString(int doc) {
        return description();
      }
    };
  }

  @Override
  public int hashCode() {
    long bits = Double.doubleToRawLongBits(constant);
    return (int)(bits ^ (bits >>> 32));
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof DoubleConstValueSource)) return false;
    DoubleConstValueSource other = (DoubleConstValueSource) o;
    return this.constant == other.constant;
  }

  @Override
  public int getInt() {
    return (int)lv;
  }

  @Override
  public long getLong() {
    return lv;
  }

  @Override
  public float getFloat() {
    return fv;
  }

  @Override
  public double getDouble() {
    return constant;
  }

  @Override
  public Number getNumber() {
    return constant;
  }

  @Override
  public boolean getBool() {
    return constant != 0;
  }
}
