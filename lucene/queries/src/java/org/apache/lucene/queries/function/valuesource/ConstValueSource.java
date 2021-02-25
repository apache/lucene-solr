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
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.docvalues.FloatDocValues;

/** <code>ConstValueSource</code> returns a constant for all documents */
public class ConstValueSource extends ConstNumberSource {
  final float constant;
  private final double dv;

  public ConstValueSource(float constant) {
    this.constant = constant;
    this.dv = constant;
  }

  @Override
  public String description() {
    return "const(" + constant + ")";
  }

  @Override
  public FunctionValues getValues(Map<Object, Object> context, LeafReaderContext readerContext)
      throws IOException {
    return new FloatDocValues(this) {
      @Override
      public float floatVal(int doc) {
        return constant;
      }

      @Override
      public int intVal(int doc) {
        return (int) constant;
      }

      @Override
      public long longVal(int doc) {
        return (long) constant;
      }

      @Override
      public double doubleVal(int doc) {
        return dv;
      }

      @Override
      public String toString(int doc) {
        return description();
      }

      @Override
      public Object objectVal(int doc) {
        return constant;
      }

      @Override
      public boolean boolVal(int doc) {
        return constant != 0.0f;
      }
    };
  }

  @Override
  public int hashCode() {
    return Float.floatToIntBits(constant) * 31;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof ConstValueSource)) return false;
    ConstValueSource other = (ConstValueSource) o;
    return this.constant == other.constant;
  }

  @Override
  public int getInt() {
    return (int) constant;
  }

  @Override
  public long getLong() {
    return (long) constant;
  }

  @Override
  public float getFloat() {
    return constant;
  }

  @Override
  public double getDouble() {
    return dv;
  }

  @Override
  public Number getNumber() {
    return constant;
  }

  @Override
  public boolean getBool() {
    return constant != 0.0f;
  }
}
