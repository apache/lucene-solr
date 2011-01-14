/**
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

package org.apache.solr.search.function;

import org.apache.lucene.index.IndexReader.AtomicReaderContext;

import java.io.IOException;
import java.util.Map;

public class DoubleConstValueSource extends ConstNumberSource {
  final double constant;
  private final float fv;
  private final long lv;

  public DoubleConstValueSource(double constant) {
    this.constant = constant;
    this.fv = (float)constant;
    this.lv = (long)constant;
  }

  public String description() {
    return "const(" + constant + ")";
  }

  public DocValues getValues(Map context, AtomicReaderContext readerContext) throws IOException {
    return new DocValues() {
      public float floatVal(int doc) {
        return fv;
      }

      public int intVal(int doc) {
        return (int) lv;
      }

      public long longVal(int doc) {
        return lv;
      }

      public double doubleVal(int doc) {
        return constant;
      }

      public String strVal(int doc) {
        return Double.toString(constant);
      }

      public String toString(int doc) {
        return description();
      }
    };
  }

  public int hashCode() {
    long bits = Double.doubleToRawLongBits(constant);
    return (int)(bits ^ (bits >>> 32));
  }

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
}
