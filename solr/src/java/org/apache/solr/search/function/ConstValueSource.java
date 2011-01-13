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

/**
 * <code>ConstValueSource</code> returns a constant for all documents
 */
public class ConstValueSource extends ConstNumberSource {
  final float constant;
  private final double dv;

  public ConstValueSource(float constant) {
    this.constant = constant;
    this.dv = constant;
  }

  public String description() {
    return "const(" + constant + ")";
  }

  public DocValues getValues(Map context, AtomicReaderContext readerContext) throws IOException {
    return new DocValues() {
      public float floatVal(int doc) {
        return constant;
      }
      public int intVal(int doc) {
        return (int)constant;
      }
      public long longVal(int doc) {
        return (long)constant;
      }
      public double doubleVal(int doc) {
        return dv;
      }
      public String strVal(int doc) {
        return Float.toString(constant);
      }
      public String toString(int doc) {
        return description();
      }
    };
  }

  public int hashCode() {
    return Float.floatToIntBits(constant) * 31;
  }

  public boolean equals(Object o) {
    if (!(o instanceof ConstValueSource)) return false;
    ConstValueSource other = (ConstValueSource)o;
    return  this.constant == other.constant;
  }

  @Override
  public int getInt() {
    return (int)constant;
  }

  @Override
  public long getLong() {
    return (long)constant;
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
}
