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

import org.apache.lucene.index.IndexReader;

import java.io.IOException;
import java.util.Map;

/**
 * <code>ConstValueSource</code> returns a constant for all documents
 */
public class ConstValueSource extends ValueSource {
  final float constant;

  public ConstValueSource(float constant) {
    this.constant = constant;
  }

  public String description() {
    return "const(" + constant + ")";
  }

  public DocValues getValues(Map context, IndexReader reader) throws IOException {
    return new DocValues() {
      public float floatVal(int doc) {
        return constant;
      }
      public int intVal(int doc) {
        return (int)floatVal(doc);
      }
      public long longVal(int doc) {
        return (long)floatVal(doc);
      }
      public double doubleVal(int doc) {
        return (double)floatVal(doc);
      }
      public String strVal(int doc) {
        return Float.toString(floatVal(doc));
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
    if (ConstValueSource.class != o.getClass()) return false;
    ConstValueSource other = (ConstValueSource)o;
    return  this.constant == other.constant;
  }
}
