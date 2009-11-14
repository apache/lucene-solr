package org.apache.solr.search.function;
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

import org.apache.lucene.index.IndexReader;

import java.io.IOException;
import java.util.Map;


/**
 * Take a ValueSourc and produce convert the number to radians and
 * return that value
 */
public class RadianFunction extends ValueSource {
  protected ValueSource valSource;

  public RadianFunction(ValueSource valSource) {
    this.valSource = valSource;
  }

  public String description() {
    return "rad(" + valSource.description() + ')';
  }

  public DocValues getValues(Map context, IndexReader reader) throws IOException {
    final DocValues dv = valSource.getValues(context, reader);
    return new DocValues() {
      public float floatVal(int doc) {
        return (float) doubleVal(doc);
      }

      public int intVal(int doc) {
        return (int) doubleVal(doc);
      }

      public long longVal(int doc) {
        return (long) doubleVal(doc);
      }

      public double doubleVal(int doc) {
        return Math.toRadians(dv.doubleVal(doc));
      }

      public String strVal(int doc) {
        return Double.toString(doubleVal(doc));
      }

      public String toString(int doc) {
        return description() + '=' + floatVal(doc);
      }
    };
  }

  public boolean equals(Object o) {
    if (o.getClass() != RadianFunction.class) return false;
    RadianFunction other = (RadianFunction) o;
    return description().equals(other.description()) && valSource.equals(other.valSource);
  }

  public int hashCode() {
    return description().hashCode() + valSource.hashCode();
  };

}
