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
import org.apache.lucene.search.Searcher;

import java.io.IOException;
import java.util.Map;

public abstract class DualFloatFunction extends ValueSource {
  protected final ValueSource a;
  protected final ValueSource b;

 /**
   * @param   a  the base.
   * @param   b  the exponent.
   */
  public DualFloatFunction(ValueSource a, ValueSource b) {
    this.a = a;
    this.b = b;
  }

  protected abstract String name();
  protected abstract float func(int doc, DocValues aVals, DocValues bVals);

  public String description() {
    return name() + "(" + a.description() + "," + b.description() + ")";
  }

  public DocValues getValues(Map context, IndexReader reader) throws IOException {
    final DocValues aVals =  a.getValues(context, reader);
    final DocValues bVals =  b.getValues(context, reader);
    return new DocValues() {
      public float floatVal(int doc) {
	return func(doc, aVals, bVals);
      }
      public int intVal(int doc) {
        return (int)floatVal(doc);
      }
      public long longVal(int doc) {
        return (long)floatVal(doc);
      }
      public double doubleVal(int doc) {
        return floatVal(doc);
      }
      public String strVal(int doc) {
        return Float.toString(floatVal(doc));
      }
      public String toString(int doc) {
	return name() + '(' + aVals.toString(doc) + ',' + bVals.toString(doc) + ')';
      }
    };
  }

  @Override
  public void createWeight(Map context, Searcher searcher) throws IOException {
    a.createWeight(context,searcher);
    b.createWeight(context,searcher);
  }

  public int hashCode() {
    int h = a.hashCode();
    h ^= (h << 13) | (h >>> 20);
    h += b.hashCode();
    h ^= (h << 23) | (h >>> 10);
    h += name().hashCode();
    return h;
  }

  public boolean equals(Object o) {
    if (this.getClass() != o.getClass()) return false;
    DualFloatFunction other = (DualFloatFunction)o;
    return this.a.equals(other.a)
        && this.b.equals(other.b);
  }
}
