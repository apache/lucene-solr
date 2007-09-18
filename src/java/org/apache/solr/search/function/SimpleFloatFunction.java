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

/** A simple float function with a single argument
 */
 public abstract class SimpleFloatFunction extends ValueSource {
  protected final ValueSource source;

  public SimpleFloatFunction(ValueSource source) {
    this.source = source;
  }

  protected abstract String name();
  protected abstract float func(int doc, DocValues vals);

  public String description() {
    return name() + '(' + source.description() + ')';
  }

  public DocValues getValues(IndexReader reader) throws IOException {
    final DocValues vals =  source.getValues(reader);
    return new DocValues() {
      public float floatVal(int doc) {
	return func(doc, vals);
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
	return name() + '(' + vals.toString(doc) + ')';
      }
    };
  }

  public int hashCode() {
    return source.hashCode() + name().hashCode();
  }

  public boolean equals(Object o) {
    if (this.getClass() != o.getClass()) return false;
    SimpleFloatFunction other = (SimpleFloatFunction)o;
    return this.name().equals(other.name())
         && this.source.equals(other.source);
  }
}
