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

import org.apache.lucene.search.FieldCache;
import org.apache.lucene.index.IndexReader;

import java.io.IOException;
import java.util.Map;


/**
 *
 *
 **/
public class ShortFieldSource extends FieldCacheSource{
  FieldCache.ShortParser parser;

  public ShortFieldSource(String field) {
    this(field, null);
  }

  public ShortFieldSource(String field, FieldCache.ShortParser parser) {
    super(field);
    this.parser = parser;
  }

  @Override
  public String description() {
    return "short(" + field + ')';
  }

  @Override
  public DocValues getValues(Map context, IndexReader reader) throws IOException {
    final short[] arr = (parser == null) ?
            cache.getShorts(reader, field) :
            cache.getShorts(reader, field, parser);
    return new DocValues() {
      @Override
      public byte byteVal(int doc) {
        return (byte) arr[doc];
      }

      @Override
      public short shortVal(int doc) {
        return arr[doc];
      }

      @Override
      public float floatVal(int doc) {
        return (float) arr[doc];
      }

      @Override
      public int intVal(int doc) {
        return (int) arr[doc];
      }

      @Override
      public long longVal(int doc) {
        return (long) arr[doc];
      }

      @Override
      public double doubleVal(int doc) {
        return (double) arr[doc];
      }

      @Override
      public String strVal(int doc) {
        return Short.toString(arr[doc]);
      }

      @Override
      public String toString(int doc) {
        return description() + '=' + shortVal(doc);
      }

    };
  }

  @Override
  public boolean equals(Object o) {
    if (o.getClass() != ShortFieldSource.class) return false;
    ShortFieldSource
            other = (ShortFieldSource) o;
    return super.equals(other)
            && this.parser == null ? other.parser == null :
            this.parser.getClass() == other.parser.getClass();
  }

  @Override
  public int hashCode() {
    int h = parser == null ? Short.class.hashCode() : parser.getClass().hashCode();
    h += super.hashCode();
    return h;
  }
}
