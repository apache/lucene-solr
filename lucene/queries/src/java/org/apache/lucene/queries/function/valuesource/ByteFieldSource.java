package org.apache.lucene.queries.function.valuesource;
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

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.search.FieldCache;

/**
 * Obtains int field values from the {@link org.apache.lucene.search.FieldCache}
 * using <code>getInts()</code>
 * and makes those values available as other numeric types, casting as needed. *
 *
 *
 */

public class ByteFieldSource extends FieldCacheSource {

  private final FieldCache.ByteParser parser;

  public ByteFieldSource(String field) {
    this(field, null);
  }

  public ByteFieldSource(String field, FieldCache.ByteParser parser) {
    super(field);
    this.parser = parser;
  }

  @Override
  public String description() {
    return "byte(" + field + ')';
  }

  @Override
  public FunctionValues getValues(Map context, AtomicReaderContext readerContext) throws IOException {
    final FieldCache.Bytes arr = cache.getBytes(readerContext.reader(), field, parser, false);
    
    return new FunctionValues() {
      @Override
      public byte byteVal(int doc) {
        return arr.get(doc);
      }

      @Override
      public short shortVal(int doc) {
        return (short) arr.get(doc);
      }

      @Override
      public float floatVal(int doc) {
        return (float) arr.get(doc);
      }

      @Override
      public int intVal(int doc) {
        return (int) arr.get(doc);
      }

      @Override
      public long longVal(int doc) {
        return (long) arr.get(doc);
      }

      @Override
      public double doubleVal(int doc) {
        return (double) arr.get(doc);
      }

      @Override
      public String strVal(int doc) {
        return Byte.toString(arr.get(doc));
      }

      @Override
      public String toString(int doc) {
        return description() + '=' + byteVal(doc);
      }

      @Override
      public Object objectVal(int doc) {
        return arr.get(doc);  // TODO: valid?
      }

    };
  }

  @Override
  public boolean equals(Object o) {
    if (o.getClass() != ByteFieldSource.class) return false;
    ByteFieldSource
            other = (ByteFieldSource) o;
    return super.equals(other)
      && (this.parser == null ? other.parser == null :
          this.parser.getClass() == other.parser.getClass());
  }

  @Override
  public int hashCode() {
    int h = parser == null ? Byte.class.hashCode() : parser.getClass().hashCode();
    h += super.hashCode();
    return h;
  }
}
