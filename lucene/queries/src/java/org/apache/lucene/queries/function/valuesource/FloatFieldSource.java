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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.docvalues.FloatDocValues;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.mutable.MutableValue;
import org.apache.lucene.util.mutable.MutableValueFloat;

/**
 * Obtains float field values from the {@link org.apache.lucene.search.FieldCache}
 * using <code>getFloats()</code>
 * and makes those values available as other numeric types, casting as needed.
 *
 *
 */

public class FloatFieldSource extends FieldCacheSource {

  protected final FieldCache.FloatParser parser;

  public FloatFieldSource(String field) {
    this(field, null);
  }

  public FloatFieldSource(String field, FieldCache.FloatParser parser) {
    super(field);
    this.parser = parser;
  }

  @Override
  public String description() {
    return "float(" + field + ')';
  }

  @Override
  public FunctionValues getValues(Map context, AtomicReaderContext readerContext) throws IOException {
    final FieldCache.Floats arr = cache.getFloats(readerContext.reader(), field, parser, true);
    final Bits valid = cache.getDocsWithField(readerContext.reader(), field);

    return new FloatDocValues(this) {
      @Override
      public float floatVal(int doc) {
        return arr.get(doc);
      }

      @Override
      public Object objectVal(int doc) {
        return valid.get(doc) ? arr.get(doc) : null;
      }

      @Override
      public boolean exists(int doc) {
        return valid.get(doc);
      }

      @Override
      public ValueFiller getValueFiller() {
        return new ValueFiller() {
          private final MutableValueFloat mval = new MutableValueFloat();

          @Override
          public MutableValue getValue() {
            return mval;
          }

          @Override
          public void fillValue(int doc) {
            mval.value = arr.get(doc);
            mval.exists = valid.get(doc);
          }
        };
      }

    };
  }

  @Override
  public boolean equals(Object o) {
    if (o.getClass() !=  FloatFieldSource.class) return false;
    FloatFieldSource other = (FloatFieldSource)o;
    return super.equals(other)
      && (this.parser==null ? other.parser==null :
          this.parser.getClass() == other.parser.getClass());
  }

  @Override
  public int hashCode() {
    int h = parser==null ? Float.class.hashCode() : parser.getClass().hashCode();
    h += super.hashCode();
    return h;
  }
}
