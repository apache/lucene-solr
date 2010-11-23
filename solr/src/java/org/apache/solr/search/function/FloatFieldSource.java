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

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.util.Bits;
import org.apache.lucene.search.cache.FloatValuesCreator;
import org.apache.lucene.search.cache.CachedArray.FloatValues;
import org.apache.solr.search.MutableValue;
import org.apache.solr.search.MutableValueFloat;

/**
 * Obtains float field values from the {@link org.apache.lucene.search.FieldCache}
 * using <code>getFloats()</code>
 * and makes those values available as other numeric types, casting as needed.
 *
 * @version $Id$
 */

public class FloatFieldSource extends NumericFieldCacheSource<FloatValues> {

  public FloatFieldSource(FloatValuesCreator creator) {
    super(creator);
  }

  public String description() {
    return "float(" + field + ')';
  }

  public DocValues getValues(Map context, IndexReader reader) throws IOException {
    final FloatValues vals = cache.getFloats(reader, field, creator);
    final float[] arr = vals.values;
	final Bits valid = vals.valid;
    
    return new DocValues() {
      public float floatVal(int doc) {
        return arr[doc];
      }

      public int intVal(int doc) {
        return (int)arr[doc];
      }

      public long longVal(int doc) {
        return (long)arr[doc];
      }

      public double doubleVal(int doc) {
        return (double)arr[doc];
      }

      public String strVal(int doc) {
        return Float.toString(arr[doc]);
      }

      public String toString(int doc) {
        return description() + '=' + floatVal(doc);
      }

      @Override
      public ValueFiller getValueFiller() {
        return new ValueFiller() {
          private final float[] floatArr = arr;
          private final MutableValueFloat mval = new MutableValueFloat();

          @Override
          public MutableValue getValue() {
            return mval;
          }

          @Override
          public void fillValue(int doc) {
            mval.value = floatArr[doc];
            mval.exists = valid.get(doc);
          }
        };
      }

    };
  }
}
