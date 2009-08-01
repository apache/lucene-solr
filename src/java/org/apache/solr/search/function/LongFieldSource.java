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
import org.apache.lucene.search.FieldCache;


import java.io.IOException;

/**
 * Obtains float field values from the {@link org.apache.lucene.search.FieldCache}
 * using <code>getFloats()</code>
 * and makes those values available as other numeric types, casting as needed.
 *
 * @version $Id: FloatFieldSource.java 555343 2007-07-11 17:46:25Z hossman $
 */

public class LongFieldSource extends FieldCacheSource {
  protected FieldCache.LongParser parser;

  public LongFieldSource(String field) {
    this(field, null);
  }

  public LongFieldSource(String field, FieldCache.LongParser parser) {
    super(field);
    this.parser = parser;
  }

  public String description() {
    return "long(" + field + ')';
  }

  public DocValues getValues(IndexReader reader) throws IOException {
    final long[] arr = (parser == null) ?
            ((FieldCache) cache).getLongs(reader, field) :
            ((FieldCache) cache).getLongs(reader, field, parser);
    return new DocValues() {
      public float floatVal(int doc) {
        return (float) arr[doc];
      }

      public int intVal(int doc) {
        return (int) arr[doc];
      }

      public long longVal(int doc) {
        return (long) arr[doc];
      }

      public double doubleVal(int doc) {
        return arr[doc];
      }

      public String strVal(int doc) {
        return Long.toString(arr[doc]);
      }

      public String toString(int doc) {
        return description() + '=' + longVal(doc);
      }


      @Override
      public ValueSourceScorer getRangeScorer(IndexReader reader, String lowerVal, String upperVal, boolean includeLower, boolean includeUpper) {
        long lower,upper;

        // instead of using separate comparison functions, adjust the endpoints.

        if (lowerVal==null) {
          lower = Long.MIN_VALUE;
        } else {
          lower = Long.parseLong(lowerVal);
          if (!includeLower && lower < Long.MAX_VALUE) lower++;
        }

         if (upperVal==null) {
          upper = Long.MAX_VALUE;
        } else {
          upper = Long.parseLong(upperVal);
          if (!includeUpper && upper > Long.MIN_VALUE) upper--;
        }

        final long ll = lower;
        final long uu = upper;

        return new ValueSourceScorer(reader, this) {
          @Override
          public boolean matchesValue(int doc) {
            long val = arr[doc];
            // only check for deleted if it's the default value
            // if (val==0 && reader.isDeleted(doc)) return false;
            return val >= ll && val <= uu;
          }
        };
      }


    };
  }

  public boolean equals(Object o) {
    if (o.getClass() != LongFieldSource.class) return false;
    LongFieldSource other = (LongFieldSource) o;
    return super.equals(other)
            && this.parser == null ? other.parser == null :
            this.parser.getClass() == other.parser.getClass();
  }

  public int hashCode() {
    int h = parser == null ? Long.class.hashCode() : parser.getClass().hashCode();
    h += super.hashCode();
    return h;
  }

}
