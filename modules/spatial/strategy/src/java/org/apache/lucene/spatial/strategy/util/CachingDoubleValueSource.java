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

package org.apache.lucene.spatial.strategy.util;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class CachingDoubleValueSource extends ValueSource {

  final ValueSource source;
  final Map<Integer, Double> cache;

  public CachingDoubleValueSource( ValueSource source )
  {
    this.source = source;
    cache = new HashMap<Integer, Double>();
  }

  @Override
  public String description() {
    return "Cached["+source.description()+"]";
  }

  @Override
  public FunctionValues getValues(Map context, AtomicReaderContext readerContext) throws IOException {
    final int base = readerContext.docBase;
    final FunctionValues vals = source.getValues(context,readerContext);
    return new FunctionValues() {

      @Override
      public double doubleVal(int doc) {
        Integer key = Integer.valueOf( base+doc );
        Double v = cache.get( key );
        if( v == null ) {
          v = Double.valueOf( vals.doubleVal(doc) );
          cache.put( key, v );
        }
        return v.doubleValue();
      }

      @Override
      public float floatVal(int doc) {
        return (float)doubleVal(doc);
      }

      @Override
      public String toString(int doc) {
        return doubleVal(doc)+"";
      }
    };
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) { return false; }
    if (obj == this) { return true; }
    if (obj.getClass() != getClass()) {
      return false;
    }
    CachingDoubleValueSource rhs = (CachingDoubleValueSource) obj;
    return source.equals( rhs.source );
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(61, 23).
      append(source).
      toHashCode();
  }
}
