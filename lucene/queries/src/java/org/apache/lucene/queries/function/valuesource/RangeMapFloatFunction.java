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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.docvalues.FloatDocValues;
import org.apache.lucene.search.IndexSearcher;

import java.io.IOException;
import java.util.Map;

/**
 * <code>LinearFloatFunction</code> implements a linear function over
 * another {@link org.apache.lucene.queries.function.ValueSource}.
 * <br>
 * Normally Used as an argument to a {@link org.apache.lucene.queries.function.FunctionQuery}
 *
 *
 */
public class RangeMapFloatFunction extends ValueSource {
  protected final ValueSource source;
  protected final float min;
  protected final float max;
  protected final float target;
  protected final Float defaultVal;

  public RangeMapFloatFunction(ValueSource source, float min, float max, float target, Float def) {
    this.source = source;
    this.min = min;
    this.max = max;
    this.target = target;
    this.defaultVal = def;
  }

  @Override
  public String description() {
    return "map(" + source.description() + "," + min + "," + max + "," + target + ")";
  }

  @Override
  public FunctionValues getValues(Map context, AtomicReaderContext readerContext) throws IOException {
    final FunctionValues vals =  source.getValues(context, readerContext);
    return new FloatDocValues(this) {
      @Override
      public float floatVal(int doc) {
        float val = vals.floatVal(doc);
        return (val>=min && val<=max) ? target : (defaultVal == null ? val : defaultVal);
      }
      @Override
      public String toString(int doc) {
        return "map(" + vals.toString(doc) + ",min=" + min + ",max=" + max + ",target=" + target + ")";
      }
    };
  }

  @Override
  public void createWeight(Map context, IndexSearcher searcher) throws IOException {
    source.createWeight(context, searcher);
  }

  @Override
  public int hashCode() {
    int h = source.hashCode();
    h ^= (h << 10) | (h >>> 23);
    h += Float.floatToIntBits(min);
    h ^= (h << 14) | (h >>> 19);
    h += Float.floatToIntBits(max);
    h ^= (h << 13) | (h >>> 20);
    h += Float.floatToIntBits(target);
    if (defaultVal != null)
      h += defaultVal.hashCode();
    return h;
  }

  @Override
  public boolean equals(Object o) {
    if (RangeMapFloatFunction.class != o.getClass()) return false;
    RangeMapFloatFunction other = (RangeMapFloatFunction)o;
    return  this.min == other.min
         && this.max == other.max
         && this.target == other.target
         && this.source.equals(other.source)
         && (this.defaultVal == other.defaultVal || (this.defaultVal != null && this.defaultVal.equals(other.defaultVal)));
  }
}
