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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.docvalues.FloatDocValues;
import org.apache.lucene.search.IndexSearcher;

import java.io.IOException;
import java.util.Map;

/**
 * <code>RangeMapFloatFunction</code> implements a map function over
 * another {@link ValueSource} whose values fall within min and max inclusive to target.
 * <br>
 * Normally Used as an argument to a {@link org.apache.lucene.queries.function.FunctionQuery}
 *
 *
 */
public class RangeMapFloatFunction extends ValueSource {
  protected final ValueSource source;
  protected final float min;
  protected final float max;
  protected final ValueSource target;
  protected final ValueSource defaultVal;

  public RangeMapFloatFunction(ValueSource source, float min, float max, float target, Float def) {
    this(source, min, max, new ConstValueSource(target), def == null ? null : new ConstValueSource(def));
  }

  public RangeMapFloatFunction(ValueSource source, float min, float max, ValueSource target, ValueSource def) {
    this.source = source;
    this.min = min;
    this.max = max;
    this.target = target;
    this.defaultVal = def;
  }

  @Override
  public String description() {
    return "map(" + source.description() + "," + min + "," + max + "," + target.description() + "," + (defaultVal == null ? "null" : defaultVal.description()) + ")";
  }

  @Override
  public FunctionValues getValues(Map context, LeafReaderContext readerContext) throws IOException {
    final FunctionValues vals =  source.getValues(context, readerContext);
    final FunctionValues targets = target.getValues(context, readerContext);
    final FunctionValues defaults = (this.defaultVal == null) ? null : defaultVal.getValues(context, readerContext);
    return new FloatDocValues(this) {
      @Override
      public float floatVal(int doc) throws IOException {
        float val = vals.floatVal(doc);
        return (val>=min && val<=max) ? targets.floatVal(doc) : (defaultVal == null ? val : defaults.floatVal(doc));
      }
      @Override
      public String toString(int doc) throws IOException {
        return "map(" + vals.toString(doc) + ",min=" + min + ",max=" + max + ",target=" + targets.toString(doc) + ",defaultVal=" + (defaults == null ? "null" : (defaults.toString(doc))) + ")";
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
    h += target.hashCode();
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
         && this.target.equals(other.target)
         && this.source.equals(other.source)
         && (this.defaultVal == other.defaultVal || (this.defaultVal != null && this.defaultVal.equals(other.defaultVal)));
  }
}
