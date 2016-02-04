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
 * <code>LinearFloatFunction</code> implements a linear function over
 * another {@link ValueSource}.
 * <br>
 * Normally Used as an argument to a {@link org.apache.lucene.queries.function.FunctionQuery}
 *
 *
 */
public class LinearFloatFunction extends ValueSource {
  protected final ValueSource source;
  protected final float slope;
  protected final float intercept;

  public LinearFloatFunction(ValueSource source, float slope, float intercept) {
    this.source = source;
    this.slope = slope;
    this.intercept = intercept;
  }
  
  @Override
  public String description() {
    return slope + "*float(" + source.description() + ")+" + intercept;
  }

  @Override
  public FunctionValues getValues(Map context, LeafReaderContext readerContext) throws IOException {
    final FunctionValues vals =  source.getValues(context, readerContext);
    return new FloatDocValues(this) {
      @Override
      public float floatVal(int doc) {
        return vals.floatVal(doc) * slope + intercept;
      }
      @Override
      public boolean exists(int doc) {
        return vals.exists(doc);
      }
      @Override
      public String toString(int doc) {
        return slope + "*float(" + vals.toString(doc) + ")+" + intercept;
      }
    };
  }

  @Override
  public void createWeight(Map context, IndexSearcher searcher) throws IOException {
    source.createWeight(context, searcher);
  }

  @Override
  public int hashCode() {
    int h = Float.floatToIntBits(slope);
    h = (h >>> 2) | (h << 30);
    h += Float.floatToIntBits(intercept);
    h ^= (h << 14) | (h >>> 19);
    return h + source.hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (LinearFloatFunction.class != o.getClass()) return false;
    LinearFloatFunction other = (LinearFloatFunction)o;
    return  this.slope == other.slope
         && this.intercept == other.intercept
         && this.source.equals(other.source);
  }
}
