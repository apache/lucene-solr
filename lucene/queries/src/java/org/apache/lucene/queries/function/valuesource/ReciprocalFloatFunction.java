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
 * <code>ReciprocalFloatFunction</code> implements a reciprocal function f(x) = a/(mx+b), based on
 * the float value of a field or function as exported by {@link org.apache.lucene.queries.function.ValueSource}.
 * <br>
 *
 * When a and b are equal, and x&gt;=0, this function has a maximum value of 1 that drops as x increases.
 * Increasing the value of a and b together results in a movement of the entire function to a flatter part of the curve.
 * <p>These properties make this an idea function for boosting more recent documents.
 * <p>Example:<code>  recip(ms(NOW,mydatefield),3.16e-11,1,1)</code>
 * <p>A multiplier of 3.16e-11 changes the units from milliseconds to years (since there are about 3.16e10 milliseconds
 * per year).  Thus, a very recent date will yield a value close to 1/(0+1) or 1,
 * a date a year in the past will get a multiplier of about 1/(1+1) or 1/2,
 * and date two years old will yield 1/(2+1) or 1/3.
 *
 * @see org.apache.lucene.queries.function.FunctionQuery
 *
 *
 */
public class ReciprocalFloatFunction extends ValueSource {
  protected final ValueSource source;
  protected final float m;
  protected final float a;
  protected final float b;

  /**
   *  f(source) = a/(m*float(source)+b)
   */
  public ReciprocalFloatFunction(ValueSource source, float m, float a, float b) {
    this.source=source;
    this.m=m;
    this.a=a;
    this.b=b;
  }

  @Override
  public FunctionValues getValues(Map context, LeafReaderContext readerContext) throws IOException {
    final FunctionValues vals = source.getValues(context, readerContext);
    return new FloatDocValues(this) {
      @Override
      public float floatVal(int doc) {
        return a/(m*vals.floatVal(doc) + b);
      }
      @Override
      public boolean exists(int doc) {
        return vals.exists(doc);
      }
      @Override
      public String toString(int doc) {
        return Float.toString(a) + "/("
                + m + "*float(" + vals.toString(doc) + ')'
                + '+' + b + ')';
      }
    };
  }

  @Override
  public void createWeight(Map context, IndexSearcher searcher) throws IOException {
    source.createWeight(context, searcher);
  }

  @Override
  public String description() {
    return Float.toString(a) + "/("
           + m + "*float(" + source.description() + ")"
           + "+" + b + ')';
  }

  @Override
  public int hashCode() {
    int h = Float.floatToIntBits(a) + Float.floatToIntBits(m);
    h ^= (h << 13) | (h >>> 20);
    return h + (Float.floatToIntBits(b)) + source.hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (ReciprocalFloatFunction.class != o.getClass()) return false;
    ReciprocalFloatFunction other = (ReciprocalFloatFunction)o;
    return this.m == other.m
            && this.a == other.a
            && this.b == other.b
            && this.source.equals(other.source);
  }
}
