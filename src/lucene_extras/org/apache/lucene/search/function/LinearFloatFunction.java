/**
 * Copyright 2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.lucene.search.function;

import org.apache.lucene.index.IndexReader;

import java.io.IOException;

/**
 * <code>LinearFloatFunction</code> implements a linear function over
 * another {@link ValueSource}.
 * <br>
 * Normally Used as an argument to a {@link FunctionQuery}
 *
 * @author yonik
 * @version $Id: LinearFloatFunction.java,v 1.2 2005/11/22 05:23:21 yonik Exp $
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
  
  public String description() {
    return slope + "*float(" + source.description() + ")+" + intercept;
  }

  public DocValues getValues(IndexReader reader) throws IOException {
    final DocValues vals =  source.getValues(reader);
    return new DocValues() {
      public float floatVal(int doc) {
        return vals.floatVal(doc) * slope + intercept;
      }
      public int intVal(int doc) {
        return (int)floatVal(doc);
      }
      public long longVal(int doc) {
        return (long)floatVal(doc);
      }
      public double doubleVal(int doc) {
        return (double)floatVal(doc);
      }
      public String strVal(int doc) {
        return Float.toString(floatVal(doc));
      }
      public String toString(int doc) {
        return slope + "*float(" + vals.toString(doc) + ")+" + intercept;
      }
    };
  }

  public int hashCode() {
    int h = Float.floatToIntBits(slope);
    h = (h >>> 2) | (h << 30);
    h += Float.floatToIntBits(intercept);
    h ^= (h << 14) | (h >>> 19);
    return h + source.hashCode();
  }

  public boolean equals(Object o) {
    if (LinearFloatFunction.class != o.getClass()) return false;
    LinearFloatFunction other = (LinearFloatFunction)o;
    return  this.slope == other.slope
         && this.intercept == other.intercept
         && this.source.equals(other.source);
  }
}
