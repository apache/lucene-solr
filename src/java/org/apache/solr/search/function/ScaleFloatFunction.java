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
import org.apache.lucene.search.Searcher;

import java.io.IOException;
import java.util.Map;

/**
 * Scales values to be between min and max.
 * <p>This implementation currently traverses all of the source values to obtain
 * their min and max.
 * <p>This implementation currently cannot distinguish when documents have been
 * deleted or documents that have no value, and 0.0 values will be used for
 * these cases.  This means that if values are normally all greater than 0.0, one can
 * still end up with 0.0 as the min value to map from.  In these cases, an
 * appropriate map() function could be used as a workaround to change 0.0
 * to a value in the real range.
 */
public class ScaleFloatFunction extends ValueSource {
  protected final ValueSource source;
  protected final float min;
  protected final float max;

  public ScaleFloatFunction(ValueSource source, float min, float max) {
    this.source = source;
    this.min = min;
    this.max = max;
  }

  public String description() {
    return "scale(" + source.description() + "," + min + "," + max + ")";
  }

  public DocValues getValues(Map context, IndexReader reader) throws IOException {
    final DocValues vals =  source.getValues(context, reader);
    int maxDoc = reader.maxDoc();

    // this doesn't take into account deleted docs!
    float minVal=0.0f;
    float maxVal=0.0f;

    if (maxDoc>0) {
      minVal = maxVal = vals.floatVal(0);      
    }

    // Traverse the complete set of values to get the min and the max.
    // Future alternatives include being able to ask a DocValues for min/max
    // Another memory-intensive option is to cache the values in
    // a float[] on this first pass.

    for (int i=0; i<maxDoc; i++) {
      float val = vals.floatVal(i);
      if ((Float.floatToRawIntBits(val) & (0xff<<23)) == 0xff<<23) {
        // if the exponent in the float is all ones, then this is +Inf, -Inf or NaN
        // which don't make sense to factor into the scale function
        continue;
      }
      if (val < minVal) {
        minVal = val;
      } else if (val > maxVal) {
        maxVal = val;
      }
    }

    final float scale = (maxVal-minVal==0) ? 0 : (max-min)/(maxVal-minVal);
    final float minSource = minVal;
    final float maxSource = maxVal;

    return new DocValues() {
      public float floatVal(int doc) {
	return (vals.floatVal(doc) - minSource) * scale + min;
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
	return "scale(" + vals.toString(doc) + ",toMin=" + min + ",toMax=" + max
                + ",fromMin=" + minSource
                + ",fromMax=" + maxSource
                + ")";
      }
    };
  }

  @Override
  public void createWeight(Map context, Searcher searcher) throws IOException {
    source.createWeight(context, searcher);
  }

  public int hashCode() {
    int h = Float.floatToIntBits(min);
    h = h*29;
    h += Float.floatToIntBits(max);
    h = h*29;
    h += source.hashCode();
    return h;
  }

  public boolean equals(Object o) {
    if (ScaleFloatFunction.class != o.getClass()) return false;
    ScaleFloatFunction other = (ScaleFloatFunction)o;
    return this.min == other.min
         && this.max == other.max
         && this.source.equals(other.source);
  }
}
