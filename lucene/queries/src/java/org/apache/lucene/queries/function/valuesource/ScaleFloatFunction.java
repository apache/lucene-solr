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
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.docvalues.FloatDocValues;
import org.apache.lucene.search.IndexSearcher;

import java.io.IOException;
import java.util.List;
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

  @Override
  public String description() {
    return "scale(" + source.description() + "," + min + "," + max + ")";
  }

  private static class ScaleInfo {
    float minVal;
    float maxVal;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private ScaleInfo createScaleInfo(Map context, LeafReaderContext readerContext) throws IOException {
    final List<LeafReaderContext> leaves = ReaderUtil.getTopLevelContext(readerContext).leaves();

    float minVal = Float.POSITIVE_INFINITY;
    float maxVal = Float.NEGATIVE_INFINITY;

    for (LeafReaderContext leaf : leaves) {
      int maxDoc = leaf.reader().maxDoc();
      FunctionValues vals =  source.getValues(context, leaf);
      for (int i=0; i<maxDoc; i++) {
        if ( ! vals.exists(i) ) {
          continue;
        }
        float val = vals.floatVal(i);
        if ((Float.floatToRawIntBits(val) & (0xff<<23)) == 0xff<<23) {
          // if the exponent in the float is all ones, then this is +Inf, -Inf or NaN
          // which don't make sense to factor into the scale function
          continue;
        }
        if (val < minVal) {
          minVal = val;
        }
        if (val > maxVal) {
          maxVal = val;
        }
      }
    }

    if (minVal == Float.POSITIVE_INFINITY) {
    // must have been an empty index
      minVal = maxVal = 0;
    }

    ScaleInfo scaleInfo = new ScaleInfo();
    scaleInfo.minVal = minVal;
    scaleInfo.maxVal = maxVal;
    context.put(ScaleFloatFunction.this, scaleInfo);
    return scaleInfo;
  }

  @SuppressWarnings("rawtypes")
  @Override
  public FunctionValues getValues(Map context, LeafReaderContext readerContext) throws IOException {

    ScaleInfo scaleInfo = (ScaleInfo)context.get(ScaleFloatFunction.this);
    if (scaleInfo == null) {
      scaleInfo = createScaleInfo(context, readerContext);
    }

    final float scale = (scaleInfo.maxVal-scaleInfo.minVal==0) ? 0 : (max-min)/(scaleInfo.maxVal-scaleInfo.minVal);
    final float minSource = scaleInfo.minVal;
    final float maxSource = scaleInfo.maxVal;

    final FunctionValues vals =  source.getValues(context, readerContext);

    return new FloatDocValues(this) {
      @Override
      public boolean exists(int doc) throws IOException {
        return vals.exists(doc);
      }
      @Override
      public float floatVal(int doc) throws IOException {
        return (vals.floatVal(doc) - minSource) * scale + min;
      }
      @Override
      public String toString(int doc) throws IOException {
        return "scale(" + vals.toString(doc) + ",toMin=" + min + ",toMax=" + max
                + ",fromMin=" + minSource
                + ",fromMax=" + maxSource
                + ")";
      }
    };
  }

  @SuppressWarnings("rawtypes")
  @Override
  public void createWeight(Map context, IndexSearcher searcher) throws IOException {
    source.createWeight(context, searcher);
  }

  @Override
  public int hashCode() {
    int h = Float.floatToIntBits(min);
    h = h*29;
    h += Float.floatToIntBits(max);
    h = h*29;
    h += source.hashCode();
    return h;
  }

  @Override
  public boolean equals(Object o) {
    if (ScaleFloatFunction.class != o.getClass()) return false;
    ScaleFloatFunction other = (ScaleFloatFunction)o;
    return this.min == other.min
         && this.max == other.max
         && this.source.equals(other.source);
  }
}
