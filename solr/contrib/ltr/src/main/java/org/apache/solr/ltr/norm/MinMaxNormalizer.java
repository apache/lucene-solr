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
package org.apache.solr.ltr.norm;

import java.util.LinkedHashMap;

/**
 * A Normalizer to scale a feature value using a (min,max) range.
 * <p>
 * Example configuration:
<pre>
"norm" : {
    "class" : "org.apache.solr.ltr.norm.MinMaxNormalizer",
    "params" : { "min":"0", "max":"50" }
}
</pre>
 * Example normalizations:
 * <ul>
 * <li>-5 will be normalized to -0.1
 * <li>55 will be normalized to  1.1
 * <li>+5 will be normalized to +0.1
 * </ul>
 */
public class MinMaxNormalizer extends Normalizer {

  private float min = Float.NEGATIVE_INFINITY;
  private float max = Float.POSITIVE_INFINITY;
  private float delta = max - min;

  private void updateDelta() {
    delta = max - min;
  }

  public float getMin() {
    return min;
  }

  public void setMin(float min) {
    this.min = min;
    updateDelta();
  }

  public void setMin(String min) {
    this.min = Float.parseFloat(min);
    updateDelta();
  }

  public float getMax() {
    return max;
  }

  public void setMax(float max) {
    this.max = max;
    updateDelta();
  }

  public void setMax(String max) {
    this.max = Float.parseFloat(max);
    updateDelta();
  }

  @Override
  protected void validate() throws NormalizerException {
    if (delta == 0f) {
      throw
      new NormalizerException("MinMax Normalizer delta must not be zero " +
          "| min = " + min + ",max = " + max + ",delta = " + delta);
    }
  }

  @Override
  public float normalize(float value) {
    return (value - min) / delta;
  }

  @Override
  public LinkedHashMap<String,Object> paramsToMap() {
    final LinkedHashMap<String,Object> params = new LinkedHashMap<>(2, 1.0f);
    params.put("min", Float.toString(min));
    params.put("max", Float.toString(max));
    return params;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(64); // default initialCapacity of 16 won't be enough
    sb.append(getClass().getSimpleName()).append('(');
    sb.append("min=").append(min);
    sb.append(",max=").append(max).append(')');
    return sb.toString();
  }

}
