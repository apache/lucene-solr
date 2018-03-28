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
package org.apache.lucene.search.similarities;


import org.apache.lucene.search.Explanation;

/**
 * Pareto-Zipf Normalization
 * @lucene.experimental
 */
public class NormalizationZ extends Normalization {
  final float z;

  /**
   * Calls {@link #NormalizationZ(float) NormalizationZ(0.3)}
   */
  public NormalizationZ() {
    this(0.30F);
  }

  /**
   * Creates NormalizationZ with the supplied parameter <code>z</code>.
   * @param z represents <code>A/(A+1)</code> where <code>A</code> 
   *          measures the specificity of the language. It ranges from (0 .. 0.5)
   */
  public NormalizationZ(float z) {
    if (Float.isNaN(z) || z <= 0f || z >= 0.5f) {
      throw new IllegalArgumentException("illegal z value: " + z + ", must be in the range (0 .. 0.5)");
    }
    this.z = z;
  }
  
  @Override
  public double tfn(BasicStats stats, double tf, double len) {
    return tf * Math.pow(stats.avgFieldLength / len, z);
  }

  @Override
  public Explanation explain(BasicStats stats, double tf, double len) {
    return Explanation.match(
        (float) tfn(stats, tf, len),
        getClass().getSimpleName()
            + ", computed as tf * Math.pow(avgfl / fl, z) from:",
        Explanation.match((float) tf,
            "tf, number of occurrences of term in the document"),
        Explanation.match((float) stats.getAvgFieldLength(),
            "avgfl, average length of field across all documents"),
        Explanation.match((float) len, "fl, field length of the document"),
        Explanation.match(z, "z, relates to specificity of the language"));
  }

  @Override
  public String toString() {
    return "Z(" + z + ")";
  }
  
  /**
   * Returns the parameter <code>z</code>
   * @see #NormalizationZ(float)
   */
  public float getZ() {
    return z;
  }
}
