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
package org.apache.lucene.misc;

import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.index.FieldInvertState;

/**
 * <p>
 * A similarity with a lengthNorm that provides for a "plateau" of
 * equally good lengths, and tf helper functions.
 * </p>
 * <p>
 * For lengthNorm, A min/max can be specified to define the
 * plateau of lengths that should all have a norm of 1.0.
 * Below the min, and above the max the lengthNorm drops off in a
 * sqrt function.
 * </p>
 * <p>
 * For tf, baselineTf and hyperbolicTf functions are provided, which
 * subclasses can choose between.
 * </p>
 *
 * @see <a href="doc-files/ss.gnuplot">A Gnuplot file used to generate some of the visualizations refrenced from each function.</a> 
 */
public class SweetSpotSimilarity extends DefaultSimilarity {

  private int ln_min = 1;
  private int ln_max = 1;
  private float ln_steep = 0.5f;

  private float tf_base = 0.0f;
  private float tf_min = 0.0f;

  private float tf_hyper_min = 0.0f;
  private float tf_hyper_max = 2.0f;
  private double tf_hyper_base = 1.3d;
  private float tf_hyper_xoffset = 10.0f;
    
  public SweetSpotSimilarity() {
    super();
  }

  /**
   * Sets the baseline and minimum function variables for baselineTf
   *
   * @see #baselineTf
   */
  public void setBaselineTfFactors(float base, float min) {
    tf_min = min;
    tf_base = base;
  }
  
  /**
   * Sets the function variables for the hyperbolicTf functions
   *
   * @param min the minimum tf value to ever be returned (default: 0.0)
   * @param max the maximum tf value to ever be returned (default: 2.0)
   * @param base the base value to be used in the exponential for the hyperbolic function (default: 1.3)
   * @param xoffset the midpoint of the hyperbolic function (default: 10.0)
   * @see #hyperbolicTf
   */
  public void setHyperbolicTfFactors(float min, float max,
                                     double base, float xoffset) {
    tf_hyper_min = min;
    tf_hyper_max = max;
    tf_hyper_base = base;
    tf_hyper_xoffset = xoffset;
  }
    
  /**
   * Sets the default function variables used by lengthNorm when no field
   * specific variables have been set.
   *
   * @see #computeLengthNorm
   */
  public void setLengthNormFactors(int min, int max, float steepness, boolean discountOverlaps) {
    this.ln_min = min;
    this.ln_max = max;
    this.ln_steep = steepness;
    this.discountOverlaps = discountOverlaps;
  }
    
  /**
   * Implemented as <code> state.getBoost() *
   * computeLengthNorm(numTokens) </code> where
   * numTokens does not count overlap tokens if
   * discountOverlaps is true by default or true for this
   * specific field. 
   */
  @Override
  public float lengthNorm(FieldInvertState state) {
    final int numTokens;

    if (discountOverlaps)
      numTokens = state.getLength() - state.getNumOverlap();
    else
      numTokens = state.getLength();

    return state.getBoost() * computeLengthNorm(numTokens);
  }

  /**
   * Implemented as:
   * <code>
   * 1/sqrt( steepness * (abs(x-min) + abs(x-max) - (max-min)) + 1 )
   * </code>.
   *
   * <p>
   * This degrades to <code>1/sqrt(x)</code> when min and max are both 1 and
   * steepness is 0.5
   * </p>
   *
   * <p>
   * :TODO: potential optimization is to just flat out return 1.0f if numTerms
   * is between min and max.
   * </p>
   *
   * @see #setLengthNormFactors
   * @see <a href="doc-files/ss.computeLengthNorm.svg">An SVG visualization of this function</a> 
   */
  public float computeLengthNorm(int numTerms) {
    final int l = ln_min;
    final int h = ln_max;
    final float s = ln_steep;
  
    return (float)
      (1.0f /
       Math.sqrt
       (
        (
         s *
         (float)(Math.abs(numTerms - l) + Math.abs(numTerms - h) - (h-l))
         )
        + 1.0f
        )
       );
  }

  /**
   * Delegates to baselineTf
   *
   * @see #baselineTf
   */
  @Override
  public float tf(float freq) {
    return baselineTf(freq);
  }
  
  /**
   * Implemented as:
   * <code>
   *  (x &lt;= min) &#63; base : sqrt(x+(base**2)-min)
   * </code>
   * ...but with a special case check for 0.
   * <p>
   * This degrates to <code>sqrt(x)</code> when min and base are both 0
   * </p>
   *
   * @see #setBaselineTfFactors
   * @see <a href="doc-files/ss.baselineTf.svg">An SVG visualization of this function</a> 
   */
  public float baselineTf(float freq) {

    if (0.0f == freq) return 0.0f;
  
    return (freq <= tf_min)
      ? tf_base
      : (float)Math.sqrt(freq + (tf_base * tf_base) - tf_min);
  }

  /**
   * Uses a hyperbolic tangent function that allows for a hard max...
   *
   * <code>
   * tf(x)=min+(max-min)/2*(((base**(x-xoffset)-base**-(x-xoffset))/(base**(x-xoffset)+base**-(x-xoffset)))+1)
   * </code>
   *
   * <p>
   * This code is provided as a convenience for subclasses that want
   * to use a hyperbolic tf function.
   * </p>
   *
   * @see #setHyperbolicTfFactors
   * @see <a href="doc-files/ss.hyperbolicTf.svg">An SVG visualization of this function</a> 
   */
  public float hyperbolicTf(float freq) {
    if (0.0f == freq) return 0.0f;

    final float min = tf_hyper_min;
    final float max = tf_hyper_max;
    final double base = tf_hyper_base;
    final float xoffset = tf_hyper_xoffset;
    final double x = (double)(freq - xoffset);
  
    final float result = min +
      (float)(
              (max-min) / 2.0f
              *
              (
               ( ( Math.pow(base,x) - Math.pow(base,-x) )
                 / ( Math.pow(base,x) + Math.pow(base,-x) )
                 )
               + 1.0d
               )
              );

    return Float.isNaN(result) ? max : result;
    
  }

}
