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

package org.apache.lucene.queries.intervals;

import java.util.Objects;

import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.similarities.Similarity;

abstract class IntervalScoreFunction {

  static IntervalScoreFunction saturationFunction(float pivot) {
    if (pivot <= 0 || Float.isFinite(pivot) == false) {
      throw new IllegalArgumentException("pivot must be > 0, got: " + pivot);
    }
    return new SaturationFunction(pivot);
  }

  static IntervalScoreFunction sigmoidFunction(float pivot, float exp) {
    if (pivot <= 0 || Float.isFinite(pivot) == false) {
      throw new IllegalArgumentException("pivot must be > 0, got: " + pivot);
    }
    if (exp <= 0 || Float.isFinite(exp) == false) {
      throw new IllegalArgumentException("exp must be > 0, got: " + exp);
    }
    return new SigmoidFunction(pivot, exp);
  }

  public abstract Similarity.SimScorer scorer(float weight);

  public abstract Explanation explain(String interval, float weight, float sloppyFreq);

  @Override
  public abstract boolean equals(Object other);

  @Override
  public abstract int hashCode();

  @Override
  public abstract String toString();

  private static class SaturationFunction extends IntervalScoreFunction {

    final float pivot;

    private SaturationFunction(float pivot) {
      this.pivot = pivot;
    }

    @Override
    public Similarity.SimScorer scorer(float weight) {
      return new Similarity.SimScorer() {
        @Override
        public float score(float freq, long norm) {
          // should be f / (f + k) but we rewrite it to
          // 1 - k / (f + k) to make sure it doesn't decrease
          // with f in spite of rounding
          return weight * (1.0f - pivot / (pivot + freq));
        }
      };
    }

    @Override
    public Explanation explain(String interval, float weight, float sloppyFreq) {
      float score = scorer(weight).score(sloppyFreq, 1L);
      return Explanation.match(score,
          "Saturation function on interval frequency, computed as w * S / (S + k) from:",
          Explanation.match(weight, "w, weight of this function"),
          Explanation.match(pivot, "k, pivot feature value that would give a score contribution equal to w/2"),
          Explanation.match(sloppyFreq, "S, the sloppy frequency of the interval query " + interval));
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      SaturationFunction that = (SaturationFunction) o;
      return Float.compare(that.pivot, pivot) == 0;
    }

    @Override
    public int hashCode() {
      return Objects.hash(pivot);
    }

    @Override
    public String toString() {
      return "SaturationFunction(pivot=" + pivot + ")";
    }
  }

  private static class SigmoidFunction extends IntervalScoreFunction {

    private final float pivot, a;
    private final double pivotPa;

    private SigmoidFunction(float pivot, float a) {
      this.pivot = pivot;
      this.a = a;
      this.pivotPa = Math.pow(pivot, a);
    }

    @Override
    public Similarity.SimScorer scorer(float weight) {
      return new Similarity.SimScorer() {
        @Override
        public float score(float freq, long norm) {
          // should be f^a / (f^a + k^a) but we rewrite it to
          // 1 - k^a / (f + k^a) to make sure it doesn't decrease
          // with f in spite of rounding
          return (float) (weight * (1.0f - pivotPa / (Math.pow(freq, a) + pivotPa)));
        }
      };
    }

    @Override
    public Explanation explain(String interval, float weight, float sloppyFreq) {
      float score = scorer(weight).score(sloppyFreq, 1L);
      return Explanation.match(score,
          "Sigmoid function on interval frequency, computed as w * S^a / (S^a + k^a) from:",
          Explanation.match(weight, "w, weight of this function"),
          Explanation.match(pivot, "k, pivot feature value that would give a score contribution equal to w/2"),
          Explanation.match(a, "a, exponent, higher values make the function grow slower before k and faster after k"),
          Explanation.match(sloppyFreq, "S, the sloppy frequency of the interval query " + interval));
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      SigmoidFunction that = (SigmoidFunction) o;
      return Float.compare(that.pivot, pivot) == 0 &&
          Float.compare(that.a, a) == 0;
    }

    @Override
    public int hashCode() {
      return Objects.hash(pivot, a);
    }

    @Override
    public String toString() {
      return "SigmoidFunction(pivot=" + pivot + ", a=" + a + ")";
    }
  }

}
