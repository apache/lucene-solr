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

package org.apache.lucene.luke.models.search;

/**
 * Configurations for Similarity.
 */
public final class SimilarityConfig {

  private final boolean useClassicSimilarity;

  /* BM25Similarity parameters */

  private final float k1;

  private final float b;

  /* Common parameters */

  private final boolean discountOverlaps;

  /** Builder for {@link SimilarityConfig} */
  public static class Builder {
    private boolean useClassicSimilarity = false;
    private float k1 = 1.2f;
    private float b = 0.75f;
    private boolean discountOverlaps = true;

    public Builder useClassicSimilarity(boolean val) {
      useClassicSimilarity = val;
      return this;
    }

    public Builder k1(float val) {
      k1 = val;
      return this;
    }

    public Builder b(float val) {
      b = val;
      return this;
    }

    public Builder discountOverlaps (boolean val) {
      discountOverlaps = val;
      return this;
    }

    public SimilarityConfig build() {
      return new SimilarityConfig(this);
    }
  }

  private SimilarityConfig(Builder builder) {
    this.useClassicSimilarity = builder.useClassicSimilarity;
    this.k1 = builder.k1;
    this.b = builder.b;
    this.discountOverlaps = builder.discountOverlaps;
  }

  public boolean isUseClassicSimilarity() {
    return useClassicSimilarity;
  }

  public float getK1() {
    return k1;
  }

  public float getB() {
    return b;
  }

  public boolean isDiscountOverlaps() {
    return discountOverlaps;
  }

  public String toString() {
    return "SimilarityConfig: [" +
        " use classic similarity=" + useClassicSimilarity + ";" +
        " discount overlaps=" + discountOverlaps + ";" +
        " k1=" + k1 + ";" +
        " b=" + b + ";" +
        "]";
  }
}
