package org.apache.lucene.facet.range;

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

import org.apache.lucene.document.FloatDocValuesField; // javadocs

/** Represents a range over float values indexed as {@link
 *  FloatDocValuesField}.  */
public final class FloatRange extends Range {
  private final float minIncl;
  private final float maxIncl;

  public final float min;
  public final float max;
  public final boolean minInclusive;
  public final boolean maxInclusive;

  /** Create a FloatRange. */
  public FloatRange(String label, float min, boolean minInclusive, float max, boolean maxInclusive) {
    super(label);
    this.min = min;
    this.max = max;
    this.minInclusive = minInclusive;
    this.maxInclusive = maxInclusive;

    // TODO: if FloatDocValuesField used
    // NumericUtils.floatToSortableInt format (instead of
    // Float.floatToRawIntBits) we could do comparisons
    // in int space 

    if (Float.isNaN(min)) {
      throw new IllegalArgumentException("min cannot be NaN");
    }
    if (!minInclusive) {
      min = Math.nextUp(min);
    }

    if (Float.isNaN(max)) {
      throw new IllegalArgumentException("max cannot be NaN");
    }
    if (!maxInclusive) {
      // Why no Math.nextDown?
      max = Math.nextAfter(max, Float.NEGATIVE_INFINITY);
    }

    this.minIncl = min;
    this.maxIncl = max;
  }

  @Override
  public boolean accept(long value) {
    float floatValue = Float.intBitsToFloat((int) value);
    return floatValue >= minIncl && floatValue <= maxIncl;
  }
}
