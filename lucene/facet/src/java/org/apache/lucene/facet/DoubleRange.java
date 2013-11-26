package org.apache.lucene.facet;

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

import org.apache.lucene.document.DoubleDocValuesField; // javadocs

/** Represents a range over double values indexed as {@link
 *  DoubleDocValuesField}.  */
public final class DoubleRange extends Range {
  private final double minIncl;
  private final double maxIncl;

  public final double min;
  public final double max;
  public final boolean minInclusive;
  public final boolean maxInclusive;

  /** Create a DoubleRange. */
  public DoubleRange(String label, double min, boolean minInclusive, double max, boolean maxInclusive) {
    super(label);
    this.min = min;
    this.max = max;
    this.minInclusive = minInclusive;
    this.maxInclusive = maxInclusive;

    // TODO: if DoubleDocValuesField used
    // NumericUtils.doubleToSortableLong format (instead of
    // Double.doubleToRawLongBits) we could do comparisons
    // in long space 

    if (Double.isNaN(min)) {
      throw new IllegalArgumentException("min cannot be NaN");
    }
    if (!minInclusive) {
      min = Math.nextUp(min);
    }

    if (Double.isNaN(max)) {
      throw new IllegalArgumentException("max cannot be NaN");
    }
    if (!maxInclusive) {
      // Why no Math.nextDown?
      max = Math.nextAfter(max, Double.NEGATIVE_INFINITY);
    }

    this.minIncl = min;
    this.maxIncl = max;
  }

  @Override
  public boolean accept(long value) {
    double doubleValue = Double.longBitsToDouble(value);
    return doubleValue >= minIncl && doubleValue <= maxIncl;
  }
}

