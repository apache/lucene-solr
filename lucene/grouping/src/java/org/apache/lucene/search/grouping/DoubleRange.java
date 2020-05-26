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

package org.apache.lucene.search.grouping;

import java.util.Objects;

/**
 * Represents a contiguous range of double values, with an inclusive minimum and
 * exclusive maximum
 */
public class DoubleRange {

  /** The inclusive minimum value of this range */
  public double min;
  /** The exclusive maximum value of this range */
  public double max;

  /**
   * Creates a new double range, running from {@code min} inclusive to {@code max} exclusive
   */
  public DoubleRange(double min, double max) {
    this.min = min;
    this.max = max;
  }

  @Override
  public String toString() {
    return "DoubleRange(" + min + ", " + max + ")";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    DoubleRange that = (DoubleRange) o;
    return Double.compare(that.min, min) == 0 &&
        Double.compare(that.max, max) == 0;
  }

  @Override
  public int hashCode() {
    return Objects.hash(min, max);
  }
}
