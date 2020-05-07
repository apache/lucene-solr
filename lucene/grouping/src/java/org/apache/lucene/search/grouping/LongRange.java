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
 * Represents a contiguous range of long values, with an inclusive minimum and
 * exclusive maximum
 */
public class LongRange {

  /** The inclusive minimum value of this range */
  public long min;
  /** The exclusive maximum value of this range */
  public long max;

  /**
   * Creates a new double range, running from {@code min} inclusive to {@code max} exclusive
   */
  public LongRange(long min, long max) {
    this.min = min;
    this.max = max;
  }

  @Override
  public String toString() {
    return "LongRange(" + min + ", " + max + ")";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    LongRange that = (LongRange) o;
    return that.min == min && that.max == max;
  }

  @Override
  public int hashCode() {
    return Objects.hash(min, max);
  }
}
