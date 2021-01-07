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

/** Groups double values into ranges */
public class LongRangeFactory {

  private final long min;
  private final long width;
  private final long max;

  /**
   * Creates a new LongRangeFactory
   *
   * @param min a minimum value; all longs below this value are grouped into a single range
   * @param width a standard width; all ranges between {@code min} and {@code max} are this wide,
   *     with the exception of the final range which may be up to this width. Ranges are inclusive
   *     at the lower end, and exclusive at the upper end.
   * @param max a maximum value; all longs above this value are grouped into a single range
   */
  public LongRangeFactory(long min, long width, long max) {
    this.min = min;
    this.width = width;
    this.max = max;
  }

  /**
   * Finds the LongRange that a value should be grouped into
   *
   * @param value the value to group
   * @param reuse an existing LongRange object to reuse
   */
  public LongRange getRange(long value, LongRange reuse) {
    if (reuse == null) {
      reuse = new LongRange(Long.MIN_VALUE, Long.MAX_VALUE);
    }
    if (value < min) {
      reuse.max = min;
      reuse.min = Long.MIN_VALUE;
      return reuse;
    }
    if (value >= max) {
      reuse.min = max;
      reuse.max = Long.MAX_VALUE;
      return reuse;
    }
    long bucket = (value - min) / width;
    reuse.min = min + (bucket * width);
    reuse.max = reuse.min + width;
    return reuse;
  }
}
