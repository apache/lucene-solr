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

/**
 * Groups double values into ranges
 */
public class DoubleRangeFactory {

  private final double min;
  private final double width;
  private final double max;

  /**
   * Creates a new DoubleRangeFactory
   * @param min     a minimum value; all doubles below this value are grouped into a single range
   * @param width   a standard width; all ranges between {@code min} and {@code max} are this wide,
   *                with the exception of the final range which may be up to this width.  Ranges
   *                are inclusive at the lower end, and exclusive at the upper end.
   * @param max     a maximum value; all doubles above this value are grouped into a single range
   */
  public DoubleRangeFactory(double min, double width, double max) {
    this.min = min;
    this.width = width;
    this.max = max;
  }

  /**
   * Finds the DoubleRange that a value should be grouped into
   * @param value the value to group
   * @param reuse an existing DoubleRange object to reuse
   */
  public DoubleRange getRange(double value, DoubleRange reuse) {
    if (reuse == null)
      reuse = new DoubleRange(Double.MIN_VALUE, Double.MAX_VALUE);
    if (value < min) {
      reuse.max = min;
      reuse.min = Double.MIN_VALUE;
      return reuse;
    }
    if (value >= max) {
      reuse.min = max;
      reuse.max = Double.MAX_VALUE;
      return reuse;
    }
    double bucket = Math.floor((value - min) / width);
    reuse.min = min + (bucket * width);
    reuse.max = reuse.min + width;
    return reuse;
  }

}
