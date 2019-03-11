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
package org.apache.lucene.geo;

public class XYRectangle {
  /** minimum x value */
  public final double minX;
  /** minimum y value */
  public final double maxX;
  /** maximum x value */
  public final double minY;
  /** maximum y value */
  public final double maxY;

  /**
   * Constructs a bounding box by first validating the provided latitude and longitude coordinates
   */
  public XYRectangle(double minX, double maxX, double minY, double maxY) {
    this.minX = minX;
    this.maxX = maxX;
    this.minY = minY;
    this.maxY = maxY;
    assert minX <= maxX;
    assert minY <= maxY;

    // NOTE: cannot assert maxLon >= minLon since this rect could cross the dateline
  }
}
