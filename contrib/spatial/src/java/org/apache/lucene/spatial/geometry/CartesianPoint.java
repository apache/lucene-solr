/**
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

package org.apache.lucene.spatial.geometry;

/**
 * Represents lat/lngs as fixed point numbers translated so that all
 * world coordinates are in the first quadrant.  The same fixed point
 * scale as is used for FixedLatLng is employed.
 */
public class CartesianPoint {
  private int x;
  private int y;
  
  public CartesianPoint(int x, int y) {
    this.x=x;
    this.y=y;
  }
  
  public int getX() {
    return x;
  }
  
  public int getY() {
    return y;
  }
  
  @Override
  public String toString() {
    return "Point(" + x + "," + y + ")";
  }

  /**
   * Return a new point translated in the x and y dimensions
   * @param i
   * @param translation
   * @return
   */
  public CartesianPoint translate(int deltaX, int deltaY) {
    return new CartesianPoint(this.x+deltaX, this.y+deltaY);
  }
}
