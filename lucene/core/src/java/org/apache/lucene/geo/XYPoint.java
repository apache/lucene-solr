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

import static org.apache.lucene.geo.XYEncodingUtils.checkVal;

/**
 * Represents a point on the earth's surface.  You can construct the point directly with {@code double}
 * coordinates.
 * <p>
 * NOTES:
 * <ol>
 *   <li>latitude/longitude values must be in decimal degrees.
 *   <li>For more advanced GeoSpatial indexing and query operations see the {@code spatial-extras} module
 * </ol>
 */
public final class XYPoint extends XYGeometry {

  /** latitude coordinate */
  private final float x;
  /** longitude coordinate */
  private final float y;

  /**
   * Creates a new Point from the supplied latitude/longitude.
   */
  public XYPoint(float x, float y) {
    this.x = checkVal(x);
    this.y = checkVal(y);
  }

  /** Returns latitude value at given index */
  public float getX() {
    return x;
  }

  /** Returns longitude value at given index */
  public float getY() {
    return y;
  }

  @Override
  protected Component2D toComponent2D() {
    return Point2D.create(this);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof XYPoint)) return false;
    XYPoint point = (XYPoint) o;
    return point.x == x && point.y == y;
  }

  @Override
  public int hashCode() {
    int result = Float.hashCode(x);
    result = 31 * result + Float.hashCode(y);
    return result;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Point(");
    sb.append(x);
    sb.append(",");
    sb.append(y);
    sb.append(')');
    return sb.toString();
  }
}
