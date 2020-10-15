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
 * Represents a circle on the XY plane.
 * <p>
 * NOTES:
 * <ol>
 *   <li> X/Y precision is float.
 *   <li> Radius precision is float.
 * </ol>
 * @lucene.experimental
 */
public final class XYCircle extends XYGeometry {
  /** Center x */
  private final float x;
  /** Center y */
  private final float y;
  /** radius */
  private final float radius;

  /**
   * Creates a new circle from the supplied x/y center and radius.
   */
  public XYCircle(float x, float y, float radius) {
    if (radius <= 0) {
       throw new IllegalArgumentException("radius must be bigger than 0, got " + radius);
    }
    if (Float.isFinite(radius) == false) {
      throw new IllegalArgumentException("radius must be finite, got " + radius);
    }
    this.x = checkVal(x);
    this.y = checkVal(y);
    this.radius = radius;
  }

  /** Returns the center's x */
  public float getX() {
    return x;
  }

  /** Returns the center's y */
  public float getY() {
    return y;
  }

  /** Returns the radius */
  public float getRadius() {
    return radius;
  }

  @Override
  protected Component2D toComponent2D() {
    return Circle2D.create(this);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof XYCircle)) return false;
    XYCircle circle = (XYCircle) o;
    return x == circle.x && y == circle.y && radius == circle.radius;
  }

  @Override
  public int hashCode() {
    int result = Float.hashCode(x);
    result = 31 * result + Float.hashCode(y);
    result = 31 * result + Float.hashCode(radius);
    return result;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("CIRCLE(");
    sb.append("[" + x + "," + y + "]");
    sb.append(" radius = " + radius);
    sb.append(')');
    return sb.toString();
  }
}
