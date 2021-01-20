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

/** Represents a x/y cartesian rectangle. */
public final class XYRectangle extends XYGeometry {
  /** minimum x value */
  public final float minX;
  /** minimum y value */
  public final float maxX;
  /** maximum x value */
  public final float minY;
  /** maximum y value */
  public final float maxY;

  /** Constructs a bounding box by first validating the provided x and y coordinates */
  public XYRectangle(float minX, float maxX, float minY, float maxY) {
    if (minX > maxX) {
      throw new IllegalArgumentException(
          "minX must be lower than maxX, got " + minX + " > " + maxX);
    }
    if (minY > maxY) {
      throw new IllegalArgumentException(
          "minY must be lower than maxY, got " + minY + " > " + maxY);
    }
    this.minX = checkVal(minX);
    this.maxX = checkVal(maxX);
    this.minY = checkVal(minY);
    this.maxY = checkVal(maxY);
  }

  @Override
  protected Component2D toComponent2D() {
    return Rectangle2D.create(this);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    XYRectangle rectangle = (XYRectangle) o;

    if (Float.compare(rectangle.minX, minX) != 0) return false;
    if (Float.compare(rectangle.minY, minY) != 0) return false;
    if (Float.compare(rectangle.maxX, maxX) != 0) return false;
    return Float.compare(rectangle.maxY, maxY) == 0;
  }

  /** Compute Bounding Box for a circle in cartesian geometry */
  public static XYRectangle fromPointDistance(final float x, final float y, final float radius) {
    checkVal(x);
    checkVal(y);
    if (radius < 0) {
      throw new IllegalArgumentException("radius must be bigger than 0, got " + radius);
    }
    if (Float.isFinite(radius) == false) {
      throw new IllegalArgumentException("radius must be finite, got " + radius);
    }
    // LUCENE-9243: We round up the bounding box to avoid
    // numerical errors.
    float distanceBox = Math.nextUp(radius);
    float minX = Math.max(-Float.MAX_VALUE, x - distanceBox);
    float maxX = Math.min(Float.MAX_VALUE, x + distanceBox);
    float minY = Math.max(-Float.MAX_VALUE, y - distanceBox);
    float maxY = Math.min(Float.MAX_VALUE, y + distanceBox);
    return new XYRectangle(minX, maxX, minY, maxY);
  }

  @Override
  public int hashCode() {
    int result;
    long temp;
    temp = Float.floatToIntBits(minX);
    result = (int) (temp ^ (temp >>> 32));
    temp = Float.floatToIntBits(minY);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    temp = Float.floatToIntBits(maxX);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    temp = Float.floatToIntBits(maxY);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    return result;
  }

  @Override
  public String toString() {
    StringBuilder b = new StringBuilder();
    b.append("XYRectangle(x=");
    b.append(minX);
    b.append(" TO ");
    b.append(maxX);
    b.append(" y=");
    b.append(minY);
    b.append(" TO ");
    b.append(maxY);
    b.append(")");

    return b.toString();
  }
}
