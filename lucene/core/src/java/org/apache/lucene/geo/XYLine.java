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

import java.util.Arrays;

import static org.apache.lucene.geo.XYEncodingUtils.checkVal;

/**
 * Represents a line in cartesian space. You can construct the Line directly with {@code float[]}, {@code float[]} x, y arrays
 * coordinates.
 */
public class XYLine extends XYGeometry {
  /** array of x coordinates */
  private final float[] x;
  /** array of y coordinates */
  private final float[] y;

  /** minimum x of this line's bounding box */
  public final float minX;
  /** maximum y of this line's bounding box */
  public final float maxX;
  /** minimum y of this line's bounding box */
  public final float minY;
  /** maximum y of this line's bounding box */
  public final float maxY;

  /**
   * Creates a new Line from the supplied X/Y array.
   */
  public XYLine(float[] x, float[] y) {
    if (x == null) {
      throw new IllegalArgumentException("x must not be null");
    }
    if (y == null) {
      throw new IllegalArgumentException("y must not be null");
    }
    if (x.length != y.length) {
      throw new IllegalArgumentException("x and y must be equal length");
    }
    if (x.length < 2) {
      throw new IllegalArgumentException("at least 2 line points required");
    }

    // compute bounding box
    float minX = Float.MAX_VALUE;
    float minY = Float.MAX_VALUE;
    float maxX = -Float.MAX_VALUE;
    float maxY = -Float.MAX_VALUE;
    for (int i = 0; i < x.length; ++i) {
      minX = Math.min(checkVal(x[i]), minX);
      minY = Math.min(checkVal(y[i]), minY);
      maxX = Math.max(x[i], maxX);
      maxY = Math.max(y[i], maxY);
    }
    this.x = x.clone();
    this.y = y.clone();

    this.minX = minX;
    this.maxX = maxX;
    this.minY = minY;
    this.maxY = maxY;
  }

  /** returns the number of vertex points */
  public int numPoints() {
    return x.length;
  }

  /** Returns x value at given index */
  public float getX(int vertex) {
    return x[vertex];
  }

  /** Returns y value at given index */
  public float getY(int vertex) {
    return y[vertex];
  }

  /** Returns a copy of the internal x array */
  public float[] getX() {
    return x.clone();
  }

  /** Returns a copy of the internal y array */
  public float[] getY() {
    return y.clone();
  }

  @Override
  protected Component2D toComponent2D() {
    return Line2D.create(this);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof XYLine)) return false;
    XYLine line = (XYLine) o;
    return Arrays.equals(x, line.x) && Arrays.equals(y, line.y);
  }

  @Override
  public int hashCode() {
    int result = Arrays.hashCode(x);
    result = 31 * result + Arrays.hashCode(y);
    return result;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("XYLINE(");
    for (int i = 0; i < x.length; i++) {
      sb.append("[")
          .append(x[i])
          .append(", ")
          .append(y[i])
          .append("]");
    }
    sb.append(')');
    return sb.toString();
  }
}
