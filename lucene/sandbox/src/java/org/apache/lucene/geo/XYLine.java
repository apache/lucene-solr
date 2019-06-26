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

/**
 * Represents a line in cartesian space. You can construct the Line directly with {@code double[]}, {@code double[]} x, y arrays
 * coordinates.
 *
 * @lucene.experimental
 */
public class XYLine {
  /** array of x coordinates */
  private final double[] x;
  /** array of y coordinates */
  private final double[] y;

  /** minimum x of this line's bounding box */
  public final double minX;
  /** maximum x of this line's bounding box */
  public final double maxX;
  /** minimum y of this line's bounding box */
  public final double minY;
  /** maximum y of this line's bounding box */
  public final double maxY;

  /**
   * Creates a new Line from the supplied x/y array.
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
    double minX = x[0];
    double minY = y[0];
    double maxX = x[0];
    double maxY = y[0];
    for (int i = 0; i < x.length; ++i) {
      minX = Math.min(x[i], minX);
      minY = Math.min(y[i], minY);
      maxX = Math.max(x[i], maxX);
      maxY = Math.max(y[i], maxY);
    }

    this.x = new double[x.length];
    this.y = new double[y.length];
    for (int i = 0; i < x.length; ++i) {
      this.x[i] = (double)x[i];
      this.y[i] = (double)y[i];
    }
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
  public double getX(int vertex) {
    return x[vertex];
  }

  /** Returns y value at given index */
  public double getY(int vertex) {
    return y[vertex];
  }

  /** Returns a copy of the internal x array */
  public double[] getX() {
    return x.clone();
  }

  /** Returns a copy of the internal y array */
  public double[] getY() {
    return y.clone();
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

  /** prints polygons as geojson */
  public String toGeoJSON() {
    StringBuilder sb = new StringBuilder();
    sb.append("[");
    sb.append(Polygon.verticesToGeoJSON(x, y));
    sb.append("]");
    return sb.toString();
  }
}
