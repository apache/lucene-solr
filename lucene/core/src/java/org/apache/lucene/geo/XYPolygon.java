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
 * Represents a polygon in cartesian space. You can construct the Polygon directly with {@code float[]}, {@code float[]} x, y arrays
 * coordinates.
 */
public final class XYPolygon extends XYGeometry {
  private final float[] x;
  private final float[] y;
  private final XYPolygon[] holes;

  /** minimum x of this polygon's bounding box area */
  public final float minX;
  /** maximum x of this polygon's bounding box area */
  public final float maxX;
  /** minimum y of this polygon's bounding box area */
  public final float minY;
  /** maximum y of this polygon's bounding box area */
  public final float maxY;
  /** winding order of the vertices */
  private final GeoUtils.WindingOrder windingOrder;

  /**
   * Creates a new Polygon from the supplied x, y arrays, and optionally any holes.
   */
  public XYPolygon(float[] x, float[] y, XYPolygon... holes) {
    if (x == null) {
      throw new IllegalArgumentException("x must not be null");
    }
    if (y == null) {
      throw new IllegalArgumentException("y must not be null");
    }
    if (holes == null) {
      throw new IllegalArgumentException("holes must not be null");
    }
    if (x.length != y.length) {
      throw new IllegalArgumentException("x and y must be equal length");
    }
    if (x.length < 4) {
      throw new IllegalArgumentException("at least 4 polygon points required");
    }
    if (x[0] != x[x.length-1]) {
      throw new IllegalArgumentException("first and last points of the polygon must be the same (it must close itself): x[0]=" + x[0] + " x[" + (x.length-1) + "]=" + x[x.length-1]);
    }
    if (y[0] != y[y.length-1]) {
      throw new IllegalArgumentException("first and last points of the polygon must be the same (it must close itself): y[0]=" + y[0] + " y[" + (y.length-1) + "]=" + y[y.length-1]);
    }
    for (int i = 0; i < holes.length; i++) {
      XYPolygon inner = holes[i];
      if (inner.holes.length > 0) {
        throw new IllegalArgumentException("holes may not contain holes: polygons may not nest.");
      }
    }
    this.x = x.clone();
    this.y = y.clone();
    this.holes = holes.clone();

    // compute bounding box
    float minX = checkVal(x[0]);
    float maxX = x[0];
    float minY = checkVal(y[0]);
    float maxY = y[0];

    double windingSum = 0d;
    final int numPts = x.length - 1;
    for (int i = 1, j = 0; i < numPts; j = i++) {
      minX = Math.min(checkVal(x[i]), minX);
      maxX = Math.max(x[i], maxX);
      minY = Math.min(checkVal(y[i]), minY);
      maxY = Math.max(y[i], maxY);
      // compute signed area
      windingSum += (x[j] - x[numPts])*(y[i] - y[numPts])
          - (y[j] - y[numPts])*(x[i] - x[numPts]);
    }
    this.minX = minX;
    this.maxX = maxX;
    this.minY = minY;
    this.maxY = maxY;
    this.windingOrder = (windingSum < 0) ? GeoUtils.WindingOrder.CCW : GeoUtils.WindingOrder.CW;
  }

  /** returns the number of vertex points */
  public int numPoints() {
    return x.length;
  }

  /** Returns a copy of the internal x array */
  public float[] getPolyX() {
    return x.clone();
  }

  /** Returns x value at given index */
  public float getPolyX(int vertex) {
    return x[vertex];
  }

  /** Returns a copy of the internal y array */
  public float[] getPolyY() {
    return y.clone();
  }

  /** Returns y value at given index */
  public float getPolyY(int vertex) {
    return y[vertex];
  }

  /** Returns a copy of the internal holes array */
  public XYPolygon[] getHoles() {
    return holes.clone();
  }

  XYPolygon getHole(int i) {
    return holes[i];
  }

  /** Returns the winding order (CW, COLINEAR, CCW) for the polygon shell */
  public GeoUtils.WindingOrder getWindingOrder() {
    return this.windingOrder;
  }

  /** returns the number of holes for the polygon */
  public int numHoles() {
    return holes.length;
  }

  @Override
  protected Component2D toComponent2D() {
    return Polygon2D.create(this);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + Arrays.hashCode(holes);
    result = prime * result + Arrays.hashCode(x);
    result = prime * result + Arrays.hashCode(y);
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    XYPolygon other = (XYPolygon) obj;
    if (!Arrays.equals(holes, other.holes)) return false;
    if (!Arrays.equals(x, other.x)) return false;
    if (!Arrays.equals(y, other.y)) return false;
    return true;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < x.length; i++) {
      sb.append("[")
          .append(x[i])
          .append(", ")
          .append(y[i])
          .append("] ");
    }
    if (holes.length > 0) {
      sb.append(", holes=");
      sb.append(Arrays.toString(holes));
    }
    return sb.toString();
  }
}
