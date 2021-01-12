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
package org.apache.lucene.spatial3d.geom;

/**
 * Implemented by Geo3D shapes that can compute the distance from a point to the closest outside
 * edge.
 *
 * @lucene.experimental
 */
public interface GeoOutsideDistance extends Membership {

  // The following methods compute distances from the shape to a point
  // expected to be OUTSIDE the shape.  Typically a value of 0.0
  // is returned for points that happen to be within the shape.

  /**
   * Compute this shape's distance to the GeoPoint. A return value of 0.0 should be returned for
   * points inside of the shape.
   *
   * @param distanceStyle is the distance style.
   * @param point is the point to compute the distance to.
   * @return the distance.
   */
  public default double computeOutsideDistance(
      final DistanceStyle distanceStyle, final GeoPoint point) {
    return computeOutsideDistance(distanceStyle, point.x, point.y, point.z);
  }

  /**
   * Compute this shape's distance to the GeoPoint. A return value of 0.0 should be returned for
   * points inside of the shape.
   *
   * @param distanceStyle is the distance style.
   * @param x is the point's unit x coordinate (using U.S. convention).
   * @param y is the point's unit y coordinate (using U.S. convention).
   * @param z is the point's unit z coordinate (using U.S. convention).
   * @return the distance.
   */
  public double computeOutsideDistance(
      final DistanceStyle distanceStyle, final double x, final double y, final double z);
}
