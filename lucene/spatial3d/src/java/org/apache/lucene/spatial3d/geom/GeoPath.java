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
 * Interface describing a path.
 *
 * @lucene.experimental
 */
public interface GeoPath extends GeoDistanceShape {

  // The following methods compute distances along the path from the shape to a point
  // that doesn't need to be inside the shape.  The perpendicular distance from the path
  // itself to the point is not included in the calculation.

  /**
   * Compute the nearest path distance to the GeoPoint. The path distance will not include the
   * distance from the path itself to the point, but just the distance along the path to the nearest
   * point on the path.
   *
   * @param distanceStyle is the distance style.
   * @param point is the point to compute the distance to.
   * @return the distance to the nearest path point.
   */
  public default double computeNearestDistance(
      final DistanceStyle distanceStyle, final GeoPoint point) {
    return computeNearestDistance(distanceStyle, point.x, point.y, point.z);
  }

  /**
   * Compute the nearest path distance to the GeoPoint. The path distance will not include the
   * distance from the path itself to the point, but just the distance along the path to the nearest
   * point on the path.
   *
   * @param x is the point's unit x coordinate (using U.S. convention).
   * @param y is the point's unit y coordinate (using U.S. convention).
   * @param z is the point's unit z coordinate (using U.S. convention).
   * @return the distance to the nearest path point.
   */
  public double computeNearestDistance(
      final DistanceStyle distanceStyle, final double x, final double y, final double z);

  // The following methods compute the best distance from the path center to the point.

  /**
   * Compute the shortest distance from the path center to the GeoPoint. The distance is meant to
   * allow comparisons between different paths to find the one that goes closest to a point.
   *
   * @param distanceStyle is the distance style.
   * @param point is the point to compute the distance to.
   * @return the shortest distance from the path center to the point.
   */
  public default double computePathCenterDistance(
      final DistanceStyle distanceStyle, final GeoPoint point) {
    return computePathCenterDistance(distanceStyle, point.x, point.y, point.z);
  }

  /**
   * Compute the shortest distance from the path center to the GeoPoint. The distance is meant to
   * allow comparisons between different paths to find the one that goes closest to a point.
   *
   * @param distanceStyle is the distance style.
   * @param x is the point's unit x coordinate (using U.S. convention).
   * @param y is the point's unit y coordinate (using U.S. convention).
   * @param z is the point's unit z coordinate (using U.S. convention).
   * @return the shortest distance from the path center to the point.
   */
  public double computePathCenterDistance(
      final DistanceStyle distanceStyle, final double x, final double y, final double z);
}
