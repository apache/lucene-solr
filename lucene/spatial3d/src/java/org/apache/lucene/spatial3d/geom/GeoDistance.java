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
 * An implementer of this interface is capable of computing the described "distance" values, which
 * are meant to provide both actual distance values, as well as distance estimates that can be
 * computed more cheaply.
 *
 * @lucene.experimental
 */
public interface GeoDistance extends Membership {

  // The following methods compute distances from the shape to a point
  // expected to be INSIDE the shape.  Typically a value of Double.POSITIVE_INFINITY
  // is returned for points that happen to be outside the shape.

  /**
   * Compute this shape's <em>internal</em> "distance" to the GeoPoint. Implementations should
   * clarify how this is computed when it's non-obvious. A return value of Double.POSITIVE_INFINITY
   * should be returned for points outside of the shape.
   *
   * @param distanceStyle is the distance style.
   * @param point is the point to compute the distance to.
   * @return the distance.
   */
  public default double computeDistance(final DistanceStyle distanceStyle, final GeoPoint point) {
    return computeDistance(distanceStyle, point.x, point.y, point.z);
  }

  /**
   * Compute this shape's <em>internal</em> "distance" to the GeoPoint. Implementations should
   * clarify how this is computed when it's non-obvious. A return value of Double.POSITIVE_INFINITY
   * should be returned for points outside of the shape.
   *
   * @param distanceStyle is the distance style.
   * @param x is the point's unit x coordinate (using U.S. convention).
   * @param y is the point's unit y coordinate (using U.S. convention).
   * @param z is the point's unit z coordinate (using U.S. convention).
   * @return the distance.
   */
  public double computeDistance(
      final DistanceStyle distanceStyle, final double x, final double y, final double z);

  /**
   * Compute the shape's <em>delta</em> distance given a point. This is defined as the distance that
   * someone traveling the "length" of the shape would have to go out of their way to include the
   * point. For some shapes, e.g. paths, this makes perfect sense. For other shapes, e.g. circles,
   * the "length" of the shape is zero, and the delta is computed as the distance from the center to
   * the point and back. A return value of Double.POSITIVE_INFINITY should be returned for points
   * outside of the shape.
   *
   * @param distanceStyle is the distance style.
   * @param point is the point to compute the distance to.
   * @return the distance.
   */
  public default double computeDeltaDistance(
      final DistanceStyle distanceStyle, final GeoPoint point) {
    return computeDeltaDistance(distanceStyle, point.x, point.y, point.z);
  }

  /**
   * Compute the shape's <em>delta</em> distance given a point. This is defined as the distance that
   * someone traveling the "length" of the shape would have to go out of their way to include the
   * point. For some shapes, e.g. paths, this makes perfect sense. For other shapes, e.g. circles,
   * the "length" of the shape is zero, and the delta is computed as the distance from the center to
   * the point and back. A return value of Double.POSITIVE_INFINITY should be returned for points
   * outside of the shape.
   *
   * @param distanceStyle is the distance style.
   * @param x is the point's unit x coordinate (using U.S. convention).
   * @param y is the point's unit y coordinate (using U.S. convention).
   * @param z is the point's unit z coordinate (using U.S. convention).
   * @return the distance.
   */
  public default double computeDeltaDistance(
      final DistanceStyle distanceStyle, final double x, final double y, final double z) {
    // Default to standard distance * 2, which is fine for circles
    return computeDistance(distanceStyle, x, y, z) * 2.0;
  }
}
