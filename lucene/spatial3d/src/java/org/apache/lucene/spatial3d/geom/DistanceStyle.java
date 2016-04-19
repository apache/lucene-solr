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
 * Distance computation styles, supporting various ways of computing
 * distance to shapes.
 *
 * @lucene.experimental
 */
public interface DistanceStyle {

  // convenient access to built-in styles:

  /** Arc distance calculator */
  public static final ArcDistance ARC = ArcDistance.INSTANCE;
  /** Linear distance calculator */
  public static final LinearDistance LINEAR = LinearDistance.INSTANCE;
  /** Linear distance squared calculator */
  public static final LinearSquaredDistance LINEAR_SQUARED = LinearSquaredDistance.INSTANCE;
  /** Normal distance calculator */
  public static final NormalDistance NORMAL = NormalDistance.INSTANCE;
  /** Normal distance squared calculator */
  public static final NormalSquaredDistance NORMAL_SQUARED = NormalSquaredDistance.INSTANCE;

  /** Compute the distance from a point to another point.
   * @param point1 Starting point
   * @param point2 Final point
   * @return the distance
   */
  public default double computeDistance(final GeoPoint point1, final GeoPoint point2) {
    return computeDistance(point1, point2.x, point2.y, point2.z);
  }
  
  /** Compute the distance from a point to another point.
   * @param point1 Starting point
   * @param x2 Final point x
   * @param y2 Final point y
   * @param z2 Final point z
   * @return the distance
   */
  public double computeDistance(final GeoPoint point1, final double x2, final double y2, final double z2);

  /** Compute the distance from a plane to a point.
   * @param planetModel The planet model
   * @param plane The plane
   * @param point The point
   * @param bounds are the plane bounds
   * @return the distance
   */
  public default double computeDistance(final PlanetModel planetModel, final Plane plane, final GeoPoint point,
                                        final Membership... bounds) {
    return computeDistance(planetModel, plane, point.x, point.y, point.z, bounds);
  }
  
  /** Compute the distance from a plane to a point.
   * @param planetModel The planet model
   * @param plane The plane
   * @param x The point x
   * @param y The point y
   * @param z The point z
   * @param bounds are the plane bounds
   * @return the distance
   */
  public double computeDistance(final PlanetModel planetModel, final Plane plane, final double x, final double y, final double z, final Membership... bounds);

  // The following methods are used to go from a distance value back to something
  // that can be used to construct a constrained shape.
  
  /**  Find a GeoPoint, at a specified distance from a starting point, within the
   * specified bounds.  The GeoPoint must be in the specified plane.
   * @param planetModel is the planet model.
   * @param distanceValue is the distance to set the new point at, measured from point1 and on the way to point2.
   * @param startPoint is the starting point.
   * @param plane is the plane that the point must be in.
   * @param bounds are the constraints on where the point can be found.
   * @return zero, one, or two points at the proper distance from startPoint.
   */
  public GeoPoint[] findDistancePoints(final PlanetModel planetModel, final double distanceValue, final GeoPoint startPoint, final Plane plane, final Membership... bounds);
  
  /** Given a distance metric, find the minimum arc distance represented by that distance metric.
   * @param planetModel is the planet model.
   * @param distanceValue is the distance metric.
   * @return the minimum arc distance that that distance value can represent given the planet model.
   */
  public double findMinimumArcDistance(final PlanetModel planetModel, final double distanceValue);
  
  /** Given a distance metric, find the maximum arc distance represented by the distance metric.
   * @param planetModel is the planet model.
   * @param distanceValue is the distance metric.
   * @return the maximum arc distance that that distance value can represent given the planet model.
   */
  public double findMaximumArcDistance(final PlanetModel planetModel, final double distanceValue);

}


