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
 * Linear distance computation style.
 *
 * @lucene.experimental
 */
public class LinearDistance implements DistanceStyle {

  /** A convenient instance */
  public static final LinearDistance INSTANCE = new LinearDistance();

  /** Constructor. */
  public LinearDistance() {}

  @Override
  public double computeDistance(final GeoPoint point1, final GeoPoint point2) {
    return point1.linearDistance(point2);
  }

  @Override
  public double computeDistance(
      final GeoPoint point1, final double x2, final double y2, final double z2) {
    return point1.linearDistance(x2, y2, z2);
  }

  @Override
  public double computeDistance(
      final PlanetModel planetModel,
      final Plane plane,
      final GeoPoint point,
      final Membership... bounds) {
    return plane.linearDistance(planetModel, point, bounds);
  }

  @Override
  public double computeDistance(
      final PlanetModel planetModel,
      final Plane plane,
      final double x,
      final double y,
      final double z,
      final Membership... bounds) {
    return plane.linearDistance(planetModel, x, y, z, bounds);
  }

  @Override
  public GeoPoint[] findDistancePoints(
      final PlanetModel planetModel,
      final double distanceValue,
      final GeoPoint startPoint,
      final Plane plane,
      final Membership... bounds) {
    throw new IllegalStateException("Reverse mapping not implemented for this distance metric");
  }

  @Override
  public double findMinimumArcDistance(final PlanetModel planetModel, final double distanceValue) {
    throw new IllegalStateException("Reverse mapping not implemented for this distance metric");
  }

  @Override
  public double findMaximumArcDistance(final PlanetModel planetModel, final double distanceValue) {
    throw new IllegalStateException("Reverse mapping not implemented for this distance metric");
  }
}
