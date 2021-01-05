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
 * Distance shapes have capabilities of both geohashing and distance computation (which also
 * includes point membership determination).
 *
 * @lucene.experimental
 */
public abstract class GeoBaseDistanceShape extends GeoBaseAreaShape implements GeoDistanceShape {

  /**
   * Constructor.
   *
   * @param planetModel is the planet model to use.
   */
  public GeoBaseDistanceShape(final PlanetModel planetModel) {
    super(planetModel);
  }

  @Override
  public boolean isWithin(Vector point) {
    return isWithin(point.x, point.y, point.z);
  }

  @Override
  public double computeDistance(final DistanceStyle distanceStyle, final GeoPoint point) {
    return computeDistance(distanceStyle, point.x, point.y, point.z);
  }

  @Override
  public double computeDistance(
      final DistanceStyle distanceStyle, final double x, final double y, final double z) {
    if (!isWithin(x, y, z)) {
      return Double.POSITIVE_INFINITY;
    }
    return distance(distanceStyle, x, y, z);
  }

  /** Called by a {@code computeDistance} method if X/Y/Z is within this shape. */
  protected abstract double distance(
      final DistanceStyle distanceStyle, final double x, final double y, final double z);

  @Override
  public double computeDeltaDistance(final DistanceStyle distanceStyle, final GeoPoint point) {
    return computeDeltaDistance(distanceStyle, point.x, point.y, point.z);
  }

  @Override
  public double computeDeltaDistance(
      final DistanceStyle distanceStyle, final double x, final double y, final double z) {
    if (!isWithin(x, y, z)) {
      return Double.POSITIVE_INFINITY;
    }
    return deltaDistance(distanceStyle, x, y, z);
  }

  /** Called by a {@code computeDeltaDistance} method if X/Y/Z is within this shape. */
  protected double deltaDistance(
      final DistanceStyle distanceStyle, final double x, final double y, final double z) {
    return distance(distanceStyle, x, y, z) * 2.0;
  }

  @Override
  public void getDistanceBounds(
      final Bounds bounds, final DistanceStyle distanceStyle, final double distanceValue) {
    if (distanceValue == Double.POSITIVE_INFINITY) {
      getBounds(bounds);
      return;
    }
    distanceBounds(bounds, distanceStyle, distanceValue);
  }

  /**
   * Called by a {@code getDistanceBounds} method if distanceValue is not Double.POSITIVE_INFINITY.
   */
  protected abstract void distanceBounds(
      final Bounds bounds, final DistanceStyle distanceStyle, final double distanceValue);
}
