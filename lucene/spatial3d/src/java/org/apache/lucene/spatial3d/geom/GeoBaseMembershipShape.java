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
 * Membership shapes have capabilities of both geohashing and membership determination. This is a
 * useful baseclass for them.
 *
 * @lucene.experimental
 */
public abstract class GeoBaseMembershipShape extends GeoBaseShape implements GeoMembershipShape {

  /**
   * Constructor.
   *
   * @param planetModel is the planet model to use.
   */
  public GeoBaseMembershipShape(final PlanetModel planetModel) {
    super(planetModel);
  }

  @Override
  public boolean isWithin(Vector point) {
    return isWithin(point.x, point.y, point.z);
  }

  @Override
  public double computeOutsideDistance(final DistanceStyle distanceStyle, final GeoPoint point) {
    return computeOutsideDistance(distanceStyle, point.x, point.y, point.z);
  }

  @Override
  public double computeOutsideDistance(
      final DistanceStyle distanceStyle, final double x, final double y, final double z) {
    if (isWithin(x, y, z)) {
      return 0.0;
    }
    return outsideDistance(distanceStyle, x, y, z);
  }

  /** Called by a {@code computeOutsideDistance} method if X/Y/Z is not within this shape. */
  protected abstract double outsideDistance(
      final DistanceStyle distanceStyle, final double x, final double y, final double z);
}
