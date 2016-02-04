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
package org.apache.lucene.geo3d;

/**
 * Distance shapes have capabilities of both geohashing and distance
 * computation (which also includes point membership determination).
 *
 * @lucene.experimental
 */
public abstract class GeoBaseDistanceShape extends GeoBaseMembershipShape implements GeoDistanceShape {

  /** Constructor.
   *@param planetModel is the planet model to use.
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
  public double computeDistance(final DistanceStyle distanceStyle, final double x, final double y, final double z) {
    if (!isWithin(x,y,z)) {
      return Double.MAX_VALUE;
    }
    return distance(distanceStyle, x, y, z);
  }

  /** Called by a {@code computeDistance} method if X/Y/Z is not within this shape. */
  protected abstract double distance(final DistanceStyle distanceStyle, final double x, final double y, final double z);

}

