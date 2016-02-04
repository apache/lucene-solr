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
 * Bounding box including the entire world.
 *
 * @lucene.internal
 */
public class GeoWorld extends GeoBaseBBox {
  /** No points on the edge of the shape */
  protected final static GeoPoint[] edgePoints = new GeoPoint[0];
  /** Point in the middle of the world */
  protected final GeoPoint originPoint;
  
  /** Constructor.
   *@param planetModel is the planet model.
   */
  public GeoWorld(final PlanetModel planetModel) {
    super(planetModel);
    originPoint = new GeoPoint(planetModel.ab, 1.0, 0.0, 0.0);
  }

  @Override
  public GeoBBox expand(final double angle) {
    return this;
  }

  @Override
  public double getRadius() {
    return Math.PI;
  }

  @Override
  public GeoPoint getCenter() {
    // Totally arbitrary
    return originPoint;
  }

  @Override
  public boolean isWithin(final double x, final double y, final double z) {
    return true;
  }

  @Override
  public GeoPoint[] getEdgePoints() {
    return edgePoints;
  }

  @Override
  public boolean intersects(final Plane p, final GeoPoint[] notablePoints, final Membership... bounds) {
    return false;
  }

  @Override
  public void getBounds(Bounds bounds) {
    super.getBounds(bounds);
    // Unnecessary
    //bounds.noLongitudeBound().noTopLatitudeBound().noBottomLatitudeBound();
  }

  @Override
  public int getRelationship(final GeoShape path) {
    if (path.getEdgePoints().length > 0)
      // Path is always within the world
      return WITHIN;

    return OVERLAPS;
  }

  @Override
  protected double outsideDistance(final DistanceStyle distanceStyle, final double x, final double y, final double z) {
    return 0.0;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof GeoWorld))
      return false;
    return super.equals(o);
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }

  @Override
  public String toString() {
    return "GeoWorld: {planetmodel="+planetModel+"}";
  }
}
