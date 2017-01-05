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
 * This class represents a degenerate point bounding box.
 * It is not a simple GeoPoint because we must have the latitude and longitude.
 *
 * @lucene.internal
 */
class GeoDegeneratePoint extends GeoPoint implements GeoBBox, GeoCircle {
  /** Current planet model, since we don't extend BasePlanetObject */
  protected final PlanetModel planetModel;
  /** Edge point is an area containing just this */
  protected final GeoPoint[] edgePoints;

  /** Constructor.
   *@param planetModel is the planet model to use.
   *@param lat is the latitude.
   *@param lon is the longitude.
   */
  public GeoDegeneratePoint(final PlanetModel planetModel, final double lat, final double lon) {
    super(planetModel, lat, lon);
    this.planetModel = planetModel;
    this.edgePoints = new GeoPoint[]{this};
  }

  @Override
  public GeoBBox expand(final double angle) {
    final double newTopLat = latitude + angle;
    final double newBottomLat = latitude - angle;
    final double newLeftLon = longitude - angle;
    final double newRightLon = longitude + angle;
    return GeoBBoxFactory.makeGeoBBox(planetModel, newTopLat, newBottomLat, newLeftLon, newRightLon);
  }

  @Override
  public GeoPoint[] getEdgePoints() {
    return edgePoints;
  }

  @Override
  public boolean intersects(final Plane plane, final GeoPoint[] notablePoints, final Membership... bounds) {
    // If not on the plane, no intersection
    if (!plane.evaluateIsZero(this))
      return false;

    for (Membership m : bounds) {
      if (!m.isWithin(this))
        return false;
    }
    return true;
  }

  @Override
  public void getBounds(Bounds bounds) {
    bounds.addPoint(this);
  }

  @Override
  public double computeOutsideDistance(final DistanceStyle distanceStyle, final GeoPoint point) {
    return distanceStyle.computeDistance(this, point);
  }

  @Override
  public double computeOutsideDistance(final DistanceStyle distanceStyle, final double x, final double y, final double z) {
    return distanceStyle.computeDistance(this, x,y,z);
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof GeoDegeneratePoint))
      return false;
    GeoDegeneratePoint other = (GeoDegeneratePoint) o;
    return super.equals(other) && other.latitude == latitude && other.longitude == longitude;
  }

  @Override
  public String toString() {
    return "GeoDegeneratePoint: {planetmodel="+planetModel+", lat=" + latitude + "(" + latitude * 180.0 / Math.PI + "), lon=" + longitude + "(" + longitude * 180.0 / Math.PI + ")}";
  }

  @Override
  public boolean isWithin(final Vector point) {
    return isWithin(point.x, point.y, point.z);
  }

  @Override
  public boolean isWithin(final double x, final double y, final double z) {
    return x == this.x && y == this.y && z == this.z;
  }

  @Override
  public double getRadius() {
    return 0.0;
  }

  @Override
  public GeoPoint getCenter() {
    return this;
  }

  @Override
  public int getRelationship(final GeoShape shape) {
    if (shape.isWithin(this)) {
      //System.err.println("Degenerate point "+this+" is WITHIN shape "+shape);
      return CONTAINS;
    }

    //System.err.println("Degenerate point "+this+" is NOT within shape "+shape);
    return DISJOINT;
  }

  @Override
  public double computeDistance(final DistanceStyle distanceStyle, final double x, final double y, final double z) {
    if (isWithin(x,y,z))
      return 0.0;
    return Double.POSITIVE_INFINITY;
  }
  
  @Override
  public void getDistanceBounds(final Bounds bounds, final DistanceStyle distanceStyle, final double distanceValue) {
    getBounds(bounds);
  }

}

