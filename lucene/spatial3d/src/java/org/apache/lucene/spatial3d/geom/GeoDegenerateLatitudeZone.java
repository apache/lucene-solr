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
 * This GeoBBox represents an area rectangle of one specific latitude with
 * no longitude bounds.
 *
 * @lucene.internal
 */
class GeoDegenerateLatitudeZone extends GeoBaseBBox {
  /** The latitude */
  protected final double latitude;
  /** Sine of the latitude */
  protected final double sinLatitude;
  /** Plane describing the latitude zone */
  protected final Plane plane;
  /** A point on the world that's also on the zone */
  protected final GeoPoint interiorPoint;
  /** An array consisting of the interiorPoint */
  protected final GeoPoint[] edgePoints;
  /** No notable points */
  protected final static GeoPoint[] planePoints = new GeoPoint[0];

  /** Constructor.
   *@param planetModel is the planet model to use.
   *@param latitude is the latitude of the latitude zone.
   */
  public GeoDegenerateLatitudeZone(final PlanetModel planetModel, final double latitude) {
    super(planetModel);
    this.latitude = latitude;

    this.sinLatitude = Math.sin(latitude);
    double cosLatitude = Math.cos(latitude);
    this.plane = new Plane(planetModel, sinLatitude);
    // Compute an interior point.
    interiorPoint = new GeoPoint(planetModel, sinLatitude, 0.0, cosLatitude, 1.0);
    edgePoints = new GeoPoint[]{interiorPoint};
  }

  @Override
  public GeoBBox expand(final double angle) {
    double newTopLat = latitude + angle;
    double newBottomLat = latitude - angle;
    return GeoBBoxFactory.makeGeoBBox(planetModel, newTopLat, newBottomLat, -Math.PI, Math.PI);
  }

  @Override
  public boolean isWithin(final double x, final double y, final double z) {
    return Math.abs(z - this.sinLatitude) < 1e-10;
  }

  @Override
  public double getRadius() {
    return Math.PI;
  }

  @Override
  public GeoPoint getCenter() {
    // Totally arbitrary
    return interiorPoint;
  }

  @Override
  public GeoPoint[] getEdgePoints() {
    return edgePoints;
  }

  @Override
  public boolean intersects(final Plane p, final GeoPoint[] notablePoints, final Membership... bounds) {
    return p.intersects(planetModel, plane, notablePoints, planePoints, bounds);
  }

  @Override
  public void getBounds(Bounds bounds) {
    super.getBounds(bounds);
    bounds.noLongitudeBound()
      .addHorizontalPlane(planetModel, latitude, plane);
  }

  @Override
  public int getRelationship(final GeoShape path) {
    // Second, the shortcut of seeing whether endpoints are in/out is not going to
    // work with no area endpoints.  So we rely entirely on intersections.
    //System.out.println("Got here! latitude="+latitude+" path="+path);

    if (path.intersects(plane, planePoints)) {
      return OVERLAPS;
    }

    if (path.isWithin(interiorPoint)) {
      return CONTAINS;
    }

    return DISJOINT;
  }

  @Override
  protected double outsideDistance(final DistanceStyle distanceStyle, final double x, final double y, final double z) {
    return distanceStyle.computeDistance(planetModel, plane, x,y,z);
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof GeoDegenerateLatitudeZone))
      return false;
    GeoDegenerateLatitudeZone other = (GeoDegenerateLatitudeZone) o;
    return super.equals(other) && other.latitude == latitude;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    long temp = Double.doubleToLongBits(latitude);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    return result;
  }

  @Override
  public String toString() {
    return "GeoDegenerateLatitudeZone: {planetmodel="+planetModel+", lat=" + latitude + "(" + latitude * 180.0 / Math.PI + ")}";
  }
}

