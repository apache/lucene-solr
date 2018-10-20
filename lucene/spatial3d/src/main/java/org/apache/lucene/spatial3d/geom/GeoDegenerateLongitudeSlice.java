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

import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;

/**
 * Degenerate longitude slice.
 *
 * @lucene.internal
 */
class GeoDegenerateLongitudeSlice extends GeoBaseBBox {
  /** The longitude of the slice */
  protected final double longitude;

  /** The bounding plane for the slice (through both poles, perpendicular to the slice) */
  protected final SidedPlane boundingPlane;
  /** The plane of the slice */
  protected final Plane plane;
  /** A point on the slice */
  protected final GeoPoint interiorPoint;
  /** An array consisting of the one point chosen on the slice */
  protected final GeoPoint[] edgePoints;
  /** Notable points for the slice (north and south poles) */
  protected final GeoPoint[] planePoints;

  /**
   * Accepts only values in the following ranges: lon: {@code -PI -> PI}
   */
  public GeoDegenerateLongitudeSlice(final PlanetModel planetModel, final double longitude) {
    super(planetModel);
    // Argument checking
    if (longitude < -Math.PI || longitude > Math.PI)
      throw new IllegalArgumentException("Longitude out of range");
    this.longitude = longitude;

    final double sinLongitude = Math.sin(longitude);
    final double cosLongitude = Math.cos(longitude);

    this.plane = new Plane(cosLongitude, sinLongitude);
    // We need a bounding plane too, which is perpendicular to the longitude plane and sided so that the point (0.0, longitude) is inside.
    this.interiorPoint = new GeoPoint(planetModel, 0.0, sinLongitude, 1.0, cosLongitude);
    this.boundingPlane = new SidedPlane(interiorPoint, -sinLongitude, cosLongitude);
    this.edgePoints = new GeoPoint[]{interiorPoint};
    this.planePoints = new GeoPoint[]{planetModel.NORTH_POLE, planetModel.SOUTH_POLE};
  }

  /**
   * Constructor for deserialization.
   * @param planetModel is the planet model.
   * @param inputStream is the input stream.
   */
  public GeoDegenerateLongitudeSlice(final PlanetModel planetModel, final InputStream inputStream) throws IOException {
    this(planetModel, SerializableObject.readDouble(inputStream));
  }

  @Override
  public void write(final OutputStream outputStream) throws IOException {
    SerializableObject.writeDouble(outputStream, longitude);
  }

  @Override
  public GeoBBox expand(final double angle) {
    // Figuring out when we escalate to a special case requires some prefiguring
    double newLeftLon = longitude - angle;
    double newRightLon = longitude + angle;
    double currentLonSpan = 2.0 * angle;
    if (currentLonSpan + 2.0 * angle >= Math.PI * 2.0) {
      newLeftLon = -Math.PI;
      newRightLon = Math.PI;
    }
    return GeoBBoxFactory.makeGeoBBox(planetModel, Math.PI * 0.5, -Math.PI * 0.5, newLeftLon, newRightLon);
  }

  @Override
  public boolean isWithin(final double x, final double y, final double z) {
    return plane.evaluateIsZero(x, y, z) &&
        boundingPlane.isWithin(x, y, z);
  }

  @Override
  public double getRadius() {
    return Math.PI * 0.5;
  }

  @Override
  public GeoPoint getCenter() {
    return interiorPoint;
  }

  @Override
  public GeoPoint[] getEdgePoints() {
    return edgePoints;
  }

  @Override
  public boolean intersects(final Plane p, final GeoPoint[] notablePoints, final Membership... bounds) {
    return p.intersects(planetModel, plane, notablePoints, planePoints, bounds, boundingPlane);
  }

  @Override
  public boolean intersects(final GeoShape geoShape) {
    return geoShape.intersects(plane, planePoints, boundingPlane);
  }

  @Override
  public void getBounds(Bounds bounds) {
    super.getBounds(bounds);
    bounds
      .addVerticalPlane(planetModel, longitude, plane, boundingPlane)
      .addPoint(planetModel.NORTH_POLE).addPoint(planetModel.SOUTH_POLE);
  }

  @Override
  public int getRelationship(final GeoShape path) {
    // Look for intersections.
    if (intersects(path))
      return OVERLAPS;

    if (path.isWithin(interiorPoint))
      return CONTAINS;

    return DISJOINT;
  }

  @Override
  protected double outsideDistance(final DistanceStyle distanceStyle, final double x, final double y, final double z) {
    final double distance = distanceStyle.computeDistance(planetModel, plane, x,y,z, boundingPlane);
    
    final double northDistance = distanceStyle.computeDistance(planetModel.NORTH_POLE, x,y,z);
    final double southDistance = distanceStyle.computeDistance(planetModel.SOUTH_POLE, x,y,z);
    
    return Math.min(
      distance,
      Math.min(northDistance, southDistance));
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof GeoDegenerateLongitudeSlice))
      return false;
    GeoDegenerateLongitudeSlice other = (GeoDegenerateLongitudeSlice) o;
    return super.equals(other) && other.longitude == longitude;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    long temp = Double.doubleToLongBits(longitude);
    result = result * 31 + (int) (temp ^ (temp >>> 32));
    return result;
  }

  @Override
  public String toString() {
    return "GeoDegenerateLongitudeSlice: {planetmodel="+planetModel+", longitude=" + longitude + "(" + longitude * 180.0 / Math.PI + ")}";
  }
}
  

