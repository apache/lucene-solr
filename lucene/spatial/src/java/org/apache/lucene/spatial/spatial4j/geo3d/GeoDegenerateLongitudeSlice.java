package org.apache.lucene.spatial.spatial4j.geo3d;

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

/**
 * Degenerate longitude slice.
 */
public class GeoDegenerateLongitudeSlice extends GeoBBoxBase {
  public final double longitude;

  public final double sinLongitude;
  public final double cosLongitude;
  public final SidedPlane boundingPlane;
  public final Plane plane;
  public final GeoPoint interiorPoint;
  public final GeoPoint[] edgePoints;

  public final static GeoPoint[] planePoints = new GeoPoint[]{NORTH_POLE, SOUTH_POLE};

  /**
   * Accepts only values in the following ranges: lon: {@code -PI -> PI}
   */
  public GeoDegenerateLongitudeSlice(final double longitude) {
    // Argument checking
    if (longitude < -Math.PI || longitude > Math.PI)
      throw new IllegalArgumentException("Longitude out of range");
    this.longitude = longitude;

    this.sinLongitude = Math.sin(longitude);
    this.cosLongitude = Math.cos(longitude);

    this.plane = new Plane(cosLongitude, sinLongitude);
    // We need a bounding plane too, which is perpendicular to the longitude plane and sided so that the point (0.0, longitude) is inside.
    this.interiorPoint = new GeoPoint(cosLongitude, sinLongitude, 0.0);
    this.boundingPlane = new SidedPlane(interiorPoint, -sinLongitude, cosLongitude);
    this.edgePoints = new GeoPoint[]{interiorPoint};
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
    return GeoBBoxFactory.makeGeoBBox(Math.PI * 0.5, -Math.PI * 0.5, newLeftLon, newRightLon);
  }

  @Override
  public boolean isWithin(final Vector point) {
    return plane.evaluateIsZero(point) &&
        boundingPlane.isWithin(point);
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

  /**
   * Returns the center of a circle into which the area will be inscribed.
   *
   * @return the center.
   */
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
    return p.intersects(plane, notablePoints, planePoints, bounds, boundingPlane);
  }

  /**
   * Compute longitude/latitude bounds for the shape.
   *
   * @param bounds is the optional input bounds object.  If this is null,
   *               a bounds object will be created.  Otherwise, the input object will be modified.
   * @return a Bounds object describing the shape's bounds.  If the bounds cannot
   * be computed, then return a Bounds object with noLongitudeBound,
   * noTopLatitudeBound, and noBottomLatitudeBound.
   */
  @Override
  public Bounds getBounds(Bounds bounds) {
    if (bounds == null)
      bounds = new Bounds();
    bounds.noTopLatitudeBound().noBottomLatitudeBound();
    bounds.addLongitudeSlice(longitude, longitude);
    return bounds;
  }

  @Override
  public int getRelationship(final GeoShape path) {
    // Look for intersections.
    if (path.intersects(plane, planePoints, boundingPlane))
      return OVERLAPS;

    if (path.isWithin(interiorPoint))
      return CONTAINS;

    return DISJOINT;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof GeoDegenerateLongitudeSlice))
      return false;
    GeoDegenerateLongitudeSlice other = (GeoDegenerateLongitudeSlice) o;
    return other.longitude == longitude;
  }

  @Override
  public int hashCode() {
    int result;
    long temp;
    temp = Double.doubleToLongBits(longitude);
    result = (int) (temp ^ (temp >>> 32));
    return result;
  }

  @Override
  public String toString() {
    return "GeoDegenerateLongitudeSlice: {longitude=" + longitude + "(" + longitude * 180.0 / Math.PI + ")}";
  }
}
  

