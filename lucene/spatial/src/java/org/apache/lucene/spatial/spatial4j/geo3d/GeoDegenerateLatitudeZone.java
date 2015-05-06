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
 * This GeoBBox represents an area rectangle of one specific latitude with
 * no longitude bounds.
 *
 * @lucene.internal
 */
public class GeoDegenerateLatitudeZone extends GeoBBoxBase {
  public final double latitude;

  public final double sinLatitude;
  public final Plane plane;
  public final GeoPoint interiorPoint;
  public final GeoPoint[] edgePoints;
  public final static GeoPoint[] planePoints = new GeoPoint[0];

  public GeoDegenerateLatitudeZone(final double latitude) {
    this.latitude = latitude;

    this.sinLatitude = Math.sin(latitude);
    double cosLatitude = Math.cos(latitude);
    this.plane = new Plane(sinLatitude);
    // Compute an interior point.
    interiorPoint = new GeoPoint(cosLatitude, 0.0, sinLatitude);
    edgePoints = new GeoPoint[]{interiorPoint};
  }

  @Override
  public GeoBBox expand(final double angle) {
    double newTopLat = latitude + angle;
    double newBottomLat = latitude - angle;
    return GeoBBoxFactory.makeGeoBBox(newTopLat, newBottomLat, -Math.PI, Math.PI);
  }

  @Override
  public boolean isWithin(final Vector point) {
    return Math.abs(point.z - this.sinLatitude) < 1e-10;
  }

  @Override
  public boolean isWithin(final double x, final double y, final double z) {
    return Math.abs(z - this.sinLatitude) < 1e-10;
  }

  @Override
  public double getRadius() {
    return Math.PI;
  }

  /**
   * Returns the center of a circle into which the area will be inscribed.
   *
   * @return the center.
   */
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
    return p.intersects(plane, notablePoints, planePoints, bounds);
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
    bounds.noLongitudeBound().addLatitudeZone(latitude);
    return bounds;
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
  public boolean equals(Object o) {
    if (!(o instanceof GeoDegenerateLatitudeZone))
      return false;
    GeoDegenerateLatitudeZone other = (GeoDegenerateLatitudeZone) o;
    return other.latitude == latitude;
  }

  @Override
  public int hashCode() {
    long temp = Double.doubleToLongBits(latitude);
    int result = (int) (temp ^ (temp >>> 32));
    return result;
  }

  @Override
  public String toString() {
    return "GeoDegenerateLatitudeZone: {lat=" + latitude + "(" + latitude * 180.0 / Math.PI + ")}";
  }
}

