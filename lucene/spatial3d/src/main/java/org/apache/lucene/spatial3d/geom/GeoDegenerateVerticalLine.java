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
 * Degenerate bounding box limited on two sides (top lat, bottom lat).
 *
 * @lucene.internal
 */
public class GeoDegenerateVerticalLine extends GeoBaseBBox {
  /** Top latitude of the vertical line */
  protected final double topLat;
  /** Bottom latitude of the vertical line */
  protected final double bottomLat;
  /** Longitude of the vertical line */
  protected final double longitude;

  /** Point at the upper end of the vertical line */
  protected final GeoPoint UHC;
  /** Point at the lower end of the vertical line */
  protected final GeoPoint LHC;

  /** Top end cutoff plane */
  protected final SidedPlane topPlane;
  /** Bottom end cutoff plane */
  protected final SidedPlane bottomPlane;
  /** Back-side cutoff plane */
  protected final SidedPlane boundingPlane;
  /** The vertical line plane */
  protected final Plane plane;
  /** Notable points for the line (end points) */
  protected final GeoPoint[] planePoints;
  /** A computed center point for the line */
  protected final GeoPoint centerPoint;
  /** A point that's on the line */
  protected final GeoPoint[] edgePoints;

  /**
   * Accepts only values in the following ranges: lat: {@code -PI/2 -> PI/2}, longitude: {@code -PI -> PI}
   */
  public GeoDegenerateVerticalLine(final PlanetModel planetModel, final double topLat, final double bottomLat, final double longitude) {
    super(planetModel);
    // Argument checking
    if (topLat > Math.PI * 0.5 || topLat < -Math.PI * 0.5)
      throw new IllegalArgumentException("Top latitude out of range");
    if (bottomLat > Math.PI * 0.5 || bottomLat < -Math.PI * 0.5)
      throw new IllegalArgumentException("Bottom latitude out of range");
    if (topLat < bottomLat)
      throw new IllegalArgumentException("Top latitude less than bottom latitude");
    if (longitude < -Math.PI || longitude > Math.PI)
      throw new IllegalArgumentException("Longitude out of range");

    this.topLat = topLat;
    this.bottomLat = bottomLat;
    this.longitude = longitude;

    final double sinTopLat = Math.sin(topLat);
    final double cosTopLat = Math.cos(topLat);
    final double sinBottomLat = Math.sin(bottomLat);
    final double cosBottomLat = Math.cos(bottomLat);
    final double sinLongitude = Math.sin(longitude);
    final double cosLongitude = Math.cos(longitude);

    // Now build the two points
    this.UHC = new GeoPoint(planetModel, sinTopLat, sinLongitude, cosTopLat, cosLongitude, topLat, longitude);
    this.LHC = new GeoPoint(planetModel, sinBottomLat, sinLongitude, cosBottomLat, cosLongitude, bottomLat, longitude);

    this.plane = new Plane(cosLongitude, sinLongitude);

    final double middleLat = (topLat + bottomLat) * 0.5;
    final double sinMiddleLat = Math.sin(middleLat);
    final double cosMiddleLat = Math.cos(middleLat);

    this.centerPoint = new GeoPoint(planetModel, sinMiddleLat, sinLongitude, cosMiddleLat, cosLongitude);

    this.topPlane = new SidedPlane(centerPoint, planetModel, sinTopLat);
    this.bottomPlane = new SidedPlane(centerPoint, planetModel, sinBottomLat);

    this.boundingPlane = new SidedPlane(centerPoint, -sinLongitude, cosLongitude);

    this.planePoints = new GeoPoint[]{UHC, LHC};

    this.edgePoints = new GeoPoint[]{centerPoint};
  }

  /**
   * Constructor for deserialization.
   * @param planetModel is the planet model.
   * @param inputStream is the input stream.
   */
  public GeoDegenerateVerticalLine(final PlanetModel planetModel, final InputStream inputStream) throws IOException {
    this(planetModel, SerializableObject.readDouble(inputStream), SerializableObject.readDouble(inputStream), SerializableObject.readDouble(inputStream));
  }

  @Override
  public void write(final OutputStream outputStream) throws IOException {
    SerializableObject.writeDouble(outputStream, topLat);
    SerializableObject.writeDouble(outputStream, bottomLat);
    SerializableObject.writeDouble(outputStream, longitude);
  }

  @Override
  public GeoBBox expand(final double angle) {
    final double newTopLat = topLat + angle;
    final double newBottomLat = bottomLat - angle;
    double newLeftLon = longitude - angle;
    double newRightLon = longitude + angle;
    double currentLonSpan = 2.0 * angle;
    if (currentLonSpan + 2.0 * angle >= Math.PI * 2.0) {
      newLeftLon = -Math.PI;
      newRightLon = Math.PI;
    }
    return GeoBBoxFactory.makeGeoBBox(planetModel, newTopLat, newBottomLat, newLeftLon, newRightLon);
  }

  @Override
  public boolean isWithin(final double x, final double y, final double z) {
    return plane.evaluateIsZero(x, y, z) &&
        boundingPlane.isWithin(x, y, z) &&
        topPlane.isWithin(x, y, z) &&
        bottomPlane.isWithin(x, y, z);
  }

  @Override
  public double getRadius() {
    // Here we compute the distance from the middle point to one of the corners.  However, we need to be careful
    // to use the longest of three distances: the distance to a corner on the top; the distnace to a corner on the bottom, and
    // the distance to the right or left edge from the center.
    final double topAngle = centerPoint.arcDistance(UHC);
    final double bottomAngle = centerPoint.arcDistance(LHC);
    return Math.max(topAngle, bottomAngle);
  }

  @Override
  public GeoPoint getCenter() {
    return centerPoint;
  }

  @Override
  public GeoPoint[] getEdgePoints() {
    return edgePoints;
  }

  @Override
  public boolean intersects(final Plane p, final GeoPoint[] notablePoints, final Membership... bounds) {
    return p.intersects(planetModel, plane, notablePoints, planePoints, bounds, boundingPlane, topPlane, bottomPlane);
  }

  @Override
  public boolean intersects(final GeoShape geoShape) {
    return geoShape.intersects(plane, planePoints, boundingPlane, topPlane, bottomPlane);
  }

  @Override
  public void getBounds(Bounds bounds) {
    super.getBounds(bounds);
    bounds.addVerticalPlane(planetModel, longitude, plane, boundingPlane, topPlane, bottomPlane)
      .addPoint(UHC).addPoint(LHC);
  }

  @Override
  public int getRelationship(final GeoShape path) {
    //System.err.println(this+" relationship to "+path);
    if (intersects(path)) {
      //System.err.println(" overlaps");
      return OVERLAPS;
    }

    if (path.isWithin(centerPoint)) {
      //System.err.println(" contains");
      return CONTAINS;
    }

    //System.err.println(" disjoint");
    return DISJOINT;
  }

  @Override
  protected double outsideDistance(final DistanceStyle distanceStyle, final double x, final double y, final double z) {
    final double distance = distanceStyle.computeDistance(planetModel, plane, x,y,z, topPlane, bottomPlane, boundingPlane);
    
    final double UHCDistance = distanceStyle.computeDistance(UHC, x,y,z);
    final double LHCDistance = distanceStyle.computeDistance(LHC, x,y,z);
    
    return Math.min(
      distance,
      Math.min(UHCDistance, LHCDistance));
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof GeoDegenerateVerticalLine))
      return false;
    GeoDegenerateVerticalLine other = (GeoDegenerateVerticalLine) o;
    return super.equals(other) && other.UHC.equals(UHC) && other.LHC.equals(LHC);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + UHC.hashCode();
    result = 31 * result + LHC.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "GeoDegenerateVerticalLine: {longitude=" + longitude + "(" + longitude * 180.0 / Math.PI + "), toplat=" + topLat + "(" + topLat * 180.0 / Math.PI + "), bottomlat=" + bottomLat + "(" + bottomLat * 180.0 / Math.PI + ")}";
  }
}
  

