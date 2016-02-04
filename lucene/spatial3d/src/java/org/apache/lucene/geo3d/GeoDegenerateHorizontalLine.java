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
 * Degenerate bounding box limited on two sides (left lon, right lon).
 * The left-right maximum extent for this shape is PI; for anything larger, use
 * GeoWideDegenerateHorizontalLine.
 *
 * @lucene.internal
 */
public class GeoDegenerateHorizontalLine extends GeoBaseBBox {
  /** Latitude of horizontal line */
  protected final double latitude;
  /** Left bounding longitude of line */
  protected final double leftLon;
  /** Right bounding longitude of line */
  protected final double rightLon;

  /** Left hand endpoint of line */
  protected final GeoPoint LHC;
  /** Right hand endpoint of line */
  protected final GeoPoint RHC;

  /** The plane describing the line */
  protected final Plane plane;
  /** The left side end plane */
  protected final SidedPlane leftPlane;
  /** The right side end plane */
  protected final SidedPlane rightPlane;

  /** Notable points for the line */
  protected final GeoPoint[] planePoints;

  /** Center of line */
  protected final GeoPoint centerPoint;
  /** A point that's on the line */
  protected final GeoPoint[] edgePoints;

  /**
   * Accepts only values in the following ranges: lat: {@code -PI/2 -> PI/2}, lon: {@code -PI -> PI}
   *@param planetModel is the planet model.
   *@param latitude is the latitude of the line.
   *@param leftLon is the left end longitude.
   *@param rightLon is the right end longitude.
   */
  public GeoDegenerateHorizontalLine(final PlanetModel planetModel, final double latitude, final double leftLon, double rightLon) {
    super(planetModel);
    // Argument checking
    if (latitude > Math.PI * 0.5 || latitude < -Math.PI * 0.5)
      throw new IllegalArgumentException("Latitude out of range");
    if (leftLon < -Math.PI || leftLon > Math.PI)
      throw new IllegalArgumentException("Left longitude out of range");
    if (rightLon < -Math.PI || rightLon > Math.PI)
      throw new IllegalArgumentException("Right longitude out of range");
    double extent = rightLon - leftLon;
    if (extent < 0.0) {
      extent += 2.0 * Math.PI;
    }
    if (extent > Math.PI)
      throw new IllegalArgumentException("Width of rectangle too great");

    this.latitude = latitude;
    this.leftLon = leftLon;
    this.rightLon = rightLon;

    final double sinLatitude = Math.sin(latitude);
    final double cosLatitude = Math.cos(latitude);
    final double sinLeftLon = Math.sin(leftLon);
    final double cosLeftLon = Math.cos(leftLon);
    final double sinRightLon = Math.sin(rightLon);
    final double cosRightLon = Math.cos(rightLon);

    // Now build the two points
    this.LHC = new GeoPoint(planetModel, sinLatitude, sinLeftLon, cosLatitude, cosLeftLon, latitude, leftLon);
    this.RHC = new GeoPoint(planetModel, sinLatitude, sinRightLon, cosLatitude, cosRightLon, latitude, rightLon);

    this.plane = new Plane(planetModel, sinLatitude);

    // Normalize
    while (leftLon > rightLon) {
      rightLon += Math.PI * 2.0;
    }
    final double middleLon = (leftLon + rightLon) * 0.5;
    final double sinMiddleLon = Math.sin(middleLon);
    final double cosMiddleLon = Math.cos(middleLon);

    this.centerPoint = new GeoPoint(planetModel, sinLatitude, sinMiddleLon, cosLatitude, cosMiddleLon);
    this.leftPlane = new SidedPlane(centerPoint, cosLeftLon, sinLeftLon);
    this.rightPlane = new SidedPlane(centerPoint, cosRightLon, sinRightLon);

    this.planePoints = new GeoPoint[]{LHC, RHC};

    this.edgePoints = new GeoPoint[]{centerPoint};
  }

  @Override
  public GeoBBox expand(final double angle) {
    double newTopLat = latitude + angle;
    double newBottomLat = latitude - angle;
    // Figuring out when we escalate to a special case requires some prefiguring
    double currentLonSpan = rightLon - leftLon;
    if (currentLonSpan < 0.0)
      currentLonSpan += Math.PI * 2.0;
    double newLeftLon = leftLon - angle;
    double newRightLon = rightLon + angle;
    if (currentLonSpan + 2.0 * angle >= Math.PI * 2.0) {
      newLeftLon = -Math.PI;
      newRightLon = Math.PI;
    }
    return GeoBBoxFactory.makeGeoBBox(planetModel, newTopLat, newBottomLat, newLeftLon, newRightLon);
  }

  @Override
  public boolean isWithin(final double x, final double y, final double z) {
    return plane.evaluateIsZero(x, y, z) &&
        leftPlane.isWithin(x, y, z) &&
        rightPlane.isWithin(x, y, z);
  }

  @Override
  public double getRadius() {
    double topAngle = centerPoint.arcDistance(RHC);
    double bottomAngle = centerPoint.arcDistance(LHC);
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
    return p.intersects(planetModel, plane, notablePoints, planePoints, bounds, leftPlane, rightPlane);
  }

  @Override
  public void getBounds(Bounds bounds) {
    super.getBounds(bounds);
    bounds.addHorizontalPlane(planetModel, latitude, plane, leftPlane, rightPlane)
      .addPoint(LHC).addPoint(RHC);
  }

  @Override
  public int getRelationship(final GeoShape path) {
    //System.err.println("getting relationship between "+this+" and "+path);
    if (path.intersects(plane, planePoints, leftPlane, rightPlane)) {
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
    final double distance = distanceStyle.computeDistance(planetModel, plane, x,y,z, leftPlane, rightPlane);
    
    final double LHCDistance = distanceStyle.computeDistance(LHC, x,y,z);
    final double RHCDistance = distanceStyle.computeDistance(RHC, x,y,z);
    
    return Math.min(
      distance,
      Math.min(LHCDistance, RHCDistance));
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof GeoDegenerateHorizontalLine))
      return false;
    GeoDegenerateHorizontalLine other = (GeoDegenerateHorizontalLine) o;
    return super.equals(other) && other.LHC.equals(LHC) && other.RHC.equals(RHC);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + LHC.hashCode();
    result = 31 * result + RHC.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "GeoDegenerateHorizontalLine: {planetmodel="+planetModel+", latitude=" + latitude + "(" + latitude * 180.0 / Math.PI + "), leftlon=" + leftLon + "(" + leftLon * 180.0 / Math.PI + "), rightLon=" + rightLon + "(" + rightLon * 180.0 / Math.PI + ")}";
  }
}
  

