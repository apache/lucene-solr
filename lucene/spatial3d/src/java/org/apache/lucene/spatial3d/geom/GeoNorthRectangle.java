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
 * Bounding box limited on three sides (bottom lat, left lon, right lon), including
 * the north pole.
 * The left-right maximum extent for this shape is PI; for anything larger, use
 * {@link GeoWideNorthRectangle}.
 *
 * @lucene.internal
 */
class GeoNorthRectangle extends GeoBaseBBox {
  /** The bottom latitude of the rectangle */
  protected final double bottomLat;
  /** The left longitude */
  protected final double leftLon;
  /** The right longitude */
  protected final double rightLon;
  /** Cosine of the middle latitude */
  protected final double cosMiddleLat;
  /** Lower right hand corner point */
  protected final GeoPoint LRHC;
  /** Lower left hand corner point */
  protected final GeoPoint LLHC;
  /** Bottom edge plane */
  protected final SidedPlane bottomPlane;
  /** Left-side plane */
  protected final SidedPlane leftPlane;
  /** Right-side plane */
  protected final SidedPlane rightPlane;
  /** Bottom plane notable points */
  protected final GeoPoint[] bottomPlanePoints;
  /** Left plane notable points */
  protected final GeoPoint[] leftPlanePoints;
  /** Right plane notable points */
  protected final GeoPoint[] rightPlanePoints;
  /** Center point */
  protected final GeoPoint centerPoint;
  /** A point on the edge */
  protected final GeoPoint[] edgePoints;

  /**
   * Accepts only values in the following ranges: lat: {@code -PI/2 -> PI/2}, lon: {@code -PI -> PI}
   *@param planetModel is the planet model.
   *@param bottomLat is the bottom latitude.
   *@param leftLon is the left longitude.
   *@param rightLon is the right longitude.
   */
  public GeoNorthRectangle(final PlanetModel planetModel, final double bottomLat, final double leftLon, double rightLon) {
    super(planetModel);
    // Argument checking
    if (bottomLat > Math.PI * 0.5 || bottomLat < -Math.PI * 0.5)
      throw new IllegalArgumentException("Bottom latitude out of range");
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

    this.bottomLat = bottomLat;
    this.leftLon = leftLon;
    this.rightLon = rightLon;

    final double sinBottomLat = Math.sin(bottomLat);
    final double cosBottomLat = Math.cos(bottomLat);
    final double sinLeftLon = Math.sin(leftLon);
    final double cosLeftLon = Math.cos(leftLon);
    final double sinRightLon = Math.sin(rightLon);
    final double cosRightLon = Math.cos(rightLon);

    // Now build the points
    this.LRHC = new GeoPoint(planetModel, sinBottomLat, sinRightLon, cosBottomLat, cosRightLon, bottomLat, rightLon);
    this.LLHC = new GeoPoint(planetModel, sinBottomLat, sinLeftLon, cosBottomLat, cosLeftLon, bottomLat, leftLon);

    final double middleLat = (Math.PI * 0.5 + bottomLat) * 0.5;
    final double sinMiddleLat = Math.sin(middleLat);
    this.cosMiddleLat = Math.cos(middleLat);
    // Normalize
    while (leftLon > rightLon) {
      rightLon += Math.PI * 2.0;
    }
    final double middleLon = (leftLon + rightLon) * 0.5;
    final double sinMiddleLon = Math.sin(middleLon);
    final double cosMiddleLon = Math.cos(middleLon);

    this.centerPoint = new GeoPoint(planetModel, sinMiddleLat, sinMiddleLon, cosMiddleLat, cosMiddleLon);

    this.bottomPlane = new SidedPlane(centerPoint, planetModel, sinBottomLat);
    this.leftPlane = new SidedPlane(centerPoint, cosLeftLon, sinLeftLon);
    this.rightPlane = new SidedPlane(centerPoint, cosRightLon, sinRightLon);

    this.bottomPlanePoints = new GeoPoint[]{LLHC, LRHC};
    this.leftPlanePoints = new GeoPoint[]{planetModel.NORTH_POLE, LLHC};
    this.rightPlanePoints = new GeoPoint[]{planetModel.NORTH_POLE, LRHC};

    this.edgePoints = new GeoPoint[]{planetModel.NORTH_POLE};
  }

  /**
   * Constructor for deserialization.
   * @param planetModel is the planet model.
   * @param inputStream is the input stream.
   */
  public GeoNorthRectangle(final PlanetModel planetModel, final InputStream inputStream) throws IOException {
    this(planetModel, SerializableObject.readDouble(inputStream), SerializableObject.readDouble(inputStream), SerializableObject.readDouble(inputStream));
  }

  @Override
  public void write(final OutputStream outputStream) throws IOException {
    SerializableObject.writeDouble(outputStream, bottomLat);
    SerializableObject.writeDouble(outputStream, leftLon);
    SerializableObject.writeDouble(outputStream, rightLon);
  }

  @Override
  public GeoBBox expand(final double angle) {
    final double newTopLat = Math.PI * 0.5;
    final double newBottomLat = bottomLat - angle;
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
    return
        bottomPlane.isWithin(x, y, z) &&
            leftPlane.isWithin(x, y, z) &&
            rightPlane.isWithin(x, y, z);
  }

  @Override
  public double getRadius() {
    // Here we compute the distance from the middle point to one of the corners.  However, we need to be careful
    // to use the longest of three distances: the distance to a corner on the top; the distnace to a corner on the bottom, and
    // the distance to the right or left edge from the center.
    final double centerAngle = (rightLon - (rightLon + leftLon) * 0.5) * cosMiddleLat;
    final double bottomAngle = centerPoint.arcDistance(LLHC);
    return Math.max(centerAngle, bottomAngle);
  }

  @Override
  public GeoPoint[] getEdgePoints() {
    return edgePoints;
  }

  /**
   * Returns the center of a circle into which the area will be inscribed.
   *
   * @return the center.
   */
  @Override
  public GeoPoint getCenter() {
    return centerPoint;
  }

  @Override
  public boolean intersects(final Plane p, final GeoPoint[] notablePoints, final Membership... bounds) {
    return
        p.intersects(planetModel, bottomPlane, notablePoints, bottomPlanePoints, bounds, leftPlane, rightPlane) ||
            p.intersects(planetModel, leftPlane, notablePoints, leftPlanePoints, bounds, rightPlane, bottomPlane) ||
            p.intersects(planetModel, rightPlane, notablePoints, rightPlanePoints, bounds, leftPlane, bottomPlane);
  }

  @Override
  public boolean intersects(final GeoShape geoShape) {
    return
        geoShape.intersects(bottomPlane, bottomPlanePoints, leftPlane, rightPlane) ||
            geoShape.intersects(leftPlane, leftPlanePoints, rightPlane, bottomPlane) ||
            geoShape.intersects(rightPlane, rightPlanePoints, leftPlane, bottomPlane);
  }

  @Override
  public void getBounds(Bounds bounds) {
    super.getBounds(bounds);
    bounds
      .addHorizontalPlane(planetModel, bottomLat, bottomPlane, leftPlane, rightPlane)
      .addVerticalPlane(planetModel, leftLon, leftPlane, bottomPlane, rightPlane)
      .addVerticalPlane(planetModel, rightLon, rightPlane, bottomPlane, leftPlane)
      .addIntersection(planetModel, rightPlane, leftPlane, bottomPlane)
      .addPoint(LLHC).addPoint(LRHC).addPoint(planetModel.NORTH_POLE);
  }

  @Override
  protected double outsideDistance(final DistanceStyle distanceStyle, final double x, final double y, final double z) {
    final double bottomDistance = distanceStyle.computeDistance(planetModel, bottomPlane, x,y,z, leftPlane, rightPlane);
    final double leftDistance = distanceStyle.computeDistance(planetModel, leftPlane, x,y,z, rightPlane, bottomPlane);
    final double rightDistance = distanceStyle.computeDistance(planetModel, rightPlane, x,y,z, leftPlane, bottomPlane);
    
    final double LRHCDistance = distanceStyle.computeDistance(LRHC, x,y,z);
    final double LLHCDistance = distanceStyle.computeDistance(LLHC, x,y,z);
    
    return
      Math.min(
        bottomDistance,
        Math.min(
          Math.min(leftDistance, rightDistance),
          Math.min(LRHCDistance, LLHCDistance)));
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof GeoNorthRectangle))
      return false;
    GeoNorthRectangle other = (GeoNorthRectangle) o;
    return super.equals(other) && other.LLHC.equals(LLHC) && other.LRHC.equals(LRHC);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + LLHC.hashCode();
    result = 31 * result + LRHC.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "GeoNorthRectangle: {planetmodel="+planetModel+", bottomlat=" + bottomLat + "(" + bottomLat * 180.0 / Math.PI + "), leftlon=" + leftLon + "(" + leftLon * 180.0 / Math.PI + "), rightlon=" + rightLon + "(" + rightLon * 180.0 / Math.PI + ")}";
  }
}
  

