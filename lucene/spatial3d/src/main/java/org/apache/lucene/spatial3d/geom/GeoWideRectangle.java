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
 * Bounding box wider than PI but limited on four sides (top lat,
 * bottom lat, left lon, right lon).
 *
 * @lucene.internal
 */
class GeoWideRectangle extends GeoBaseBBox {
  /** The top latitude */
  protected final double topLat;
  /** The bottom latitude */
  protected final double bottomLat;
  /** The left longitude */
  protected final double leftLon;
  /** The right longitude */
  protected final double rightLon;

  /** Cosine of the middle latitude */
  protected final double cosMiddleLat;

  /** Upper left hand corner point */
  protected final GeoPoint ULHC;
  /** Lower right hand corner point */
  protected final GeoPoint URHC;
  /** Lower right hand corner point */
  protected final GeoPoint LRHC;
  /** Lower left hand corner point */
  protected final GeoPoint LLHC;

  /** Top plane */
  protected final SidedPlane topPlane;
  /** Bottom plane */
  protected final SidedPlane bottomPlane;
  /** Left plane */
  protected final SidedPlane leftPlane;
  /** Right plane */
  protected final SidedPlane rightPlane;

  /** Top plane's notable points */
  protected final GeoPoint[] topPlanePoints;
  /** Bottom plane's notable points */
  protected final GeoPoint[] bottomPlanePoints;
  /** Left plane's notable points */
  protected final GeoPoint[] leftPlanePoints;
  /** Right plane's notable points */
  protected final GeoPoint[] rightPlanePoints;

  /** Center point */
  protected final GeoPoint centerPoint;

  /** Combined left/right bounds */
  protected final EitherBound eitherBound;

  /** A point on the edge */
  protected final GeoPoint[] edgePoints;

  /**
   * Accepts only values in the following ranges: lat: {@code -PI/2 -> PI/2}, lon: {@code -PI -> PI}.
   * Horizontal angle must be greater than or equal to PI.
   * @param planetModel is the planet model.
   * @param topLat is the top latitude.
   * @param bottomLat is the bottom latitude.
   * @param leftLon is the left longitude.
   * @param rightLon is the right longitude.
   */
  public GeoWideRectangle(final PlanetModel planetModel, final double topLat, final double bottomLat, final double leftLon, double rightLon) {
    super(planetModel);
    // Argument checking
    if (topLat > Math.PI * 0.5 || topLat < -Math.PI * 0.5)
      throw new IllegalArgumentException("Top latitude out of range");
    if (bottomLat > Math.PI * 0.5 || bottomLat < -Math.PI * 0.5)
      throw new IllegalArgumentException("Bottom latitude out of range");
    if (topLat < bottomLat)
      throw new IllegalArgumentException("Top latitude less than bottom latitude");
    if (leftLon < -Math.PI || leftLon > Math.PI)
      throw new IllegalArgumentException("Left longitude out of range");
    if (rightLon < -Math.PI || rightLon > Math.PI)
      throw new IllegalArgumentException("Right longitude out of range");
    double extent = rightLon - leftLon;
    if (extent < 0.0) {
      extent += 2.0 * Math.PI;
    }
    if (extent < Math.PI)
      throw new IllegalArgumentException("Width of rectangle too small");

    this.topLat = topLat;
    this.bottomLat = bottomLat;
    this.leftLon = leftLon;
    this.rightLon = rightLon;

    final double sinTopLat = Math.sin(topLat);
    final double cosTopLat = Math.cos(topLat);
    final double sinBottomLat = Math.sin(bottomLat);
    final double cosBottomLat = Math.cos(bottomLat);
    final double sinLeftLon = Math.sin(leftLon);
    final double cosLeftLon = Math.cos(leftLon);
    final double sinRightLon = Math.sin(rightLon);
    final double cosRightLon = Math.cos(rightLon);

    // Now build the four points
    this.ULHC = new GeoPoint(planetModel, sinTopLat, sinLeftLon, cosTopLat, cosLeftLon, topLat, leftLon);
    this.URHC = new GeoPoint(planetModel, sinTopLat, sinRightLon, cosTopLat, cosRightLon, topLat, rightLon);
    this.LRHC = new GeoPoint(planetModel, sinBottomLat, sinRightLon, cosBottomLat, cosRightLon, bottomLat, rightLon);
    this.LLHC = new GeoPoint(planetModel, sinBottomLat, sinLeftLon, cosBottomLat, cosLeftLon, bottomLat, leftLon);

    final double middleLat = (topLat + bottomLat) * 0.5;
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

    this.topPlane = new SidedPlane(centerPoint, planetModel, sinTopLat);
    this.bottomPlane = new SidedPlane(centerPoint, planetModel, sinBottomLat);
    this.leftPlane = new SidedPlane(centerPoint, cosLeftLon, sinLeftLon);
    this.rightPlane = new SidedPlane(centerPoint, cosRightLon, sinRightLon);

    this.topPlanePoints = new GeoPoint[]{ULHC, URHC};
    this.bottomPlanePoints = new GeoPoint[]{LLHC, LRHC};
    this.leftPlanePoints = new GeoPoint[]{ULHC, LLHC};
    this.rightPlanePoints = new GeoPoint[]{URHC, LRHC};

    this.eitherBound = new EitherBound();

    this.edgePoints = new GeoPoint[]{ULHC};
  }

  /**
   * Constructor for deserialization.
   * @param planetModel is the planet model.
   * @param inputStream is the input stream.
   */
  public GeoWideRectangle(final PlanetModel planetModel, final InputStream inputStream) throws IOException {
    this(planetModel, SerializableObject.readDouble(inputStream), SerializableObject.readDouble(inputStream), SerializableObject.readDouble(inputStream), SerializableObject.readDouble(inputStream));
  }

  @Override
  public void write(final OutputStream outputStream) throws IOException {
    SerializableObject.writeDouble(outputStream, topLat);
    SerializableObject.writeDouble(outputStream, bottomLat);
    SerializableObject.writeDouble(outputStream, leftLon);
    SerializableObject.writeDouble(outputStream, rightLon);
  }

  @Override
  public GeoBBox expand(final double angle) {
    final double newTopLat = topLat + angle;
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
    return topPlane.isWithin(x, y, z) &&
        bottomPlane.isWithin(x, y, z) &&
        (leftPlane.isWithin(x, y, z) ||
            rightPlane.isWithin(x, y, z));
  }

  @Override
  public double getRadius() {
    // Here we compute the distance from the middle point to one of the corners.  However, we need to be careful
    // to use the longest of three distances: the distance to a corner on the top; the distnace to a corner on the bottom, and
    // the distance to the right or left edge from the center.
    final double centerAngle = (rightLon - (rightLon + leftLon) * 0.5) * cosMiddleLat;
    final double topAngle = centerPoint.arcDistance(URHC);
    final double bottomAngle = centerPoint.arcDistance(LLHC);
    return Math.max(centerAngle, Math.max(topAngle, bottomAngle));
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
    // Right and left bounds are essentially independent hemispheres; crossing into the wrong part of one
    // requires crossing into the right part of the other.  So intersection can ignore the left/right bounds.
    return p.intersects(planetModel, topPlane, notablePoints, topPlanePoints, bounds, bottomPlane, eitherBound) ||
        p.intersects(planetModel, bottomPlane, notablePoints, bottomPlanePoints, bounds, topPlane, eitherBound) ||
        p.intersects(planetModel, leftPlane, notablePoints, leftPlanePoints, bounds, topPlane, bottomPlane) ||
        p.intersects(planetModel, rightPlane, notablePoints, rightPlanePoints, bounds, topPlane, bottomPlane);
  }

  @Override
  public boolean intersects(final GeoShape geoShape) {
    return geoShape.intersects(topPlane, topPlanePoints, bottomPlane, eitherBound) ||
        geoShape.intersects(bottomPlane, bottomPlanePoints, topPlane, eitherBound) ||
        geoShape.intersects(leftPlane, leftPlanePoints, topPlane, bottomPlane) ||
        geoShape.intersects(rightPlane, rightPlanePoints, topPlane, bottomPlane);
  }

  @Override
  public void getBounds(Bounds bounds) {
    super.getBounds(bounds);
    bounds.isWide()
      .addHorizontalPlane(planetModel, topLat, topPlane, bottomPlane, eitherBound)
      .addVerticalPlane(planetModel, rightLon, rightPlane, topPlane, bottomPlane)
      .addHorizontalPlane(planetModel, bottomLat, bottomPlane, topPlane, eitherBound)
      .addVerticalPlane(planetModel, leftLon, leftPlane, topPlane, bottomPlane)
      .addIntersection(planetModel, leftPlane, rightPlane, topPlane, bottomPlane)
      .addPoint(ULHC).addPoint(URHC).addPoint(LRHC).addPoint(LLHC);
  }

  @Override
  protected double outsideDistance(final DistanceStyle distanceStyle, final double x, final double y, final double z) {
    final double topDistance = distanceStyle.computeDistance(planetModel, topPlane, x,y,z, bottomPlane, eitherBound);
    final double bottomDistance = distanceStyle.computeDistance(planetModel, bottomPlane, x,y,z, topPlane, eitherBound);
    // Because the rectangle exceeds 180 degrees, it is safe to compute the horizontally 
    // unbounded distance to both the left and the right and only take the minimum of the two.
    final double leftDistance = distanceStyle.computeDistance(planetModel, leftPlane, x,y,z, topPlane, bottomPlane);
    final double rightDistance = distanceStyle.computeDistance(planetModel, rightPlane, x,y,z, topPlane, bottomPlane);
    
    final double ULHCDistance = distanceStyle.computeDistance(ULHC, x,y,z);
    final double URHCDistance = distanceStyle.computeDistance(URHC, x,y,z);
    final double LRHCDistance = distanceStyle.computeDistance(LRHC, x,y,z);
    final double LLHCDistance = distanceStyle.computeDistance(LLHC, x,y,z);
    
    return Math.min(
      Math.min(
        Math.min(topDistance, bottomDistance),
        Math.min(leftDistance, rightDistance)),
      Math.min(
        Math.min(ULHCDistance, URHCDistance),
        Math.min(LRHCDistance, LLHCDistance)));
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof GeoWideRectangle))
      return false;
    GeoWideRectangle other = (GeoWideRectangle) o;
    return super.equals(other) && other.ULHC.equals(ULHC) && other.LRHC.equals(LRHC);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + ULHC.hashCode();
    result = 31 * result + LRHC.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "GeoWideRectangle: {planetmodel=" + planetModel + ", toplat=" + topLat + "(" + topLat * 180.0 / Math.PI + "), bottomlat=" + bottomLat + "(" + bottomLat * 180.0 / Math.PI + "), leftlon=" + leftLon + "(" + leftLon * 180.0 / Math.PI + "), rightlon=" + rightLon + "(" + rightLon * 180.0 / Math.PI + ")}";
  }

  /** A membership implementation representing a wide (more than 180) left/right bound.
   */
  protected class EitherBound implements Membership {
    /** Constructor.
      */
    public EitherBound() {
    }

    @Override
    public boolean isWithin(final Vector v) {
      return leftPlane.isWithin(v) || rightPlane.isWithin(v);
    }

    @Override
    public boolean isWithin(final double x, final double y, final double z) {
      return leftPlane.isWithin(x, y, z) || rightPlane.isWithin(x, y, z);
    }
  }
}
  
