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
 * Bounding box limited on four sides (top lat, bottom lat, left lon, right lon).
 * The left-right maximum extent for this shape is PI; for anything larger, use
 * GeoWideRectangle.
 *
 * @lucene.internal
 */
public class GeoRectangle extends GeoBaseBBox {
  /** The top latitude of the rect */
  protected final double topLat;
  /** The bottom latitude of the rect */
  protected final double bottomLat;
  /** The left longitude of the rect */
  protected final double leftLon;
  /** The right longitude of the rect */
  protected final double rightLon;
  /** The cosine of a middle latitude */
  protected final double cosMiddleLat;

  /** The upper left hand corner point */
  protected final GeoPoint ULHC;
  /** The upper right hand corner point */
  protected final GeoPoint URHC;
  /** The lower right hand corner point */
  protected final GeoPoint LRHC;
  /** The lower left hand corner point */
  protected final GeoPoint LLHC;

  /** The top plane */
  protected final SidedPlane topPlane;
  /** The bottom plane */
  protected final SidedPlane bottomPlane;
  /** The left plane */
  protected final SidedPlane leftPlane;
  /** The right plane */
  protected final SidedPlane rightPlane;

  /** Notable points for the top plane */
  protected final GeoPoint[] topPlanePoints;
  /** Notable points for the bottom plane */
  protected final GeoPoint[] bottomPlanePoints;
  /** Notable points for the left plane */
  protected final GeoPoint[] leftPlanePoints;
  /** Notable points for the right plane */
  protected final GeoPoint[] rightPlanePoints;

  /** Center point */
  protected final GeoPoint centerPoint;

  /** Edge point for this rectangle */
  protected final GeoPoint[] edgePoints;

  /**
   * Accepts only values in the following ranges: lat: {@code -PI/2 -> PI/2}, lon: {@code -PI -> PI}
   *@param planetModel is the planet model.
   *@param topLat is the top latitude.
   *@param bottomLat is the bottom latitude.
   *@param leftLon is the left longitude.
   *@param rightLon is the right longitude.
   */
  public GeoRectangle(final PlanetModel planetModel, final double topLat, final double bottomLat, final double leftLon, double rightLon) {
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
    if (extent > Math.PI)
      throw new IllegalArgumentException("Width of rectangle too great");

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

    this.edgePoints = new GeoPoint[]{ULHC};
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
        leftPlane.isWithin(x, y, z) &&
        rightPlane.isWithin(x, y, z);
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

  @Override
  public GeoPoint getCenter() {
    return centerPoint;
  }

  @Override
  public boolean intersects(final Plane p, final GeoPoint[] notablePoints, final Membership... bounds) {
    return p.intersects(planetModel, topPlane, notablePoints, topPlanePoints, bounds, bottomPlane, leftPlane, rightPlane) ||
        p.intersects(planetModel, bottomPlane, notablePoints, bottomPlanePoints, bounds, topPlane, leftPlane, rightPlane) ||
        p.intersects(planetModel, leftPlane, notablePoints, leftPlanePoints, bounds, rightPlane, topPlane, bottomPlane) ||
        p.intersects(planetModel, rightPlane, notablePoints, rightPlanePoints, bounds, leftPlane, topPlane, bottomPlane);
  }

  @Override
  public void getBounds(Bounds bounds) {
    super.getBounds(bounds);
    bounds.addHorizontalPlane(planetModel, topLat, topPlane, bottomPlane, leftPlane, rightPlane)
      .addVerticalPlane(planetModel, rightLon, rightPlane, topPlane, bottomPlane, leftPlane)
      .addHorizontalPlane(planetModel, bottomLat, bottomPlane, topPlane, leftPlane, rightPlane)
      .addVerticalPlane(planetModel, leftLon, leftPlane, topPlane, bottomPlane, rightPlane)
      .addPoint(ULHC).addPoint(URHC).addPoint(LLHC).addPoint(LRHC);
  }

  @Override
  public int getRelationship(final GeoShape path) {
    //System.err.println(this+" getrelationship with "+path);
    final int insideRectangle = isShapeInsideBBox(path);
    if (insideRectangle == SOME_INSIDE) {
      //System.err.println(" some inside");
      return OVERLAPS;
    }

    final boolean insideShape = path.isWithin(ULHC);

    if (insideRectangle == ALL_INSIDE && insideShape) {
      //System.err.println(" inside of each other");
      return OVERLAPS;
    }

    if (path.intersects(topPlane, topPlanePoints, bottomPlane, leftPlane, rightPlane) ||
        path.intersects(bottomPlane, bottomPlanePoints, topPlane, leftPlane, rightPlane) ||
        path.intersects(leftPlane, leftPlanePoints, topPlane, bottomPlane, rightPlane) ||
        path.intersects(rightPlane, rightPlanePoints, leftPlane, topPlane, bottomPlane)) {
      //System.err.println(" edges intersect");
      return OVERLAPS;
    }

    if (insideRectangle == ALL_INSIDE) {
      //System.err.println(" shape inside rectangle");
      return WITHIN;
    }

    if (insideShape) {
      //System.err.println(" shape contains rectangle");
      return CONTAINS;
    }
    //System.err.println(" disjoint");
    return DISJOINT;
  }

  @Override
  protected double outsideDistance(final DistanceStyle distanceStyle, final double x, final double y, final double z) {
    final double topDistance = distanceStyle.computeDistance(planetModel, topPlane, x,y,z, bottomPlane, leftPlane, rightPlane);
    final double bottomDistance = distanceStyle.computeDistance(planetModel, bottomPlane, x,y,z, topPlane, leftPlane, rightPlane);
    final double leftDistance = distanceStyle.computeDistance(planetModel, leftPlane, x,y,z, rightPlane, topPlane, bottomPlane);
    final double rightDistance = distanceStyle.computeDistance(planetModel, rightPlane, x,y,z, leftPlane, topPlane, bottomPlane);
    
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
    if (!(o instanceof GeoRectangle))
      return false;
    GeoRectangle other = (GeoRectangle) o;
    return super.equals(other) && other.ULHC.equals(ULHC) && other.LRHC.equals(LRHC);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result  + ULHC.hashCode();
    result = 31 * result  + LRHC.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "GeoRectangle: {planetmodel="+planetModel+", toplat=" + topLat + "(" + topLat * 180.0 / Math.PI + "), bottomlat=" + bottomLat + "(" + bottomLat * 180.0 / Math.PI + "), leftlon=" + leftLon + "(" + leftLon * 180.0 / Math.PI + "), rightlon=" + rightLon + "(" + rightLon * 180.0 / Math.PI + ")}";
  }
}
  
