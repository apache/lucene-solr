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
 * Bounding box limited on three sides (top lat, left lon, right lon).  The
 * other corner is the south pole.
 * The left-right maximum extent for this shape is PI; for anything larger, use
 * {@link GeoWideSouthRectangle}.
 *
 * @lucene.internal
 */
public class GeoSouthRectangle extends GeoBaseBBox {
  /** The top latitude of the rect */
  protected final double topLat;
  /** The left longitude of the rect */
  protected final double leftLon;
  /** The right longitude of the rect */
  protected final double rightLon;
  /** The cosine of a middle latitude */
  protected final double cosMiddleLat;
  /** The upper left hand corner of the rectangle */
  protected final GeoPoint ULHC;
  /** The upper right hand corner of the rectangle */
  protected final GeoPoint URHC;

  /** The top plane */
  protected final SidedPlane topPlane;
  /** The left plane */
  protected final SidedPlane leftPlane;
  /** The right plane */
  protected final SidedPlane rightPlane;

  /** Notable points for the top plane */
  protected final GeoPoint[] topPlanePoints;
  /** Notable points for the left plane */
  protected final GeoPoint[] leftPlanePoints;
  /** Notable points for the right plane */
  protected final GeoPoint[] rightPlanePoints;

  /** The center point */
  protected final GeoPoint centerPoint;

  /** A point on the edge */
  protected final GeoPoint[] edgePoints;

  /**
   * Accepts only values in the following ranges: lat: {@code -PI/2 -> PI/2}, lon: {@code -PI -> PI}
   *@param planetModel is the planet model.
   *@param topLat is the top latitude.
   *@param leftLon is the left longitude.
   *@param rightLon is the right longitude.
   */
  public GeoSouthRectangle(final PlanetModel planetModel, final double topLat, final double leftLon, double rightLon) {
    super(planetModel);
    // Argument checking
    if (topLat > Math.PI * 0.5 || topLat < -Math.PI * 0.5)
      throw new IllegalArgumentException("Top latitude out of range");
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
    this.leftLon = leftLon;
    this.rightLon = rightLon;

    final double sinTopLat = Math.sin(topLat);
    final double cosTopLat = Math.cos(topLat);
    final double sinLeftLon = Math.sin(leftLon);
    final double cosLeftLon = Math.cos(leftLon);
    final double sinRightLon = Math.sin(rightLon);
    final double cosRightLon = Math.cos(rightLon);

    // Now build the four points
    this.ULHC = new GeoPoint(planetModel, sinTopLat, sinLeftLon, cosTopLat, cosLeftLon, topLat, leftLon);
    this.URHC = new GeoPoint(planetModel, sinTopLat, sinRightLon, cosTopLat, cosRightLon, topLat, rightLon);

    final double middleLat = (topLat - Math.PI * 0.5) * 0.5;
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
    this.leftPlane = new SidedPlane(centerPoint, cosLeftLon, sinLeftLon);
    this.rightPlane = new SidedPlane(centerPoint, cosRightLon, sinRightLon);

    this.topPlanePoints = new GeoPoint[]{ULHC, URHC};
    this.leftPlanePoints = new GeoPoint[]{ULHC, planetModel.SOUTH_POLE};
    this.rightPlanePoints = new GeoPoint[]{URHC, planetModel.SOUTH_POLE};
    
    this.edgePoints = new GeoPoint[]{planetModel.SOUTH_POLE};

  }

  @Override
  public GeoBBox expand(final double angle) {
    final double newTopLat = topLat + angle;
    final double newBottomLat = -Math.PI * 0.5;
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
    return Math.max(centerAngle, topAngle);
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
    return p.intersects(planetModel, topPlane, notablePoints, topPlanePoints, bounds, leftPlane, rightPlane) ||
        p.intersects(planetModel, leftPlane, notablePoints, leftPlanePoints, bounds, rightPlane, topPlane) ||
        p.intersects(planetModel, rightPlane, notablePoints, rightPlanePoints, bounds, leftPlane, topPlane);
  }

  @Override
  public void getBounds(Bounds bounds) {
    super.getBounds(bounds);
    bounds
      .addHorizontalPlane(planetModel, topLat, topPlane, leftPlane, rightPlane)
      .addVerticalPlane(planetModel, leftLon, leftPlane, topPlane, rightPlane)
      .addVerticalPlane(planetModel, rightLon, rightPlane, topPlane, leftPlane)
      .addPoint(URHC).addPoint(ULHC).addPoint(planetModel.SOUTH_POLE);
  }

  @Override
  public int getRelationship(final GeoShape path) {
    //System.err.println(this+" getrelationship with "+path);
    final int insideRectangle = isShapeInsideBBox(path);
    if (insideRectangle == SOME_INSIDE) {
      //System.err.println(" some inside");
      return OVERLAPS;
    }

    final boolean insideShape = path.isWithin(planetModel.SOUTH_POLE);

    if (insideRectangle == ALL_INSIDE && insideShape) {
      //System.err.println(" inside of each other");
      return OVERLAPS;
    }

    if (path.intersects(topPlane, topPlanePoints, leftPlane, rightPlane) ||
        path.intersects(leftPlane, leftPlanePoints, topPlane, rightPlane) ||
        path.intersects(rightPlane, rightPlanePoints, leftPlane, topPlane)) {
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
    final double topDistance = distanceStyle.computeDistance(planetModel, topPlane, x,y,z, leftPlane, rightPlane);
    final double leftDistance = distanceStyle.computeDistance(planetModel, leftPlane, x,y,z, rightPlane, topPlane);
    final double rightDistance = distanceStyle.computeDistance(planetModel, rightPlane, x,y,z, leftPlane, topPlane);
    
    final double ULHCDistance = distanceStyle.computeDistance(ULHC, x,y,z);
    final double URHCDistance = distanceStyle.computeDistance(URHC, x,y,z);
    
    return Math.min(
      Math.min(
        topDistance,
        Math.min(leftDistance, rightDistance)),
      Math.min(ULHCDistance, URHCDistance));
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof GeoSouthRectangle))
      return false;
    GeoSouthRectangle other = (GeoSouthRectangle) o;
    return super.equals(other) && other.ULHC.equals(ULHC) && other.URHC.equals(URHC);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + ULHC.hashCode();
    result = 31 * result + URHC.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "GeoSouthRectangle: {planetmodel="+planetModel+", toplat=" + topLat + "(" + topLat * 180.0 / Math.PI + "), leftlon=" + leftLon + "(" + leftLon * 180.0 / Math.PI + "), rightlon=" + rightLon + "(" + rightLon * 180.0 / Math.PI + ")}";
  }
}
  

