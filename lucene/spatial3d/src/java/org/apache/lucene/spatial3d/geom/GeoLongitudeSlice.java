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
 * Bounding box limited on left and right.
 * The left-right maximum extent for this shape is PI; for anything larger, use
 * {@link GeoWideLongitudeSlice}.
 *
 * @lucene.internal
 */
class GeoLongitudeSlice extends GeoBaseBBox {
  /** The left longitude of the slice */
  protected final double leftLon;
  /** The right longitude of the slice */
  protected final double rightLon;
  /** The left plane of the slice */
  protected final SidedPlane leftPlane;
  /** The right plane of the slice */
  protected final SidedPlane rightPlane;
  /** The notable points for the slice (north and south poles) */
  protected final GeoPoint[] planePoints;
  /** The center point of the slice */
  protected final GeoPoint centerPoint;
  /** A point on the edge of the slice */
  protected final GeoPoint[] edgePoints;

  /**
   * Accepts only values in the following ranges: lon: {@code -PI -> PI}
   *@param planetModel is the planet model.
   *@param leftLon is the left longitude of the slice.
   *@param rightLon is the right longitude of the slice.
   */
  public GeoLongitudeSlice(final PlanetModel planetModel, final double leftLon, double rightLon) {
    super(planetModel);
    // Argument checking
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

    this.leftLon = leftLon;
    this.rightLon = rightLon;

    final double sinLeftLon = Math.sin(leftLon);
    final double cosLeftLon = Math.cos(leftLon);
    final double sinRightLon = Math.sin(rightLon);
    final double cosRightLon = Math.cos(rightLon);

    // Normalize
    while (leftLon > rightLon) {
      rightLon += Math.PI * 2.0;
    }
    final double middleLon = (leftLon + rightLon) * 0.5;
    this.centerPoint = new GeoPoint(planetModel, 0.0, middleLon);

    this.leftPlane = new SidedPlane(centerPoint, cosLeftLon, sinLeftLon);
    this.rightPlane = new SidedPlane(centerPoint, cosRightLon, sinRightLon);

    this.planePoints = new GeoPoint[]{planetModel.NORTH_POLE, planetModel.SOUTH_POLE};
    this.edgePoints = new GeoPoint[]{planetModel.NORTH_POLE};
  }

  @Override
  public GeoBBox expand(final double angle) {
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
    return GeoBBoxFactory.makeGeoBBox(planetModel, Math.PI * 0.5, -Math.PI * 0.5, newLeftLon, newRightLon);
  }

  @Override
  public boolean isWithin(final double x, final double y, final double z) {
    return leftPlane.isWithin(x, y, z) &&
        rightPlane.isWithin(x, y, z);
  }

  @Override
  public double getRadius() {
    // Compute the extent and divide by two
    double extent = rightLon - leftLon;
    if (extent < 0.0)
      extent += Math.PI * 2.0;
    return Math.max(Math.PI * 0.5, extent * 0.5);
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
    return p.intersects(planetModel, leftPlane, notablePoints, planePoints, bounds, rightPlane) ||
        p.intersects(planetModel, rightPlane, notablePoints, planePoints, bounds, leftPlane);
  }

  @Override
  public void getBounds(Bounds bounds) {
    super.getBounds(bounds);
    bounds
      .addVerticalPlane(planetModel, leftLon, leftPlane, rightPlane)
      .addVerticalPlane(planetModel, rightLon, rightPlane, leftPlane)
      .addIntersection(planetModel, rightPlane, leftPlane)
      .addPoint(planetModel.NORTH_POLE)
      .addPoint(planetModel.SOUTH_POLE);
  }

  @Override
  public int getRelationship(final GeoShape path) {
    final int insideRectangle = isShapeInsideBBox(path);
    if (insideRectangle == SOME_INSIDE)
      return OVERLAPS;

    final boolean insideShape = path.isWithin(planetModel.NORTH_POLE);

    if (insideRectangle == ALL_INSIDE && insideShape)
      return OVERLAPS;

    if (path.intersects(leftPlane, planePoints, rightPlane) ||
        path.intersects(rightPlane, planePoints, leftPlane)) {
      return OVERLAPS;
    }

    if (insideRectangle == ALL_INSIDE) {
      return WITHIN;
    }

    if (insideShape) {
      return CONTAINS;
    }

    return DISJOINT;
  }

  @Override
  protected double outsideDistance(final DistanceStyle distanceStyle, final double x, final double y, final double z) {
    final double leftDistance = distanceStyle.computeDistance(planetModel, leftPlane, x,y,z, rightPlane);
    final double rightDistance = distanceStyle.computeDistance(planetModel, rightPlane, x,y,z, leftPlane);
    
    final double northDistance = distanceStyle.computeDistance(planetModel.NORTH_POLE, x,y,z);
    final double southDistance = distanceStyle.computeDistance(planetModel.SOUTH_POLE, x,y,z);
    
    return
      Math.min(
        Math.min(northDistance, southDistance),
        Math.min(leftDistance, rightDistance));
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof GeoLongitudeSlice))
      return false;
    GeoLongitudeSlice other = (GeoLongitudeSlice) o;
    return super.equals(other) && other.leftLon == leftLon && other.rightLon == rightLon;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    long temp = Double.doubleToLongBits(leftLon);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    temp = Double.doubleToLongBits(rightLon);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    return result;
  }

  @Override
  public String toString() {
    return "GeoLongitudeSlice: {planetmodel="+planetModel+", leftlon=" + leftLon + "(" + leftLon * 180.0 / Math.PI + "), rightlon=" + rightLon + "(" + rightLon * 180.0 / Math.PI + ")}";
  }
}
  
