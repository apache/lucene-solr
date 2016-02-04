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
 * Bounding box wider than PI but limited on three sides (top lat,
 * left lon, right lon).
 *
 * @lucene.internal
 */
public class GeoWideSouthRectangle extends GeoBaseBBox {
  /** Top latitude of rect */
  protected final double topLat;
  /** Left longitude of rect */
  protected final double leftLon;
  /** Right longitude of rect */
  protected final double rightLon;

  /** Cosine of middle latitude */
  protected final double cosMiddleLat;

  /** Upper left hand corner */
  protected final GeoPoint ULHC;
  /** Upper right hand corner */
  protected final GeoPoint URHC;

  /** The top plane */
  protected final SidedPlane topPlane;
  /** The left plane */
  protected final SidedPlane leftPlane;
  /** The right plane */
  protected final SidedPlane rightPlane;

  /** Notable points for top plane */
  protected final GeoPoint[] topPlanePoints;
  /** Notable points for left plane */
  protected final GeoPoint[] leftPlanePoints;
  /** Notable points for right plane */
  protected final GeoPoint[] rightPlanePoints;

  /** Center point */
  protected final GeoPoint centerPoint;

  /** Left/right bounds */
  protected final EitherBound eitherBound;

  /** A point on the edge */
  protected final GeoPoint[] edgePoints;

  /**
   * Accepts only values in the following ranges: lat: {@code -PI/2 -> PI/2}, lon: {@code -PI -> PI}.
   * Horizontal angle must be greater than or equal to PI.
   */
  public GeoWideSouthRectangle(final PlanetModel planetModel, final double topLat, final double leftLon, double rightLon) {
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
    if (extent < Math.PI)
      throw new IllegalArgumentException("Width of rectangle too small");

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

    this.eitherBound = new EitherBound();
    
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
    return Math.max(centerAngle, topAngle);
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
    // Right and left bounds are essentially independent hemispheres; crossing into the wrong part of one
    // requires crossing into the right part of the other.  So intersection can ignore the left/right bounds.
    return p.intersects(planetModel, topPlane, notablePoints, topPlanePoints, bounds, eitherBound) ||
        p.intersects(planetModel, leftPlane, notablePoints, leftPlanePoints, bounds, topPlane) ||
        p.intersects(planetModel, rightPlane, notablePoints, rightPlanePoints, bounds, topPlane);
  }

  @Override
  public void getBounds(Bounds bounds) {
    super.getBounds(bounds);
    bounds.isWide()
      .addHorizontalPlane(planetModel, topLat, topPlane, eitherBound)
      .addVerticalPlane(planetModel, rightLon, rightPlane, topPlane)
      .addVerticalPlane(planetModel, leftLon, leftPlane, topPlane)
      .addPoint(ULHC).addPoint(URHC).addPoint(planetModel.SOUTH_POLE);
  }

  @Override
  public int getRelationship(final GeoShape path) {
    //System.err.println(this+" comparing to "+path);
    final int insideRectangle = isShapeInsideBBox(path);
    if (insideRectangle == SOME_INSIDE) {
      //System.err.println(" some inside");
      return OVERLAPS;
    }

    final boolean insideShape = path.isWithin(planetModel.SOUTH_POLE);

    if (insideRectangle == ALL_INSIDE && insideShape) {
      //System.err.println(" both inside each other");
      return OVERLAPS;
    }

    if (path.intersects(topPlane, topPlanePoints, eitherBound) ||
        path.intersects(leftPlane, leftPlanePoints, topPlane) ||
        path.intersects(rightPlane, rightPlanePoints, topPlane)) {
      //System.err.println(" edges intersect");
      return OVERLAPS;
    }

    if (insideRectangle == ALL_INSIDE) {
      //System.err.println(" shape inside rectangle");
      return WITHIN;
    }

    if (insideShape) {
      //System.err.println(" rectangle inside shape");
      return CONTAINS;
    }

    //System.err.println(" disjoint");
    return DISJOINT;
  }

  @Override
  protected double outsideDistance(final DistanceStyle distanceStyle, final double x, final double y, final double z) {
    final double topDistance = distanceStyle.computeDistance(planetModel, topPlane, x,y,z, eitherBound);
    // Because the rectangle exceeds 180 degrees, it is safe to compute the horizontally 
    // unbounded distance to both the left and the right and only take the minimum of the two.
    final double leftDistance = distanceStyle.computeDistance(planetModel, leftPlane, x,y,z, topPlane);
    final double rightDistance = distanceStyle.computeDistance(planetModel, rightPlane, x,y,z, topPlane);
    
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
    if (!(o instanceof GeoWideSouthRectangle))
      return false;
    GeoWideSouthRectangle other = (GeoWideSouthRectangle) o;
    return super.equals(o) && other.ULHC.equals(ULHC) && other.URHC.equals(URHC);
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
    return "GeoWideSouthRectangle: {planetmodel="+planetModel+", toplat=" + topLat + "(" + topLat * 180.0 / Math.PI + "), leftlon=" + leftLon + "(" + leftLon * 180.0 / Math.PI + "), rightlon=" + rightLon + "(" + rightLon * 180.0 / Math.PI + ")}";
  }

  /** Membership implementation representing width more than 180.
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
  

