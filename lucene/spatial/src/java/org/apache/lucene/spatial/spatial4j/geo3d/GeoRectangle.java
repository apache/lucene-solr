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
 * Bounding box limited on four sides (top lat, bottom lat, left lon, right lon).
 * The left-right maximum extent for this shape is PI; for anything larger, use
 * GeoWideRectangle.
 *
 * @lucene.internal
 */
public class GeoRectangle extends GeoBBoxBase {
  public final double topLat;
  public final double bottomLat;
  public final double leftLon;
  public final double rightLon;

  public final double cosMiddleLat;

  public final GeoPoint ULHC;
  public final GeoPoint URHC;
  public final GeoPoint LRHC;
  public final GeoPoint LLHC;

  public final SidedPlane topPlane;
  public final SidedPlane bottomPlane;
  public final SidedPlane leftPlane;
  public final SidedPlane rightPlane;

  public final GeoPoint[] topPlanePoints;
  public final GeoPoint[] bottomPlanePoints;
  public final GeoPoint[] leftPlanePoints;
  public final GeoPoint[] rightPlanePoints;

  public final GeoPoint centerPoint;

  public final GeoPoint[] edgePoints;

  /**
   * Accepts only values in the following ranges: lat: {@code -PI/2 -> PI/2}, lon: {@code -PI -> PI}
   */
  public GeoRectangle(final double topLat, final double bottomLat, final double leftLon, double rightLon) {
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
    this.ULHC = new GeoPoint(sinTopLat, sinLeftLon, cosTopLat, cosLeftLon);
    this.URHC = new GeoPoint(sinTopLat, sinRightLon, cosTopLat, cosRightLon);
    this.LRHC = new GeoPoint(sinBottomLat, sinRightLon, cosBottomLat, cosRightLon);
    this.LLHC = new GeoPoint(sinBottomLat, sinLeftLon, cosBottomLat, cosLeftLon);

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

    this.centerPoint = new GeoPoint(sinMiddleLat, sinMiddleLon, cosMiddleLat, cosMiddleLon);

    this.topPlane = new SidedPlane(centerPoint, sinTopLat);
    this.bottomPlane = new SidedPlane(centerPoint, sinBottomLat);
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
    return GeoBBoxFactory.makeGeoBBox(newTopLat, newBottomLat, newLeftLon, newRightLon);
  }

  @Override
  public boolean isWithin(final Vector point) {
    return topPlane.isWithin(point) &&
        bottomPlane.isWithin(point) &&
        leftPlane.isWithin(point) &&
        rightPlane.isWithin(point);
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
    return p.intersects(topPlane, notablePoints, topPlanePoints, bounds, bottomPlane, leftPlane, rightPlane) ||
        p.intersects(bottomPlane, notablePoints, bottomPlanePoints, bounds, topPlane, leftPlane, rightPlane) ||
        p.intersects(leftPlane, notablePoints, leftPlanePoints, bounds, rightPlane, topPlane, bottomPlane) ||
        p.intersects(rightPlane, notablePoints, rightPlanePoints, bounds, leftPlane, topPlane, bottomPlane);
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
    bounds.addLatitudeZone(topLat).addLatitudeZone(bottomLat)
        .addLongitudeSlice(leftLon, rightLon);
    return bounds;
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
  public boolean equals(Object o) {
    if (!(o instanceof GeoRectangle))
      return false;
    GeoRectangle other = (GeoRectangle) o;
    return other.ULHC.equals(ULHC) && other.LRHC.equals(LRHC);
  }

  @Override
  public int hashCode() {
    int result = ULHC.hashCode();
    result = 31 * result + LRHC.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "GeoRectangle: {toplat=" + topLat + "(" + topLat * 180.0 / Math.PI + "), bottomlat=" + bottomLat + "(" + bottomLat * 180.0 / Math.PI + "), leftlon=" + leftLon + "(" + leftLon * 180.0 / Math.PI + "), rightlon=" + rightLon + "(" + rightLon * 180.0 / Math.PI + ")}";
  }
}
  
