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
 * Circular area with a center and radius.
 *
 * @lucene.experimental
 */
public class GeoCircle extends GeoBaseExtendedShape implements GeoDistanceShape, GeoSizeable {
  public final GeoPoint center;
  public final double cutoffAngle;
  public final SidedPlane circlePlane;
  public final GeoPoint[] edgePoints;
  public static final GeoPoint[] circlePoints = new GeoPoint[0];

  public GeoCircle(final PlanetModel planetModel, final double lat, final double lon, final double cutoffAngle) {
    super(planetModel);
    if (lat < -Math.PI * 0.5 || lat > Math.PI * 0.5)
      throw new IllegalArgumentException("Latitude out of bounds");
    if (lon < -Math.PI || lon > Math.PI)
      throw new IllegalArgumentException("Longitude out of bounds");
    if (cutoffAngle <= 0.0 || cutoffAngle > Math.PI)
      throw new IllegalArgumentException("Cutoff angle out of bounds");
    this.center = new GeoPoint(planetModel, lat, lon);
    // In an ellipsoidal world, cutoff distances make no sense, unfortunately.  Only membership
    // can be used to make in/out determination.
    this.cutoffAngle = cutoffAngle;
    // Compute two points on the circle, with the right angle from the center.  We'll use these
    // to obtain the perpendicular plane to the circle.
    double upperLat = lat + cutoffAngle;
    double upperLon = lon;
    if (upperLat > Math.PI * 0.5) {
      upperLon += Math.PI;
      if (upperLon > Math.PI)
        upperLon -= 2.0 * Math.PI;
      upperLat = Math.PI - upperLat;
    }
    double lowerLat = lat - cutoffAngle;
    double lowerLon = lon;
    if (lowerLat < -Math.PI * 0.5) {
      lowerLon += Math.PI;
      if (lowerLon > Math.PI)
        lowerLon -= 2.0 * Math.PI;
      lowerLat = -Math.PI - lowerLat;
    }
    final GeoPoint upperPoint = new GeoPoint(planetModel, upperLat, upperLon);
    final GeoPoint lowerPoint = new GeoPoint(planetModel, lowerLat, lowerLon);
    if (Math.abs(cutoffAngle - Math.PI) < Vector.MINIMUM_RESOLUTION) {
      // Circle is the whole world
      this.circlePlane = null;
      this.edgePoints = new GeoPoint[0];
    } else {
      // Construct normal plane
      final Plane normalPlane = new Plane(upperPoint, center);
      // Construct a sided plane that goes through the two points and whose normal is in the normalPlane.
      this.circlePlane = SidedPlane.constructNormalizedPerpendicularSidedPlane(center, normalPlane, upperPoint, lowerPoint);
      if (circlePlane == null)
        throw new RuntimeException("Couldn't construct circle plane.  Cutoff angle = "+cutoffAngle+"; upperPoint = "+upperPoint+"; lowerPoint = "+lowerPoint);
      this.edgePoints = new GeoPoint[]{upperPoint};
    }
  }

  @Override
  public double getRadius() {
    return cutoffAngle;
  }

  /**
   * Returns the center of a circle into which the area will be inscribed.
   *
   * @return the center.
   */
  @Override
  public GeoPoint getCenter() {
    return center;
  }

  /**
   * Compute an estimate of "distance" to the GeoPoint.
   * A return value of Double.MAX_VALUE should be returned for
   * points outside of the shape.
   */
  @Override
  public double computeNormalDistance(final GeoPoint point) {
    if (!isWithin(point))
      return Double.MAX_VALUE;
    return this.center.normalDistance(point);
  }

  /**
   * Compute an estimate of "distance" to the GeoPoint.
   * A return value of Double.MAX_VALUE should be returned for
   * points outside of the shape.
   */
  @Override
  public double computeNormalDistance(final double x, final double y, final double z) {
    if (!isWithin(x,y,z))
      return Double.MAX_VALUE;
    return this.center.normalDistance(x, y, z);
  }

  /**
   * Compute a squared estimate of the "distance" to the
   * GeoPoint.  Double.MAX_VALUE indicates a point outside of the
   * shape.
   */
  @Override
  public double computeSquaredNormalDistance(final GeoPoint point) {
    if (!isWithin(point))
      return Double.MAX_VALUE;
    return this.center.normalDistanceSquared(point);
  }

  /**
   * Compute a squared estimate of the "distance" to the
   * GeoPoint.  Double.MAX_VALUE indicates a point outside of the
   * shape.
   */
  @Override
  public double computeSquaredNormalDistance(final double x, final double y, final double z) {
    if (!isWithin(x,y,z))
      return Double.MAX_VALUE;
    return this.center.normalDistanceSquared(x, y, z);
  }

  /**
   * Compute a linear distance to the vector.
   * return Double.MAX_VALUE for points outside the shape.
   */
  @Override
  public double computeLinearDistance(final GeoPoint point) {
    if (!isWithin(point))
      return Double.MAX_VALUE;
    return this.center.linearDistance(point);
  }

  /**
   * Compute a linear distance to the vector.
   * return Double.MAX_VALUE for points outside the shape.
   */
  @Override
  public double computeLinearDistance(final double x, final double y, final double z) {
    if (!isWithin(x,y,z))
      return Double.MAX_VALUE;
    return this.center.linearDistance(x, y, z);
  }

  /**
   * Compute a squared linear distance to the vector.
   */
  @Override
  public double computeSquaredLinearDistance(final GeoPoint point) {
    if (!isWithin(point))
      return Double.MAX_VALUE;
    return this.center.linearDistanceSquared(point);
  }

  /**
   * Compute a squared linear distance to the vector.
   */
  @Override
  public double computeSquaredLinearDistance(final double x, final double y, final double z) {
    if (!isWithin(x,y,z))
      return Double.MAX_VALUE;
    return this.center.linearDistanceSquared(x, y, z);
  }

  /**
   * Compute a true, accurate, great-circle distance.
   * Double.MAX_VALUE indicates a point is outside of the shape.
   */
  @Override
  public double computeArcDistance(final GeoPoint point) {
    if (!isWithin(point))
      return Double.MAX_VALUE;
    return this.center.arcDistance(point);
  }

  @Override
  public boolean isWithin(final Vector point) {
    if (circlePlane == null) {
      return true;
    }
    // Fastest way of determining membership
    return circlePlane.isWithin(point);
  }

  @Override
  public boolean isWithin(final double x, final double y, final double z) {
    if (circlePlane == null) {
      return true;
    }
    // Fastest way of determining membership
    return circlePlane.isWithin(x, y, z);
  }

  @Override
  public GeoPoint[] getEdgePoints() {
    return edgePoints;
  }

  @Override
  public boolean intersects(final Plane p, final GeoPoint[] notablePoints, final Membership... bounds) {
    if (circlePlane == null) {
      return false;
    }
    return circlePlane.intersects(planetModel, p, notablePoints, circlePoints, bounds);
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
    bounds = super.getBounds(bounds);
    if (circlePlane == null) {
      // Entire world
      bounds.noTopLatitudeBound().noBottomLatitudeBound().noLongitudeBound();
      return bounds;
    }
    bounds.addPoint(center);
    circlePlane.recordBounds(planetModel, bounds);
    return bounds;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof GeoCircle))
      return false;
    GeoCircle other = (GeoCircle) o;
    return super.equals(other) && other.center.equals(center) && other.cutoffAngle == cutoffAngle;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + center.hashCode();
    long temp = Double.doubleToLongBits(cutoffAngle);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    return result;
  }

  @Override
  public String toString() {
    return "GeoCircle: {planetmodel=" + planetModel+", center=" + center + ", radius=" + cutoffAngle + "(" + cutoffAngle * 180.0 / Math.PI + ")}";
  }
}
