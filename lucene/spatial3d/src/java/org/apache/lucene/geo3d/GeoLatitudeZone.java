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
 * This GeoBBox represents an area rectangle limited only in latitude.
 *
 * @lucene.internal
 */
public class GeoLatitudeZone extends GeoBaseBBox {
  /** The top latitude of the zone */
  protected final double topLat;
  /** The bottom latitude of the zone */
  protected final double bottomLat;
  /** Cosine of the top lat */
  protected final double cosTopLat;
  /** Cosine of the bottom lat */
  protected final double cosBottomLat;
  /** The top plane */
  protected final SidedPlane topPlane;
  /** The bottom plane */
  protected final SidedPlane bottomPlane;
  /** An interior point */
  protected final GeoPoint interiorPoint;
  /** Notable points (none) */
  protected final static GeoPoint[] planePoints = new GeoPoint[0];

  // We need two additional points because a latitude zone's boundaries don't intersect.  This is a very
  // special case that most GeoBBox's do not have.
  
  /** Top boundary point */
  protected final GeoPoint topBoundaryPoint;
  /** Bottom boundary point */
  protected final GeoPoint bottomBoundaryPoint;
  /** A point on each distinct edge */
  protected final GeoPoint[] edgePoints;

  /** Constructor.
   *@param planetModel is the planet model to use.
   *@param topLat is the top latitude.
   *@param bottomLat is the bottom latitude.
   */
  public GeoLatitudeZone(final PlanetModel planetModel, final double topLat, final double bottomLat) {
    super(planetModel);
    this.topLat = topLat;
    this.bottomLat = bottomLat;

    final double sinTopLat = Math.sin(topLat);
    final double sinBottomLat = Math.sin(bottomLat);
    this.cosTopLat = Math.cos(topLat);
    this.cosBottomLat = Math.cos(bottomLat);

    // Compute an interior point.  Pick one whose lat is between top and bottom.
    final double middleLat = (topLat + bottomLat) * 0.5;
    final double sinMiddleLat = Math.sin(middleLat);
    this.interiorPoint = new GeoPoint(planetModel, sinMiddleLat, 0.0, Math.sqrt(1.0 - sinMiddleLat * sinMiddleLat), 1.0);
    this.topBoundaryPoint = new GeoPoint(planetModel, sinTopLat, 0.0, Math.sqrt(1.0 - sinTopLat * sinTopLat), 1.0);
    this.bottomBoundaryPoint = new GeoPoint(planetModel, sinBottomLat, 0.0, Math.sqrt(1.0 - sinBottomLat * sinBottomLat), 1.0);

    this.topPlane = new SidedPlane(interiorPoint, planetModel, sinTopLat);
    this.bottomPlane = new SidedPlane(interiorPoint, planetModel, sinBottomLat);

    this.edgePoints = new GeoPoint[]{topBoundaryPoint, bottomBoundaryPoint};
  }

  @Override
  public GeoBBox expand(final double angle) {
    final double newTopLat = topLat + angle;
    final double newBottomLat = bottomLat - angle;
    return GeoBBoxFactory.makeGeoBBox(planetModel, newTopLat, newBottomLat, -Math.PI, Math.PI);
  }

  @Override
  public boolean isWithin(final double x, final double y, final double z) {
    return topPlane.isWithin(x, y, z) &&
        bottomPlane.isWithin(x, y, z);
  }

  @Override
  public double getRadius() {
    // This is a bit tricky.  I guess we should interpret this as meaning the angle of a circle that
    // would contain all the bounding box points, when starting in the "center".
    if (topLat > 0.0 && bottomLat < 0.0)
      return Math.PI;
    double maxCosLat = cosTopLat;
    if (maxCosLat < cosBottomLat)
      maxCosLat = cosBottomLat;
    return maxCosLat * Math.PI;
  }

  @Override
  public GeoPoint getCenter() {
    // This is totally arbitrary and only a cartesian could agree with it.
    return interiorPoint;
  }

  @Override
  public GeoPoint[] getEdgePoints() {
    return edgePoints;
  }

  @Override
  public boolean intersects(final Plane p, final GeoPoint[] notablePoints, final Membership... bounds) {
    return p.intersects(planetModel, topPlane, notablePoints, planePoints, bounds, bottomPlane) ||
        p.intersects(planetModel, bottomPlane, notablePoints, planePoints, bounds, topPlane);
  }

  @Override
  public void getBounds(Bounds bounds) {
    super.getBounds(bounds);
    bounds.noLongitudeBound()
      .addHorizontalPlane(planetModel, topLat, topPlane)
      .addHorizontalPlane(planetModel, bottomLat, bottomPlane);
  }

  @Override
  public int getRelationship(final GeoShape path) {
    final int insideRectangle = isShapeInsideBBox(path);
    if (insideRectangle == SOME_INSIDE)
      return OVERLAPS;

    final boolean topBoundaryInsideShape = path.isWithin(topBoundaryPoint);
    final boolean bottomBoundaryInsideShape = path.isWithin(bottomBoundaryPoint);

    if (topBoundaryInsideShape && !bottomBoundaryInsideShape ||
        !topBoundaryInsideShape && bottomBoundaryInsideShape)
      return OVERLAPS;

    final boolean insideShape = topBoundaryInsideShape && bottomBoundaryInsideShape;

    if (insideRectangle == ALL_INSIDE && insideShape)
      return OVERLAPS;

    // Second, the shortcut of seeing whether endpoints are in/out is not going to
    // work with no area endpoints.  So we rely entirely on intersections.

    if (path.intersects(topPlane, planePoints, bottomPlane) ||
        path.intersects(bottomPlane, planePoints, topPlane))
      return OVERLAPS;

    // There is another case for latitude zones only.  This is when the boundaries of the shape all fit
    // within the zone, but the shape includes areas outside the zone crossing a pole.
    // In this case, the above "overlaps" check is insufficient.  We also need to check a point on either boundary
    // whether it is within the shape.  If both such points are within, then CONTAINS is the right answer.  If
    // one such point is within, then OVERLAPS is the right answer.

    if (insideShape)
      return CONTAINS;

    if (insideRectangle == ALL_INSIDE)
      return WITHIN;

    return DISJOINT;
  }

  @Override
  protected double outsideDistance(final DistanceStyle distanceStyle, final double x, final double y, final double z) {
    final double topDistance = distanceStyle.computeDistance(planetModel, topPlane, x,y,z, bottomPlane);
    final double bottomDistance = distanceStyle.computeDistance(planetModel, bottomPlane, x,y,z, topPlane);

    return Math.min(topDistance, bottomDistance);
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof GeoLatitudeZone))
      return false;
    GeoLatitudeZone other = (GeoLatitudeZone) o;
    return super.equals(other) && other.topBoundaryPoint.equals(topBoundaryPoint) && other.bottomBoundaryPoint.equals(bottomBoundaryPoint);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + topBoundaryPoint.hashCode();
    result = 31 * result + bottomBoundaryPoint.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "GeoLatitudeZone: {planetmodel="+planetModel+", toplat=" + topLat + "(" + topLat * 180.0 / Math.PI + "), bottomlat=" + bottomLat + "(" + bottomLat * 180.0 / Math.PI + ")}";
  }
}
