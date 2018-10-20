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
 * This GeoBBox represents an area rectangle limited only in north latitude.
 *
 * @lucene.internal
 */
class GeoSouthLatitudeZone extends GeoBaseBBox {
  /** The top latitude of the zone */
  protected final double topLat;
  /** The cosine of the top latitude of the zone */
  protected final double cosTopLat;
  /** The top plane of the zone */
  protected final SidedPlane topPlane;
  /** An interior point of the zone */
  protected final GeoPoint interiorPoint;
  /** Notable points for the plane (none) */
  protected final static GeoPoint[] planePoints = new GeoPoint[0];
  /** A point on the top boundary */
  protected final GeoPoint topBoundaryPoint;
  /** Edge points; a reference to the topBoundaryPoint */
  protected final GeoPoint[] edgePoints;

  /** Constructor.
   *@param planetModel is the planet model.
   *@param topLat is the top latitude of the zone.
   */
  public GeoSouthLatitudeZone(final PlanetModel planetModel, final double topLat) {
    super(planetModel);
    this.topLat = topLat;

    final double sinTopLat = Math.sin(topLat);
    this.cosTopLat = Math.cos(topLat);

    // Compute an interior point.  Pick one whose lat is between top and bottom.
    final double middleLat = (topLat - Math.PI * 0.5) * 0.5;
    final double sinMiddleLat = Math.sin(middleLat);
    this.interiorPoint = new GeoPoint(planetModel, sinMiddleLat, 0.0, Math.sqrt(1.0 - sinMiddleLat * sinMiddleLat), 1.0);
    this.topBoundaryPoint = new GeoPoint(planetModel, sinTopLat, 0.0, Math.sqrt(1.0 - sinTopLat * sinTopLat), 1.0);

    this.topPlane = new SidedPlane(interiorPoint, planetModel, sinTopLat);

    this.edgePoints = new GeoPoint[]{topBoundaryPoint};
  }

  /**
   * Constructor for deserialization.
   * @param planetModel is the planet model.
   * @param inputStream is the input stream.
   */
  public GeoSouthLatitudeZone(final PlanetModel planetModel, final InputStream inputStream) throws IOException {
    this(planetModel, SerializableObject.readDouble(inputStream));
  }

  @Override
  public void write(final OutputStream outputStream) throws IOException {
    SerializableObject.writeDouble(outputStream, topLat);
  }

  @Override
  public GeoBBox expand(final double angle) {
    final double newTopLat = topLat + angle;
    final double newBottomLat = -Math.PI * 0.5;
    return GeoBBoxFactory.makeGeoBBox(planetModel, newTopLat, newBottomLat, -Math.PI, Math.PI);
  }

  @Override
  public boolean isWithin(final double x, final double y, final double z) {
    return topPlane.isWithin(x, y, z);
  }

  @Override
  public double getRadius() {
    // This is a bit tricky.  I guess we should interpret this as meaning the angle of a circle that
    // would contain all the bounding box points, when starting in the "center".
    if (topLat > 0.0)
      return Math.PI;
    double maxCosLat = cosTopLat;
    return maxCosLat * Math.PI;
  }

  /**
   * Returns the center of a circle into which the area will be inscribed.
   *
   * @return the center.
   */
  @Override
  public GeoPoint getCenter() {
    return interiorPoint;
  }

  @Override
  public GeoPoint[] getEdgePoints() {
    return edgePoints;
  }

  @Override
  public boolean intersects(final Plane p, final GeoPoint[] notablePoints, final Membership... bounds) {
    return p.intersects(planetModel, topPlane, notablePoints, planePoints, bounds);
  }

  @Override
  public boolean intersects(final GeoShape geoShape) {
    return geoShape.intersects(topPlane, planePoints);
  }

  @Override
  public void getBounds(Bounds bounds) {
    super.getBounds(bounds);
    bounds
      .addHorizontalPlane(planetModel, topLat, topPlane);
  }

  @Override
  protected double outsideDistance(final DistanceStyle distanceStyle, final double x, final double y, final double z) {
    return distanceStyle.computeDistance(planetModel, topPlane, x,y,z);
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof GeoSouthLatitudeZone))
      return false;
    GeoSouthLatitudeZone other = (GeoSouthLatitudeZone) o;
    return super.equals(other) && other.topBoundaryPoint.equals(topBoundaryPoint);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + topBoundaryPoint.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "GeoSouthLatitudeZone: {planetmodel="+planetModel+", toplat=" + topLat + "(" + topLat * 180.0 / Math.PI + ")}";
  }
}

