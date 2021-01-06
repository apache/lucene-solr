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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Fast implementation of a polygon representing S2 geometry cell. There are no checks validating
 * that points are convex therefore users must be provide four points in CCW or the logic will fail.
 *
 * @lucene.internal
 */
class GeoS2Shape extends GeoBasePolygon {

  /** The first point */
  protected final GeoPoint point1;
  /** The second point */
  protected final GeoPoint point2;
  /** The third point */
  protected final GeoPoint point3;
  /** The fourth point */
  protected final GeoPoint point4;

  /** The first plane */
  protected final SidedPlane plane1;
  /** The second plane */
  protected final SidedPlane plane2;
  /** The third plane */
  protected final SidedPlane plane3;
  /** The fourth plane */
  protected final SidedPlane plane4;

  /** Notable points for the first plane */
  protected final GeoPoint[] plane1Points;
  /** Notable points for second plane */
  protected final GeoPoint[] plane2Points;
  /** Notable points for third plane */
  protected final GeoPoint[] plane3Points;
  /** Notable points for fourth plane */
  protected final GeoPoint[] plane4Points;

  /** Edge point for this S2 cell */
  protected final GeoPoint[] edgePoints;

  /**
   * It builds from 4 points given in CCW. It must be convex or logic will fail.
   *
   * @param planetModel is the planet model.
   * @param point1 the first point.
   * @param point2 the second point.
   * @param point3 the third point.
   * @param point4 the four point.
   */
  public GeoS2Shape(
      final PlanetModel planetModel,
      GeoPoint point1,
      GeoPoint point2,
      GeoPoint point3,
      GeoPoint point4) {
    super(planetModel);
    this.point1 = point1;
    this.point2 = point2;
    this.point3 = point3;
    this.point4 = point4;

    // Now build the four planes
    this.plane1 = new SidedPlane(point4, point1, point2);
    this.plane2 = new SidedPlane(point1, point2, point3);
    this.plane3 = new SidedPlane(point2, point3, point4);
    this.plane4 = new SidedPlane(point3, point4, point1);

    // collect the notable points for the planes
    this.plane1Points = new GeoPoint[] {point1, point2};
    this.plane2Points = new GeoPoint[] {point2, point3};
    this.plane3Points = new GeoPoint[] {point3, point4};
    this.plane4Points = new GeoPoint[] {point4, point1};

    this.edgePoints = new GeoPoint[] {point1};
  }

  /**
   * Constructor for deserialization.
   *
   * @param planetModel is the planet model.
   * @param inputStream is the input stream.
   */
  public GeoS2Shape(final PlanetModel planetModel, final InputStream inputStream)
      throws IOException {
    this(
        planetModel,
        (GeoPoint) SerializableObject.readObject(inputStream),
        (GeoPoint) SerializableObject.readObject(inputStream),
        (GeoPoint) SerializableObject.readObject(inputStream),
        (GeoPoint) SerializableObject.readObject(inputStream));
  }

  @Override
  public void write(final OutputStream outputStream) throws IOException {
    SerializableObject.writeObject(outputStream, point1);
    SerializableObject.writeObject(outputStream, point2);
    SerializableObject.writeObject(outputStream, point3);
    SerializableObject.writeObject(outputStream, point4);
  }

  @Override
  public boolean isWithin(final double x, final double y, final double z) {
    return plane1.isWithin(x, y, z)
        && plane2.isWithin(x, y, z)
        && plane3.isWithin(x, y, z)
        && plane4.isWithin(x, y, z);
  }

  @Override
  public GeoPoint[] getEdgePoints() {
    return edgePoints;
  }

  @Override
  public boolean intersects(
      final Plane p, final GeoPoint[] notablePoints, final Membership... bounds) {
    return p.intersects(planetModel, plane1, notablePoints, plane1Points, bounds, plane2, plane4)
        || p.intersects(planetModel, plane2, notablePoints, plane2Points, bounds, plane3, plane1)
        || p.intersects(planetModel, plane3, notablePoints, plane3Points, bounds, plane4, plane2)
        || p.intersects(planetModel, plane4, notablePoints, plane4Points, bounds, plane1, plane3);
  }

  @Override
  public boolean intersects(GeoShape geoShape) {
    return geoShape.intersects(plane1, plane1Points, plane2, plane4)
        || geoShape.intersects(plane2, plane2Points, plane3, plane1)
        || geoShape.intersects(plane3, plane3Points, plane4, plane2)
        || geoShape.intersects(plane4, plane4Points, plane1, plane3);
  }

  @Override
  public void getBounds(Bounds bounds) {
    super.getBounds(bounds);
    bounds
        .addPlane(planetModel, plane1, plane2, plane4)
        .addPlane(planetModel, plane2, plane3, plane1)
        .addPlane(planetModel, plane3, plane4, plane2)
        .addPlane(planetModel, plane4, plane1, plane3)
        .addPoint(point1)
        .addPoint(point2)
        .addPoint(point3)
        .addPoint(point4);
  }

  @Override
  public double outsideDistance(DistanceStyle distanceStyle, double x, double y, double z) {
    final double planeDistance1 =
        distanceStyle.computeDistance(planetModel, plane1, x, y, z, plane2, plane4);
    final double planeDistance2 =
        distanceStyle.computeDistance(planetModel, plane2, x, y, z, plane3, plane1);
    final double planeDistance3 =
        distanceStyle.computeDistance(planetModel, plane3, x, y, z, plane4, plane2);
    final double planeDistance4 =
        distanceStyle.computeDistance(planetModel, plane4, x, y, z, plane1, plane3);

    final double pointDistance1 = distanceStyle.computeDistance(point1, x, y, z);
    final double pointDistance2 = distanceStyle.computeDistance(point2, x, y, z);
    final double pointDistance3 = distanceStyle.computeDistance(point3, x, y, z);
    final double pointDistance4 = distanceStyle.computeDistance(point4, x, y, z);

    return Math.min(
        Math.min(
            Math.min(planeDistance1, planeDistance2), Math.min(planeDistance3, planeDistance4)),
        Math.min(
            Math.min(pointDistance1, pointDistance2), Math.min(pointDistance3, pointDistance4)));
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof GeoS2Shape)) {
      return false;
    }
    GeoS2Shape other = (GeoS2Shape) o;
    return super.equals(other)
        && other.point1.equals(point1)
        && other.point2.equals(point2)
        && other.point3.equals(point3)
        && other.point4.equals(point4);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + point1.hashCode();
    result = 31 * result + point2.hashCode();
    result = 31 * result + point3.hashCode();
    result = 31 * result + point4.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "GeoS2Shape: {planetmodel="
        + planetModel
        + ", point1="
        + point1
        + ", point2="
        + point2
        + ", point3="
        + point3
        + ", point4="
        + point4
        + "}";
  }
}
