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
 * 3D rectangle, bounded on six sides by X,Y,Z limits, degenerate in X and Y. This figure, in fact,
 * represents either zero, one, or two points, so the actual data stored is minimal.
 *
 * @lucene.internal
 */
class dXdYZSolid extends BaseXYZSolid {

  /** X */
  protected final double X;
  /** Y */
  protected final double Y;
  /** Min-Z */
  protected final double minZ;
  /** Max-Z */
  protected final double maxZ;

  /** The points in this figure on the planet surface; also doubles for edge points */
  protected final GeoPoint[] surfacePoints;

  /**
   * Sole constructor
   *
   * @param planetModel is the planet model.
   * @param X is the X value.
   * @param Y is the Y value.
   * @param minZ is the minimum Z value.
   * @param maxZ is the maximum Z value.
   */
  public dXdYZSolid(
      final PlanetModel planetModel,
      final double X,
      final double Y,
      final double minZ,
      final double maxZ) {
    super(planetModel);
    // Argument checking
    if (maxZ - minZ < Vector.MINIMUM_RESOLUTION)
      throw new IllegalArgumentException("Z values in wrong order or identical");

    this.X = X;
    this.Y = Y;
    this.minZ = minZ;
    this.maxZ = maxZ;

    // Build the planes and intersect them.
    final Plane xPlane = new Plane(xUnitVector, -X);
    final Plane yPlane = new Plane(yUnitVector, -Y);
    final SidedPlane minZPlane = new SidedPlane(0.0, 0.0, maxZ, zUnitVector, -minZ);
    final SidedPlane maxZPlane = new SidedPlane(0.0, 0.0, minZ, zUnitVector, -maxZ);
    surfacePoints = xPlane.findIntersections(planetModel, yPlane, minZPlane, maxZPlane);
  }

  /**
   * Constructor for deserialization.
   *
   * @param planetModel is the planet model.
   * @param inputStream is the input stream.
   */
  public dXdYZSolid(final PlanetModel planetModel, final InputStream inputStream)
      throws IOException {
    this(
        planetModel,
        SerializableObject.readDouble(inputStream),
        SerializableObject.readDouble(inputStream),
        SerializableObject.readDouble(inputStream),
        SerializableObject.readDouble(inputStream));
  }

  @Override
  public void write(final OutputStream outputStream) throws IOException {
    SerializableObject.writeDouble(outputStream, X);
    SerializableObject.writeDouble(outputStream, Y);
    SerializableObject.writeDouble(outputStream, minZ);
    SerializableObject.writeDouble(outputStream, maxZ);
  }

  @Override
  protected GeoPoint[] getEdgePoints() {
    return surfacePoints;
  }

  @Override
  public boolean isWithin(final double x, final double y, final double z) {
    for (final GeoPoint p : surfacePoints) {
      if (p.isIdentical(x, y, z)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public int getRelationship(final GeoShape path) {
    // System.err.println(this + " getRelationship with " + path);
    final int insideRectangle = isShapeInsideArea(path);
    if (insideRectangle == SOME_INSIDE) {
      // System.err.println(" some inside");
      return OVERLAPS;
    }

    // Figure out if the entire XYZArea is contained by the shape.
    final int insideShape = isAreaInsideShape(path);
    if (insideShape == SOME_INSIDE) {
      return OVERLAPS;
    }

    if (insideRectangle == ALL_INSIDE && insideShape == ALL_INSIDE) {
      // System.err.println(" inside of each other");
      return OVERLAPS;
    }

    if (insideRectangle == ALL_INSIDE) {
      return WITHIN;
    }

    if (insideShape == ALL_INSIDE) {
      // System.err.println(" shape contains rectangle");
      return CONTAINS;
    }
    // System.err.println(" disjoint");
    return DISJOINT;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof dXdYZSolid)) {
      return false;
    }
    dXdYZSolid other = (dXdYZSolid) o;
    if (!super.equals(other) || surfacePoints.length != other.surfacePoints.length) {
      return false;
    }
    for (int i = 0; i < surfacePoints.length; i++) {
      if (!surfacePoints[i].equals(other.surfacePoints[i])) {
        return false;
      }
    }
    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    for (final GeoPoint p : surfacePoints) {
      result = 31 * result + p.hashCode();
    }
    return result;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    for (final GeoPoint p : surfacePoints) {
      sb.append(" ").append(p).append(" ");
    }
    return "dXdYZSolid: {planetmodel=" + planetModel + ", " + sb.toString() + "}";
  }
}
