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
 * Circular area with a center and cutoff angle that represents the latitude and longitude distance
 * from the center where the planet will be cut. The resulting area is a circle for spherical
 * planets and an ellipse otherwise.
 *
 * @lucene.experimental
 */
class GeoStandardCircle extends GeoBaseCircle {
  /** Center of circle */
  protected final GeoPoint center;
  /** Cutoff angle of circle (not quite the same thing as radius) */
  protected final double cutoffAngle;
  /** The plane describing the circle (really an ellipse on a non-spherical world) */
  protected final SidedPlane circlePlane;
  /** A point that is on the world and on the circle plane */
  protected final GeoPoint[] edgePoints;
  /** Notable points for a circle -- there aren't any */
  protected static final GeoPoint[] circlePoints = new GeoPoint[0];

  /**
   * Constructor.
   *
   * @param planetModel is the planet model.
   * @param lat is the center latitude.
   * @param lon is the center longitude.
   * @param cutoffAngle is the cutoff angle for the circle.
   */
  public GeoStandardCircle(
      final PlanetModel planetModel, final double lat, final double lon, final double cutoffAngle) {
    super(planetModel);
    if (lat < -Math.PI * 0.5 || lat > Math.PI * 0.5)
      throw new IllegalArgumentException("Latitude out of bounds");
    if (lon < -Math.PI || lon > Math.PI)
      throw new IllegalArgumentException("Longitude out of bounds");
    if (cutoffAngle < 0.0 || cutoffAngle > Math.PI)
      throw new IllegalArgumentException("Cutoff angle out of bounds");
    if (cutoffAngle < Vector.MINIMUM_RESOLUTION)
      throw new IllegalArgumentException("Cutoff angle cannot be effectively zero");
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
      if (upperLon > Math.PI) {
        upperLon -= 2.0 * Math.PI;
      }
      upperLat = Math.PI - upperLat;
    }
    double lowerLat = lat - cutoffAngle;
    double lowerLon = lon;
    if (lowerLat < -Math.PI * 0.5) {
      lowerLon += Math.PI;
      if (lowerLon > Math.PI) {
        lowerLon -= 2.0 * Math.PI;
      }
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
      final Plane normalPlane = Plane.constructNormalizedZPlane(upperPoint, lowerPoint, center);
      // Construct a sided plane that goes through the two points and whose normal is in the
      // normalPlane.
      this.circlePlane =
          SidedPlane.constructNormalizedPerpendicularSidedPlane(
              center, normalPlane, upperPoint, lowerPoint);
      if (circlePlane == null) {
        throw new IllegalArgumentException(
            "Couldn't construct circle plane, probably too small?  Cutoff angle = "
                + cutoffAngle
                + "; upperPoint = "
                + upperPoint
                + "; lowerPoint = "
                + lowerPoint);
      }
      final GeoPoint recomputedIntersectionPoint =
          circlePlane.getSampleIntersectionPoint(planetModel, normalPlane);
      if (recomputedIntersectionPoint == null) {
        throw new IllegalArgumentException(
            "Couldn't construct intersection point, probably circle too small?  Plane = "
                + circlePlane);
      }
      this.edgePoints = new GeoPoint[] {recomputedIntersectionPoint};
    }
  }

  /**
   * Constructor for deserialization.
   *
   * @param planetModel is the planet model.
   * @param inputStream is the input stream.
   */
  public GeoStandardCircle(final PlanetModel planetModel, final InputStream inputStream)
      throws IOException {
    this(
        planetModel,
        SerializableObject.readDouble(inputStream),
        SerializableObject.readDouble(inputStream),
        SerializableObject.readDouble(inputStream));
  }

  @Override
  public void write(final OutputStream outputStream) throws IOException {
    SerializableObject.writeDouble(outputStream, center.getLatitude());
    SerializableObject.writeDouble(outputStream, center.getLongitude());
    SerializableObject.writeDouble(outputStream, cutoffAngle);
  }

  @Override
  public double getRadius() {
    return cutoffAngle;
  }

  @Override
  public GeoPoint getCenter() {
    return center;
  }

  @Override
  protected double distance(
      final DistanceStyle distanceStyle, final double x, final double y, final double z) {
    return distanceStyle.computeDistance(this.center, x, y, z);
  }

  @Override
  protected void distanceBounds(
      final Bounds bounds, final DistanceStyle distanceStyle, final double distanceValue) {
    // TBD: Compute actual bounds based on distance
    getBounds(bounds);
  }

  @Override
  protected double outsideDistance(
      final DistanceStyle distanceStyle, final double x, final double y, final double z) {
    return distanceStyle.computeDistance(planetModel, circlePlane, x, y, z);
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
  public boolean intersects(
      final Plane p, final GeoPoint[] notablePoints, final Membership... bounds) {
    if (circlePlane == null) {
      return false;
    }
    return circlePlane.intersects(planetModel, p, notablePoints, circlePoints, bounds);
  }

  @Override
  public boolean intersects(GeoShape geoShape) {
    if (circlePlane == null) {
      return false;
    }
    return geoShape.intersects(circlePlane, circlePoints);
  }

  @Override
  public int getRelationship(GeoShape geoShape) {
    if (circlePlane == null) {
      // same as GeoWorld
      if (geoShape.getEdgePoints().length > 0) {
        return WITHIN;
      }
      return OVERLAPS;
    }
    return super.getRelationship(geoShape);
  }

  @Override
  public void getBounds(Bounds bounds) {
    super.getBounds(bounds);
    if (circlePlane == null) {
      // Entire world; should already be covered
      return;
    }
    bounds.addPoint(center);
    bounds.addPlane(planetModel, circlePlane);
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof GeoStandardCircle)) {
      return false;
    }
    GeoStandardCircle other = (GeoStandardCircle) o;
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
    return "GeoStandardCircle: {planetmodel="
        + planetModel
        + ", center="
        + center
        + ", radius="
        + cutoffAngle
        + "("
        + cutoffAngle * 180.0 / Math.PI
        + ")}";
  }
}
