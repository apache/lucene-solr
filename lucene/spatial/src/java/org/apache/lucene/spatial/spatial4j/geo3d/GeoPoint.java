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
 * This class represents a point on the surface of a unit sphere.
 *
 * @lucene.experimental
 */
public class GeoPoint extends Vector {
  
  /** This is the lazily-evaluated magnitude.  Some constructors include it, but others don't, and
   * we try not to create extra computation by always computing it. */
  protected double magnitude = Double.NEGATIVE_INFINITY;
  
  /** Construct a GeoPoint from the trig functions of a lat and lon pair.
   * @param planetModel is the planetModel to put the point on.
   * @param sinLat is the sin of the latitude.
   * @param sinLon is the sin of the longitude.
   * @param cosLat is the cos of the latitude.
   * @param cosLon is the cos of the longitude.
   */
  public GeoPoint(final PlanetModel planetModel, final double sinLat, final double sinLon, final double cosLat, final double cosLon) {
    this(computeDesiredEllipsoidMagnitude(planetModel, cosLat * cosLon, cosLat * sinLon, sinLat),
      cosLat * cosLon, cosLat * sinLon, sinLat);
  }

  /** Construct a GeoPoint from a latitude/longitude pair.
   * @param planetModel is the planetModel to put the point on.
   * @param lat is the latitude.
   * @param lon is the longitude.
   */
  public GeoPoint(final PlanetModel planetModel, final double lat, final double lon) {
    this(planetModel, Math.sin(lat), Math.sin(lon), Math.cos(lat), Math.cos(lon));
  }

  /** Construct a GeoPoint from a unit (x,y,z) vector and a magnitude.
   * @param magnitude is the desired magnitude, provided to put the point on the ellipsoid.
   * @param x is the unit x value.
   * @param y is the unit y value.
   * @param z is the unit z value.
   */
  public GeoPoint(final double magnitude, final double x, final double y, final double z) {
    super(x * magnitude, y * magnitude, z * magnitude);
    this.magnitude = magnitude;
  }
  
  /** Construct a GeoPoint from an (x,y,z) value.
   * The (x,y,z) tuple must be on the desired ellipsoid.
   * @param x is the ellipsoid point x value.
   * @param y is the ellipsoid point y value.
   * @param z is the ellipsoid point z value.
   */
  public GeoPoint(final double x, final double y, final double z) {
    super(x, y, z);
  }

  /** Compute an arc distance between two points.
   * @param v is the second point.
   * @return the angle, in radians, between the two points.
   */
  public double arcDistance(final GeoPoint v) {
    return Tools.safeAcos(dotProduct(v)/(magnitude() * v.magnitude()));
  }

  /** Compute the latitude for the point.
   *@return the latitude.
   */
  public double getLatitude() {
    return Math.asin(z / magnitude() );
  }
  
  /** Compute the longitude for the point.
   * @return the longitude value.  Uses 0.0 if there is no computable longitude.
   */
  public double getLongitude() {
    if (Math.abs(x) < MINIMUM_RESOLUTION && Math.abs(y) < MINIMUM_RESOLUTION)
      return 0.0;
    return Math.atan2(y,x);
  }
  
  /** Compute the linear magnitude of the point.
   * @return the magnitude.
   */
  @Override
  public double magnitude() {
    if (this.magnitude == Double.NEGATIVE_INFINITY) {
      this.magnitude = super.magnitude();
    }
    return magnitude;
  }
}
