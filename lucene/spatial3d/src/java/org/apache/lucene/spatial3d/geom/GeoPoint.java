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
 * This class represents a point on the surface of a sphere or ellipsoid.
 *
 * @lucene.experimental
 */
public class GeoPoint extends Vector {
  
  // By making lazily-evaluated variables be "volatile", we guarantee atomicity when they
  // are updated.  This is necessary if we are using these classes in a multi-thread fashion,
  // because we don't try to synchronize for the lazy computation.
  
  /** This is the lazily-evaluated magnitude.  Some constructors include it, but others don't, and
   * we try not to create extra computation by always computing it.  Does not need to be
   * synchronized for thread safety, because depends wholly on immutable variables of this class. */
  protected volatile double magnitude = Double.NEGATIVE_INFINITY;
  /** Lazily-evaluated latitude.  Does not need to be
   * synchronized for thread safety, because depends wholly on immutable variables of this class.  */
  protected volatile double latitude = Double.NEGATIVE_INFINITY;
  /** Lazily-evaluated longitude.   Does not need to be
   * synchronized for thread safety, because depends wholly on immutable variables of this class.  */
  protected volatile double longitude = Double.NEGATIVE_INFINITY;

  /** Construct a GeoPoint from the trig functions of a lat and lon pair.
   * @param planetModel is the planetModel to put the point on.
   * @param sinLat is the sin of the latitude.
   * @param sinLon is the sin of the longitude.
   * @param cosLat is the cos of the latitude.
   * @param cosLon is the cos of the longitude.
   * @param lat is the latitude.
   * @param lon is the longitude.
   */
  public GeoPoint(final PlanetModel planetModel, final double sinLat, final double sinLon, final double cosLat, final double cosLon, final double lat, final double lon) {
    this(computeDesiredEllipsoidMagnitude(planetModel, cosLat * cosLon, cosLat * sinLon, sinLat),
      cosLat * cosLon, cosLat * sinLon, sinLat, lat, lon);
  }
  
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
    this(planetModel, Math.sin(lat), Math.sin(lon), Math.cos(lat), Math.cos(lon), lat, lon);
  }
  
  /** Construct a GeoPoint from a unit (x,y,z) vector and a magnitude.
   * @param magnitude is the desired magnitude, provided to put the point on the ellipsoid.
   * @param x is the unit x value.
   * @param y is the unit y value.
   * @param z is the unit z value.
   * @param lat is the latitude.
   * @param lon is the longitude.
   */
  public GeoPoint(final double magnitude, final double x, final double y, final double z, double lat, double lon) {
    super(x * magnitude, y * magnitude, z * magnitude);
    this.magnitude = magnitude;
    if (lat > Math.PI * 0.5 || lat < -Math.PI * 0.5) {
      throw new IllegalArgumentException("Latitude " + lat + " is out of range: must range from -Math.PI/2 to Math.PI/2");
    }
    if (lon < -Math.PI || lon > Math.PI) {
      throw new IllegalArgumentException("Longitude " + lon + " is out of range: must range from -Math.PI to Math.PI");
    }
    this.latitude = lat;
    this.longitude = lon;
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
   * Note: this is an angular distance, and not a surface distance, and is therefore independent of planet model.
   * For surface distance, see {@link PlanetModel#surfaceDistance(GeoPoint, GeoPoint)}
   * @param v is the second point.
   * @return the angle, in radians, between the two points.
   */
  public double arcDistance(final Vector v) {
    return Tools.safeAcos(dotProduct(v)/(magnitude() * v.magnitude()));
  }

  /** Compute an arc distance between two points.
   * @param x is the x part of the second point.
   * @param y is the y part of the second point.
   * @param z is the z part of the second point.
   * @return the angle, in radians, between the two points.
   */
  public double arcDistance(final double x, final double y, final double z) {
    return Tools.safeAcos(dotProduct(x,y,z)/(magnitude() * Vector.magnitude(x,y,z)));
  }

  /** Compute the latitude for the point.
   * @return the latitude.
   */
  public double getLatitude() {
    double lat = this.latitude;//volatile-read once
    if (lat == Double.NEGATIVE_INFINITY)
      this.latitude = lat = Math.asin(z / magnitude());
    return lat;
  }
  
  /** Compute the longitude for the point.
   * @return the longitude value.  Uses 0.0 if there is no computable longitude.
   */
  public double getLongitude() {
    double lon = this.longitude;//volatile-read once
    if (lon == Double.NEGATIVE_INFINITY) {
      if (Math.abs(x) < MINIMUM_RESOLUTION && Math.abs(y) < MINIMUM_RESOLUTION)
        this.longitude = lon = 0.0;
      else
        this.longitude = lon = Math.atan2(y,x);
    }
    return lon;
  }
  
  /** Compute the linear magnitude of the point.
   * @return the magnitude.
   */
  @Override
  public double magnitude() {
    double mag = this.magnitude;//volatile-read once
    if (mag == Double.NEGATIVE_INFINITY) {
      this.magnitude = mag = super.magnitude();
    }
    return mag;
  }
  
  /** Compute whether point matches another.
   *@param x is the x value
   *@param y is the y value
   *@param z is the z value
   *@return true if the same.
   */
  public boolean isIdentical(final double x, final double y, final double z) {
    return Math.abs(this.x - x) < MINIMUM_RESOLUTION &&
      Math.abs(this.y - y) < MINIMUM_RESOLUTION &&
      Math.abs(this.z - z) < MINIMUM_RESOLUTION;
  }
  
  @Override
  public String toString() {
    if (this.longitude == Double.NEGATIVE_INFINITY) {
      return super.toString();
    }
    return "[lat="+getLatitude()+", lon="+getLongitude()+"("+super.toString()+")]";
  }
}
