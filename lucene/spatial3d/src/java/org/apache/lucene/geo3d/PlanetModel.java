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
 * Holds mathematical constants associated with the model of a planet.
 * @lucene.experimental
 */
public class PlanetModel {
  
  /** Planet model corresponding to sphere. */
  public static final PlanetModel SPHERE = new PlanetModel(1.0,1.0);

  /** Mean radius */
  public static final double WGS84_MEAN = 6371009.0;
  /** Polar radius */
  public static final double WGS84_POLAR = 6356752.314245;
  /** Equatorial radius */
  public static final double WGS84_EQUATORIAL = 6378137.0;
  /** Planet model corresponding to WGS84 */
  public static final PlanetModel WGS84 = new PlanetModel(WGS84_EQUATORIAL/WGS84_MEAN,
    WGS84_POLAR/WGS84_MEAN);

  // Surface of the planet:
  // x^2/a^2 + y^2/b^2 + z^2/c^2 = 1.0
  // Scaling factors are a,b,c.  geo3d can only support models where a==b, so use ab instead.
  
  /** The x/y scaling factor */
  public final double ab;
  /** The z scaling factor */
  public final double c;
  /** The inverse of ab */
  public final double inverseAb;
  /** The inverse of c */
  public final double inverseC;
  /** The square of the inverse of ab */
  public final double inverseAbSquared;
  /** The square of the inverse of c */
  public final double inverseCSquared;
  /** The flattening value */
  public final double flattening;
  /** The square ratio */
  public final double squareRatio;
  
  // We do NOT include radius, because all computations in geo3d are in radians, not meters.
  
  // Compute north and south pole for planet model, since these are commonly used.
  
  /** North pole */
  public final GeoPoint NORTH_POLE;
  /** South pole */
  public final GeoPoint SOUTH_POLE;
  /** Min X pole */
  public final GeoPoint MIN_X_POLE;
  /** Max X pole */
  public final GeoPoint MAX_X_POLE;
  /** Min Y pole */
  public final GeoPoint MIN_Y_POLE;
  /** Max Y pole */
  public final GeoPoint MAX_Y_POLE;
  
  /** Constructor.
   * @param ab is the x/y scaling factor.
   * @param c is the z scaling factor.
   */
  public PlanetModel(final double ab, final double c) {
    this.ab = ab;
    this.c = c;
    this.inverseAb = 1.0 / ab;
    this.inverseC = 1.0 / c;
    this.flattening = (ab - c) * inverseAb;
    this.squareRatio = (ab * ab - c * c) / (c * c);
    this.inverseAbSquared = inverseAb * inverseAb;
    this.inverseCSquared = inverseC * inverseC;
    this.NORTH_POLE = new GeoPoint(c, 0.0, 0.0, 1.0, Math.PI * 0.5, 0.0);
    this.SOUTH_POLE = new GeoPoint(c, 0.0, 0.0, -1.0, -Math.PI * 0.5, 0.0);
    this.MIN_X_POLE = new GeoPoint(ab, -1.0, 0.0, 0.0, 0.0, -Math.PI);
    this.MAX_X_POLE = new GeoPoint(ab, 1.0, 0.0, 0.0, 0.0, 0.0);
    this.MIN_Y_POLE = new GeoPoint(ab, 0.0, -1.0, 0.0, 0.0, -Math.PI * 0.5);
    this.MAX_Y_POLE = new GeoPoint(ab, 0.0, 1.0, 0.0, 0.0, Math.PI * 0.5);
  }
  
  /** Find the minimum magnitude of all points on the ellipsoid.
   * @return the minimum magnitude for the planet.
   */
  public double getMinimumMagnitude() {
    return Math.min(this.ab, this.c);
  }

  /** Find the maximum magnitude of all points on the ellipsoid.
   * @return the maximum magnitude for the planet.
   */
  public double getMaximumMagnitude() {
    return Math.max(this.ab, this.c);
  }
  
  /** Find the minimum x value.
   *@return the minimum X value.
   */
  public double getMinimumXValue() {
    return -this.ab;
  }
  
  /** Find the maximum x value.
   *@return the maximum X value.
   */
  public double getMaximumXValue() {
    return this.ab;
  }

  /** Find the minimum y value.
   *@return the minimum Y value.
   */
  public double getMinimumYValue() {
    return -this.ab;
  }
  
  /** Find the maximum y value.
   *@return the maximum Y value.
   */
  public double getMaximumYValue() {
    return this.ab;
  }
  
  /** Find the minimum z value.
   *@return the minimum Z value.
   */
  public double getMinimumZValue() {
    return -this.c;
  }
  
  /** Find the maximum z value.
   *@return the maximum Z value.
   */
  public double getMaximumZValue() {
    return this.c;
  }

  /** Check if point is on surface.
   * @param v is the point to check.
   * @return true if the point is on the planet surface.
   */
  public boolean pointOnSurface(final Vector v) {
    return pointOnSurface(v.x, v.y, v.z);
  }
  
  /** Check if point is on surface.
   * @param x is the x coord.
   * @param y is the y coord.
   * @param z is the z coord.
   */
  public boolean pointOnSurface(final double x, final double y, final double z) {
    // Equation of planet surface is:
    // x^2 / a^2 + y^2 / b^2 + z^2 / c^2 - 1 = 0
    return Math.abs(x * x * inverseAb * inverseAb + y * y * inverseAb * inverseAb + z * z * inverseC * inverseC - 1.0) < Vector.MINIMUM_RESOLUTION;
  }

  /** Check if point is outside surface.
   * @param v is the point to check.
   * @return true if the point is outside the planet surface.
   */
  public boolean pointOutside(final Vector v) {
    return pointOutside(v.x, v.y, v.z);
  }
  
  /** Check if point is outside surface.
   * @param x is the x coord.
   * @param y is the y coord.
   * @param z is the z coord.
   */
  public boolean pointOutside(final double x, final double y, final double z) {
    // Equation of planet surface is:
    // x^2 / a^2 + y^2 / b^2 + z^2 / c^2 - 1 = 0
    return (x * x + y * y) * inverseAb * inverseAb + z * z * inverseC * inverseC - 1.0 > Vector.MINIMUM_RESOLUTION;
  }
  
  /** Compute surface distance between two points.
   * @param p1 is the first point.
   * @param p2 is the second point.
   * @return the adjusted angle, when multiplied by the mean earth radius, yields a surface distance.  This will differ
   * from GeoPoint.arcDistance() only when the planet model is not a sphere. @see {@link org.apache.lucene.geo3d.GeoPoint#arcDistance(GeoPoint)}
   */
  public double surfaceDistance(final GeoPoint p1, final GeoPoint p2) {
    final double latA = p1.getLatitude();
    final double lonA = p1.getLongitude();
    final double latB = p2.getLatitude();
    final double lonB = p2.getLongitude();

    final double L = lonB - lonA;
    final double oF = 1.0 - this.flattening;
    final double U1 = Math.atan(oF * Math.tan(latA));
    final double U2 = Math.atan(oF * Math.tan(latB));
    final double sU1 = Math.sin(U1);
    final double cU1 = Math.cos(U1);
    final double sU2 = Math.sin(U2);
    final double cU2 = Math.cos(U2);

    double sigma, sinSigma, cosSigma;
    double cos2Alpha, cos2SigmaM;
    
    double lambda = L;
    double iters = 100;

    do {
      final double sinLambda = Math.sin(lambda);
      final double cosLambda = Math.cos(lambda);
      sinSigma = Math.sqrt((cU2 * sinLambda) * (cU2 * sinLambda) + (cU1 * sU2 - sU1 * cU2 * cosLambda)
          * (cU1 * sU2 - sU1 * cU2 * cosLambda));
      if (Math.abs(sinSigma) < Vector.MINIMUM_RESOLUTION)
        return 0.0;

      cosSigma = sU1 * sU2 + cU1 * cU2 * cosLambda;
      sigma = Math.atan2(sinSigma, cosSigma);
      final double sinAlpha = cU1 * cU2 * sinLambda / sinSigma;
      cos2Alpha = 1.0 - sinAlpha * sinAlpha;
      cos2SigmaM = cosSigma - 2.0 * sU1 * sU2 / cos2Alpha;

      final double c = this.flattening * 0.625 * cos2Alpha * (4.0 + this.flattening * (4.0 - 3.0 * cos2Alpha));
      final double lambdaP = lambda;
      lambda = L + (1.0 - c) * this.flattening * sinAlpha * (sigma + c * sinSigma * (cos2SigmaM + c * cosSigma *
          (-1.0 + 2.0 * cos2SigmaM * cos2SigmaM)));
      if (Math.abs(lambda - lambdaP) < Vector.MINIMUM_RESOLUTION)
        break;
    } while (--iters > 0);

    if (iters == 0)
      return 0.0;

    final double uSq = cos2Alpha * this.squareRatio;
    final double A = 1.0 + uSq * 0.00006103515625 * (4096.0 + uSq * (-768.0 + uSq * (320.0 - 175.0 * uSq)));
    final double B = uSq * 0.0009765625 * (256.0 + uSq * (-128.0 + uSq * (74.0 - 47.0 * uSq)));
    final double deltaSigma = B * sinSigma * (cos2SigmaM + B * 0.25 * (cosSigma * (-1.0 + 2.0 * cos2SigmaM * cos2SigmaM) - B * 0.16666666666666666666667 * cos2SigmaM
            * (-3.0 + 4.0 * sinSigma * sinSigma) * (-3.0 + 4.0 * cos2SigmaM * cos2SigmaM)));

    return this.c * A * (sigma - deltaSigma);
  }

  @Override
  public boolean equals(final Object o) {
    if (!(o instanceof PlanetModel))
      return false;
    final PlanetModel other = (PlanetModel)o;
    return ab == other.ab && c == other.c;
  }

  @Override
  public int hashCode() {
    int result;
    long temp;
    temp = Double.doubleToLongBits(ab);
    result = (int) (temp ^ (temp >>> 32));
    temp = Double.doubleToLongBits(c);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    return result;
  }

  @Override
  public String toString() {
    if (this.equals(SPHERE)) {
      return "PlanetModel.SPHERE";
    } else if (this.equals(WGS84)) {
      return "PlanetModel.WGS84";
    } else {
      return "PlanetModel(ab="+ab+" c="+c+")";
    }
  }
}


