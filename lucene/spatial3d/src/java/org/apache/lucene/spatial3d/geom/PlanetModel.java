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
 * Holds mathematical constants associated with the model of a planet.
 * @lucene.experimental
 */
public class PlanetModel implements SerializableObject {
  
  /** Planet model corresponding to sphere. */
  public static final PlanetModel SPHERE = new PlanetModel(1.0,1.0);

  /** Mean radius */
  // see http://earth-info.nga.mil/GandG/publications/tr8350.2/wgs84fin.pdf
  public static final double WGS84_MEAN = 6371008.7714;
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
  /** The scale of the planet */
  public final double scale;
  /** The inverse of scale */
  public final double inverseScale;


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
  /** Minimum surface distance between poles */
  public final double minimumPoleDistance;
  
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
    this.scale = (2.0 * ab + c)/3.0;
    this.inverseScale = 1.0 / scale;
    this.minimumPoleDistance  = Math.min(surfaceDistance(NORTH_POLE, SOUTH_POLE), surfaceDistance(MIN_X_POLE, MAX_X_POLE));
  }

  /** Deserialization constructor.
   * @param inputStream is the input stream.
   */
  public PlanetModel(final InputStream inputStream) throws IOException {
    this(SerializableObject.readDouble(inputStream), SerializableObject.readDouble(inputStream));
  }
  
  @Override
  public void write(final OutputStream outputStream) throws IOException {
    SerializableObject.writeDouble(outputStream, ab);
    SerializableObject.writeDouble(outputStream, c);
  }
  
  /** Does this planet model describe a sphere?
   *@return true if so.
   */
  public boolean isSphere() {
    return this.ab == this.c;
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
  
  /** Compute a GeoPoint that's scaled to actually be on the planet surface.
   * @param vector is the vector.
   * @return the scaled point.
   */
  public GeoPoint createSurfacePoint(final Vector vector) {
    return createSurfacePoint(vector.x, vector.y, vector.z);
  }

  /** Compute a GeoPoint that's based on (x,y,z) values, but is scaled to actually be on the planet surface.
   * @param x is the x value.
   * @param y is the y value.
   * @param z is the z value.
   * @return the scaled point.
   */
  public GeoPoint createSurfacePoint(final double x, final double y, final double z) {
    // The equation of the surface is:
    // (x^2 / a^2 + y^2 / b^2 + z^2 / c^2) = 1
    // We will need to scale the passed-in x, y, z values:
    // ((tx)^2 / a^2 + (ty)^2 / b^2 + (tz)^2 / c^2) = 1
    // t^2 * (x^2 / a^2 + y^2 / b^2 + z^2 / c^2)  = 1
    // t = sqrt ( 1 / (x^2 / a^2 + y^2 / b^2 + z^2 / c^2))
    final double t = Math.sqrt(1.0 / (x*x*inverseAbSquared + y*y*inverseAbSquared + z*z*inverseCSquared));
    return new GeoPoint(t*x, t*y, t*z);
  }
  
  /** Compute a GeoPoint that's a bisection between two other GeoPoints.
   * @param pt1 is the first point.
   * @param pt2 is the second point.
   * @return the bisection point, or null if a unique one cannot be found.
   */
  public GeoPoint bisection(final GeoPoint pt1, final GeoPoint pt2) {
    final double A0 = (pt1.x + pt2.x) * 0.5;
    final double B0 = (pt1.y + pt2.y) * 0.5;
    final double C0 = (pt1.z + pt2.z) * 0.5;
      
    final double denom = inverseAbSquared * A0 * A0 +
      inverseAbSquared * B0 * B0 +
      inverseCSquared * C0 * C0;
          
    if(denom < Vector.MINIMUM_RESOLUTION) {
      // Bisection is undefined
      return null;
    }
      
    final double t = Math.sqrt(1.0 / denom);
      
    return new GeoPoint(t * A0, t * B0, t * C0);
  }
  
  /** Compute surface distance between two points.
   * @param pt1 is the first point.
   * @param pt2 is the second point.
   * @return the adjusted angle, when multiplied by the mean earth radius, yields a surface distance.  This will differ
   * from GeoPoint.arcDistance() only when the planet model is not a sphere. @see {@link GeoPoint#arcDistance(Vector)}
   */
  public double surfaceDistance(final GeoPoint pt1, final GeoPoint pt2) {
    final double L = pt2.getLongitude() - pt1.getLongitude();
    final double U1 = Math.atan((1.0-flattening) * Math.tan(pt1.getLatitude()));
    final double U2 = Math.atan((1.0-flattening) * Math.tan(pt2.getLatitude()));

    final double sinU1 = Math.sin(U1);
    final double cosU1 = Math.cos(U1);
    final double sinU2 = Math.sin(U2);
    final double cosU2 = Math.cos(U2);

    final double dCosU1CosU2 = cosU1 * cosU2;
    final double dCosU1SinU2 = cosU1 * sinU2;

    final double dSinU1SinU2 = sinU1 * sinU2;
    final double dSinU1CosU2 = sinU1 * cosU2;


    double lambda = L;
    double lambdaP = Math.PI * 2.0;
    int iterLimit = 0;
    double cosSqAlpha;
    double sinSigma;
    double cos2SigmaM;
    double cosSigma;
    double sigma;
    double sinAlpha;
    double C;
    double sinLambda, cosLambda;

    do {
      sinLambda = Math.sin(lambda);
      cosLambda = Math.cos(lambda);
      sinSigma = Math.sqrt((cosU2*sinLambda) * (cosU2*sinLambda) +
                                    (dCosU1SinU2 - dSinU1CosU2 * cosLambda) * (dCosU1SinU2 - dSinU1CosU2 * cosLambda));

      if (sinSigma==0.0) {
        return 0.0;
      }
      cosSigma = dSinU1SinU2 + dCosU1CosU2 * cosLambda;
      sigma = Math.atan2(sinSigma, cosSigma);
      sinAlpha = dCosU1CosU2 * sinLambda / sinSigma;
      cosSqAlpha = 1.0 - sinAlpha * sinAlpha;
      cos2SigmaM = cosSigma - 2.0 * dSinU1SinU2 / cosSqAlpha;

      if (Double.isNaN(cos2SigmaM))
        cos2SigmaM = 0.0;  // equatorial line: cosSqAlpha=0
      C = flattening / 16.0 * cosSqAlpha * (4.0 + flattening * (4.0 - 3.0 * cosSqAlpha));
      lambdaP = lambda;
      lambda = L + (1.0 - C) * flattening * sinAlpha *
        (sigma + C * sinSigma * (cos2SigmaM + C * cosSigma * (-1.0 + 2.0 * cos2SigmaM *cos2SigmaM)));
    } while (Math.abs(lambda-lambdaP) >= Vector.MINIMUM_RESOLUTION && ++iterLimit < 100);
    final double uSq = cosSqAlpha * this.squareRatio;
    final double A = 1.0 + uSq / 16384.0 * (4096.0 + uSq * (-768.0 + uSq * (320.0 - 175.0 * uSq)));
    final double B = uSq / 1024.0 * (256.0 + uSq * (-128.0 + uSq * (74.0 - 47.0 * uSq)));
    final double deltaSigma = B * sinSigma * (cos2SigmaM + B / 4.0 * (cosSigma * (-1.0 + 2.0 * cos2SigmaM * cos2SigmaM)-
                                        B / 6.0 * cos2SigmaM * (-3.0 + 4.0 * sinSigma * sinSigma) * (-3.0 + 4.0 * cos2SigmaM * cos2SigmaM)));

    return c * inverseScale * A * (sigma - deltaSigma);
  }

  /** Compute new point given original point, a bearing direction, and an adjusted angle (as would be computed by
   * the surfaceDistance() method above).  The original point can be anywhere on the globe.  The bearing direction
   * ranges from 0 (due east at the equator) to pi/2 (due north) to pi (due west at the equator) to 3 pi/4 (due south)
   * to 2 pi.
   * @param from is the starting point.
   * @param dist is the adjusted angle.
   * @param bearing is the direction to proceed.
   * @return the new point, consistent with the bearing direction and distance.
   */
  public GeoPoint surfacePointOnBearing(final GeoPoint from, final double dist, final double bearing) {
    // Algorithm using Vincenty's formulae (https://en.wikipedia.org/wiki/Vincenty%27s_formulae)
    // which takes into account that planets may not be spherical.
    //Code adaptation from http://www.movable-type.co.uk/scripts/latlong-vincenty.html

    double lat = from.getLatitude();
    double lon = from.getLongitude();
    double sinα1 = Math.sin(bearing);
    double cosα1 = Math.cos(bearing);

    double tanU1 = (1.0 - flattening) * Math.tan(lat);
    double cosU1 = 1.0 / Math.sqrt((1.0 + tanU1 * tanU1));
    double sinU1 = tanU1 * cosU1;

    double σ1 = Math.atan2(tanU1, cosα1);
    double sinα = cosU1 * sinα1;
    double cosSqα = 1.0 - sinα * sinα;
    double uSq = cosSqα * squareRatio;
    double A = 1.0 + uSq / 16384.0 * (4096.0 + uSq * (-768.0 + uSq * (320.0 - 175.0 * uSq)));
    double B = uSq / 1024.0 * (256.0 + uSq * (-128.0 + uSq * (74.0 - 47.0 * uSq)));

    double cos2σM;
    double sinσ;
    double cosσ;
    double Δσ;

    double σ = dist / (c * inverseScale * A);
    double σʹ;
    double iterations = 0;
    do {
      cos2σM = Math.cos(2.0 * σ1 + σ);
      sinσ = Math.sin(σ);
      cosσ = Math.cos(σ);
      Δσ = B * sinσ * (cos2σM + B / 4.0 * (cosσ * (-1.0 + 2.0 * cos2σM * cos2σM) -
          B / 6.0 * cos2σM * (-3.0 + 4.0 * sinσ * sinσ) * (-3.0 + 4.0 * cos2σM * cos2σM)));
      σʹ = σ;
      σ = dist / (c * inverseScale * A) + Δσ;
    } while (Math.abs(σ - σʹ) >= Vector.MINIMUM_RESOLUTION && ++iterations < 100);
    double x = sinU1 * sinσ - cosU1 * cosσ * cosα1;
    double φ2 = Math.atan2(sinU1 * cosσ + cosU1 * sinσ * cosα1, (1.0 - flattening) * Math.sqrt(sinα * sinα + x * x));
    double λ = Math.atan2(sinσ * sinα1, cosU1 * cosσ - sinU1 * sinσ * cosα1);
    double C = flattening / 16.0 * cosSqα * (4.0 + flattening * (4.0 - 3.0 * cosSqα));
    double L = λ - (1.0 - C) * flattening * sinα *
        (σ + C * sinσ * (cos2σM + C * cosσ * (-1.0 + 2.0 * cos2σM * cos2σM)));
    double λ2 = (lon + L + 3.0 * Math.PI) % (2.0 * Math.PI) - Math.PI;  // normalise to -180..+180

    return new GeoPoint(this, φ2, λ2);
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
    return Double.hashCode(ab) + Double.hashCode(c);
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


