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
 * Holds mathematical constants associated with the model of a planet.
 *
 * @lucene.experimental
 */
public class PlanetModel implements SerializableObject {

  /** Planet model corresponding to sphere. */
  public static final PlanetModel SPHERE = new PlanetModel(1.0, 1.0);

  /** Planet model corresponding to WGS84 ellipsoid */
  // see http://earth-info.nga.mil/GandG/publications/tr8350.2/wgs84fin.pdf
  public static final PlanetModel WGS84 = new PlanetModel(6378137.0d, 6356752.314245d);

  /** Planet model corresponding to Clarke 1866 ellipsoid */
  // see https://georepository.com/ellipsoid_7008/Clarke-1866.html
  public static final PlanetModel CLARKE_1866 = new PlanetModel(6378206.4d, 6356583.8d);

  // Surface of the planet:
  // x^2/a^2 + y^2/b^2 + z^2/zScaling^2 = 1.0
  // Scaling factors are a,b,zScaling.  geo3d can only support models where a==b, so use xyScaling
  // instead.
  /** Semi-major axis */
  public final double a;
  /** Semi-minor axis */
  public final double b;
  /** The x/y scaling factor */
  public final double xyScaling;
  /** The z scaling factor */
  public final double zScaling;
  /** The inverse of xyScaling */
  public final double inverseXYScaling;
  /** The inverse of zScaling */
  public final double inverseZScaling;
  /** The square of the inverse of xyScaling */
  public final double inverseXYScalingSquared;
  /** The square of the inverse of zScaling */
  public final double inverseZScalingSquared;
  /** The scaled flattening value */
  public final double scaledFlattening;
  /** The square ratio */
  public final double squareRatio;
  /** The mean radius of the planet */
  // Computed as (2a + b) / 3 from: "Geodetic Reference System 1980" by H. Moritz
  // ftp://athena.fsv.cvut.cz/ZFG/grs80-Moritz.pdf
  public final double meanRadius;
  /** The scale of the planet */
  public final double scale;
  /** The inverse of scale */
  public final double inverseScale;
  /** The mean radius of the planet model */

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

  // ENCODING / DECODING CONSTANTS
  /** bit space for integer encoding */
  private static final int BITS = 32;
  /** maximum magnitude value for *this* planet model */
  public final double MAX_VALUE;
  /** numeric space (buckets) for mapping double values into integer range */
  private final double MUL;
  /** scalar value used to decode from integer back into double space */
  public final double DECODE;
  /** Max encoded value */
  public final int MAX_ENCODED_VALUE;
  /** Min encoded value */
  public final int MIN_ENCODED_VALUE;
  /** utility class used to encode/decode from lat/lon (decimal degrees) into doc value integers */
  public final DocValueEncoder docValueEncoder;

  /**
   * * Construct a Planet Model from the semi major axis, semi minor axis=.
   *
   * @param semiMajorAxis is the semi major axis (in meters) defined as 'a' in projection formulae.
   * @param semiMinorAxis is the semi minor axis (in meters) defined as 'b' in projection formulae.
   */
  public PlanetModel(final double semiMajorAxis, final double semiMinorAxis) {
    this.a = semiMajorAxis;
    this.b = semiMinorAxis;
    this.meanRadius = (2.0 * semiMajorAxis + semiMinorAxis) / 3.0;
    this.xyScaling = semiMajorAxis / meanRadius;
    this.zScaling = semiMinorAxis / meanRadius;
    this.scale = (2.0 * xyScaling + zScaling) / 3.0;
    this.inverseXYScaling = 1.0 / xyScaling;
    this.inverseZScaling = 1.0 / zScaling;
    this.scaledFlattening = (xyScaling - zScaling) * inverseXYScaling;
    this.squareRatio = (xyScaling * xyScaling - zScaling * zScaling) / (zScaling * zScaling);
    this.inverseXYScalingSquared = inverseXYScaling * inverseXYScaling;
    this.inverseZScalingSquared = inverseZScaling * inverseZScaling;
    this.NORTH_POLE = new GeoPoint(zScaling, 0.0, 0.0, 1.0, Math.PI * 0.5, 0.0);
    this.SOUTH_POLE = new GeoPoint(zScaling, 0.0, 0.0, -1.0, -Math.PI * 0.5, 0.0);
    this.MIN_X_POLE = new GeoPoint(xyScaling, -1.0, 0.0, 0.0, 0.0, -Math.PI);
    this.MAX_X_POLE = new GeoPoint(xyScaling, 1.0, 0.0, 0.0, 0.0, 0.0);
    this.MIN_Y_POLE = new GeoPoint(xyScaling, 0.0, -1.0, 0.0, 0.0, -Math.PI * 0.5);
    this.MAX_Y_POLE = new GeoPoint(xyScaling, 0.0, 1.0, 0.0, 0.0, Math.PI * 0.5);

    this.inverseScale = 1.0 / scale;
    this.minimumPoleDistance =
        Math.min(surfaceDistance(NORTH_POLE, SOUTH_POLE), surfaceDistance(MIN_X_POLE, MAX_X_POLE));

    this.MAX_VALUE = getMaximumMagnitude();
    this.MUL = (0x1L << BITS) / (2 * this.MAX_VALUE);
    this.DECODE = getNextSafeDouble(1 / MUL);
    this.MIN_ENCODED_VALUE = encodeValue(-MAX_VALUE);
    this.MAX_ENCODED_VALUE = encodeValue(MAX_VALUE);

    this.docValueEncoder = new DocValueEncoder(this);
  }

  /**
   * Deserialization constructor.
   *
   * @param inputStream is the input stream.
   */
  public PlanetModel(final InputStream inputStream) throws IOException {
    this(SerializableObject.readDouble(inputStream), SerializableObject.readDouble(inputStream));
  }

  @Override
  public void write(final OutputStream outputStream) throws IOException {
    SerializableObject.writeDouble(outputStream, a);
    SerializableObject.writeDouble(outputStream, b);
  }

  /**
   * Does this planet model describe a sphere?
   *
   * @return true if so.
   */
  public boolean isSphere() {
    return this.xyScaling == this.zScaling;
  }

  /**
   * Find the minimum magnitude of all points on the ellipsoid.
   *
   * @return the minimum magnitude for the planet.
   */
  public double getMinimumMagnitude() {
    return Math.min(this.xyScaling, this.zScaling);
  }

  /**
   * Find the maximum magnitude of all points on the ellipsoid.
   *
   * @return the maximum magnitude for the planet.
   */
  public double getMaximumMagnitude() {
    return Math.max(this.xyScaling, this.zScaling);
  }

  /**
   * Find the minimum x value.
   *
   * @return the minimum X value.
   */
  public double getMinimumXValue() {
    return -this.xyScaling;
  }

  /**
   * Find the maximum x value.
   *
   * @return the maximum X value.
   */
  public double getMaximumXValue() {
    return this.xyScaling;
  }

  /**
   * Find the minimum y value.
   *
   * @return the minimum Y value.
   */
  public double getMinimumYValue() {
    return -this.xyScaling;
  }

  /**
   * Find the maximum y value.
   *
   * @return the maximum Y value.
   */
  public double getMaximumYValue() {
    return this.xyScaling;
  }

  /**
   * Find the minimum z value.
   *
   * @return the minimum Z value.
   */
  public double getMinimumZValue() {
    return -this.zScaling;
  }

  /**
   * Find the maximum z value.
   *
   * @return the maximum Z value.
   */
  public double getMaximumZValue() {
    return this.zScaling;
  }

  /** return the calculated mean radius (in units provided by ab and c) */
  public double getMeanRadius() {
    return this.meanRadius;
  }

  /** encode the provided value from double to integer space */
  public int encodeValue(double x) {
    if (x > getMaximumMagnitude()) {
      throw new IllegalArgumentException(
          "value="
              + x
              + " is out-of-bounds (greater than planetMax="
              + getMaximumMagnitude()
              + ")");
    }
    if (x == getMaximumMagnitude()) {
      x = Math.nextDown(x);
    }
    if (x < -getMaximumMagnitude()) {
      throw new IllegalArgumentException(
          "value="
              + x
              + " is out-of-bounds (less than than -planetMax="
              + -getMaximumMagnitude()
              + ")");
    }
    long result = (long) Math.floor(x / DECODE);
    assert result >= Integer.MIN_VALUE;
    assert result <= Integer.MAX_VALUE;
    return (int) result;
  }

  /** Decodes a given integer back into the radian value according to the defined planet model */
  public double decodeValue(int x) {
    double result;
    if (x == MIN_ENCODED_VALUE) {
      // We must special case this, because -MAX_VALUE is not guaranteed to land precisely at a
      // floor value, and we don't ever want to return a value outside of the planet's range
      // (I think?).  The max value is "safe" because we floor during encode:
      result = -MAX_VALUE;
    } else if (x == MAX_ENCODED_VALUE) {
      result = MAX_VALUE;
    } else {
      // We decode to the center value; this keeps the encoding stable
      result = (x + 0.5) * DECODE;
    }
    assert result >= -MAX_VALUE && result <= MAX_VALUE;
    return result;
  }

  /** return reference to the DocValueEncoder used to encode/decode Geo3DDocValues */
  public DocValueEncoder getDocValueEncoder() {
    return this.docValueEncoder;
  }

  /**
   * Returns a double value >= x such that if you multiply that value by an int, and then divide it
   * by that int again, you get precisely the same value back
   */
  private static double getNextSafeDouble(double x) {

    // Move to double space:
    long bits = Double.doubleToLongBits(x);

    // Make sure we are beyond the actual maximum value:
    bits += Integer.MAX_VALUE;

    // Clear the bottom 32 bits:
    bits &= ~((long) Integer.MAX_VALUE);

    // Convert back to double:
    double result = Double.longBitsToDouble(bits);
    assert result >= x;
    return result;
  }

  /**
   * Check if point is on surface.
   *
   * @param v is the point to check.
   * @return true if the point is on the planet surface.
   */
  public boolean pointOnSurface(final Vector v) {
    return pointOnSurface(v.x, v.y, v.z);
  }

  /**
   * Check if point is on surface.
   *
   * @param x is the x coord.
   * @param y is the y coord.
   * @param z is the z coord.
   */
  public boolean pointOnSurface(final double x, final double y, final double z) {
    // Equation of planet surface is:
    // x^2 / a^2 + y^2 / b^2 + z^2 / zScaling^2 - 1 = 0
    return Math.abs(
            x * x * inverseXYScaling * inverseXYScaling
                + y * y * inverseXYScaling * inverseXYScaling
                + z * z * inverseZScaling * inverseZScaling
                - 1.0)
        < Vector.MINIMUM_RESOLUTION;
  }

  /**
   * Check if point is outside surface.
   *
   * @param v is the point to check.
   * @return true if the point is outside the planet surface.
   */
  public boolean pointOutside(final Vector v) {
    return pointOutside(v.x, v.y, v.z);
  }

  /**
   * Check if point is outside surface.
   *
   * @param x is the x coord.
   * @param y is the y coord.
   * @param z is the z coord.
   */
  public boolean pointOutside(final double x, final double y, final double z) {
    // Equation of planet surface is:
    // x^2 / a^2 + y^2 / b^2 + z^2 / zScaling^2 - 1 = 0
    return (x * x + y * y) * inverseXYScaling * inverseXYScaling
            + z * z * inverseZScaling * inverseZScaling
            - 1.0
        > Vector.MINIMUM_RESOLUTION;
  }

  /**
   * Compute a GeoPoint that's scaled to actually be on the planet surface.
   *
   * @param vector is the vector.
   * @return the scaled point.
   */
  public GeoPoint createSurfacePoint(final Vector vector) {
    return createSurfacePoint(vector.x, vector.y, vector.z);
  }

  /**
   * Compute a GeoPoint that's based on (x,y,z) values, but is scaled to actually be on the planet
   * surface.
   *
   * @param x is the x value.
   * @param y is the y value.
   * @param z is the z value.
   * @return the scaled point.
   */
  public GeoPoint createSurfacePoint(final double x, final double y, final double z) {
    // The equation of the surface is:
    // (x^2 / a^2 + y^2 / b^2 + z^2 / zScaling^2) = 1
    // We will need to scale the passed-in x, y, z values:
    // ((tx)^2 / a^2 + (ty)^2 / b^2 + (tz)^2 / zScaling^2) = 1
    // t^2 * (x^2 / a^2 + y^2 / b^2 + z^2 / zScaling^2)  = 1
    // t = sqrt ( 1 / (x^2 / a^2 + y^2 / b^2 + z^2 / zScaling^2))
    final double t =
        Math.sqrt(
            1.0
                / (x * x * inverseXYScalingSquared
                    + y * y * inverseXYScalingSquared
                    + z * z * inverseZScalingSquared));
    return new GeoPoint(t * x, t * y, t * z);
  }

  /**
   * Compute a GeoPoint that's a bisection between two other GeoPoints.
   *
   * @param pt1 is the first point.
   * @param pt2 is the second point.
   * @return the bisection point, or null if a unique one cannot be found.
   */
  public GeoPoint bisection(final GeoPoint pt1, final GeoPoint pt2) {
    final double A0 = (pt1.x + pt2.x) * 0.5;
    final double B0 = (pt1.y + pt2.y) * 0.5;
    final double C0 = (pt1.z + pt2.z) * 0.5;

    final double denom =
        inverseXYScalingSquared * A0 * A0
            + inverseXYScalingSquared * B0 * B0
            + inverseZScalingSquared * C0 * C0;

    if (denom < Vector.MINIMUM_RESOLUTION) {
      // Bisection is undefined
      return null;
    }

    final double t = Math.sqrt(1.0 / denom);

    return new GeoPoint(t * A0, t * B0, t * C0);
  }

  /**
   * Compute surface distance between two points.
   *
   * @param pt1 is the first point.
   * @param pt2 is the second point.
   * @return the adjusted angle, when multiplied by the mean earth radius, yields a surface
   *     distance. This will differ from GeoPoint.arcDistance() only when the planet model is not a
   *     sphere. @see {@link GeoPoint#arcDistance(Vector)}
   */
  public double surfaceDistance(final GeoPoint pt1, final GeoPoint pt2) {
    final double L = pt2.getLongitude() - pt1.getLongitude();
    final double U1 = Math.atan((1.0 - scaledFlattening) * Math.tan(pt1.getLatitude()));
    final double U2 = Math.atan((1.0 - scaledFlattening) * Math.tan(pt2.getLatitude()));

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
      sinSigma =
          Math.sqrt(
              (cosU2 * sinLambda) * (cosU2 * sinLambda)
                  + (dCosU1SinU2 - dSinU1CosU2 * cosLambda)
                      * (dCosU1SinU2 - dSinU1CosU2 * cosLambda));

      if (sinSigma == 0.0) {
        return 0.0;
      }
      cosSigma = dSinU1SinU2 + dCosU1CosU2 * cosLambda;
      sigma = Math.atan2(sinSigma, cosSigma);
      sinAlpha = dCosU1CosU2 * sinLambda / sinSigma;
      cosSqAlpha = 1.0 - sinAlpha * sinAlpha;
      cos2SigmaM = cosSigma - 2.0 * dSinU1SinU2 / cosSqAlpha;

      if (Double.isNaN(cos2SigmaM)) {
        cos2SigmaM = 0.0; // equatorial line: cosSqAlpha=0
      }
      C =
          scaledFlattening
              / 16.0
              * cosSqAlpha
              * (4.0 + scaledFlattening * (4.0 - 3.0 * cosSqAlpha));
      lambdaP = lambda;
      lambda =
          L
              + (1.0 - C)
                  * scaledFlattening
                  * sinAlpha
                  * (sigma
                      + C
                          * sinSigma
                          * (cos2SigmaM + C * cosSigma * (-1.0 + 2.0 * cos2SigmaM * cos2SigmaM)));
    } while (Math.abs(lambda - lambdaP) >= Vector.MINIMUM_RESOLUTION && ++iterLimit < 100);
    final double uSq = cosSqAlpha * this.squareRatio;
    final double A = 1.0 + uSq / 16384.0 * (4096.0 + uSq * (-768.0 + uSq * (320.0 - 175.0 * uSq)));
    final double B = uSq / 1024.0 * (256.0 + uSq * (-128.0 + uSq * (74.0 - 47.0 * uSq)));
    final double deltaSigma =
        B
            * sinSigma
            * (cos2SigmaM
                + B
                    / 4.0
                    * (cosSigma * (-1.0 + 2.0 * cos2SigmaM * cos2SigmaM)
                        - B
                            / 6.0
                            * cos2SigmaM
                            * (-3.0 + 4.0 * sinSigma * sinSigma)
                            * (-3.0 + 4.0 * cos2SigmaM * cos2SigmaM)));

    return zScaling * inverseScale * A * (sigma - deltaSigma);
  }

  /**
   * Compute new point given original point, a bearing direction, and an adjusted angle (as would be
   * computed by the surfaceDistance() method above). The original point can be anywhere on the
   * globe. The bearing direction ranges from 0 (due east at the equator) to pi/2 (due north) to pi
   * (due west at the equator) to 3 pi/4 (due south) to 2 pi.
   *
   * @param from is the starting point.
   * @param dist is the adjusted angle.
   * @param bearing is the direction to proceed.
   * @return the new point, consistent with the bearing direction and distance.
   */
  public GeoPoint surfacePointOnBearing(
      final GeoPoint from, final double dist, final double bearing) {
    // Algorithm using Vincenty's formulae (https://en.wikipedia.org/wiki/Vincenty%27s_formulae)
    // which takes into account that planets may not be spherical.
    // Code adaptation from http://www.movable-type.co.uk/scripts/latlong-vincenty.html

    double lat = from.getLatitude();
    double lon = from.getLongitude();
    double sinα1 = Math.sin(bearing);
    double cosα1 = Math.cos(bearing);

    double tanU1 = (1.0 - scaledFlattening) * Math.tan(lat);
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

    double σ = dist / (zScaling * inverseScale * A);
    double σʹ;
    double iterations = 0;
    do {
      cos2σM = Math.cos(2.0 * σ1 + σ);
      sinσ = Math.sin(σ);
      cosσ = Math.cos(σ);
      Δσ =
          B
              * sinσ
              * (cos2σM
                  + B
                      / 4.0
                      * (cosσ * (-1.0 + 2.0 * cos2σM * cos2σM)
                          - B
                              / 6.0
                              * cos2σM
                              * (-3.0 + 4.0 * sinσ * sinσ)
                              * (-3.0 + 4.0 * cos2σM * cos2σM)));
      σʹ = σ;
      σ = dist / (zScaling * inverseScale * A) + Δσ;
    } while (Math.abs(σ - σʹ) >= Vector.MINIMUM_RESOLUTION && ++iterations < 100);
    double x = sinU1 * sinσ - cosU1 * cosσ * cosα1;
    double φ2 =
        Math.atan2(
            sinU1 * cosσ + cosU1 * sinσ * cosα1,
            (1.0 - scaledFlattening) * Math.sqrt(sinα * sinα + x * x));
    double λ = Math.atan2(sinσ * sinα1, cosU1 * cosσ - sinU1 * sinσ * cosα1);
    double C = scaledFlattening / 16.0 * cosSqα * (4.0 + scaledFlattening * (4.0 - 3.0 * cosSqα));
    double L =
        λ
            - (1.0 - C)
                * scaledFlattening
                * sinα
                * (σ + C * sinσ * (cos2σM + C * cosσ * (-1.0 + 2.0 * cos2σM * cos2σM)));
    double λ2 = (lon + L + 3.0 * Math.PI) % (2.0 * Math.PI) - Math.PI; // normalise to -180..+180

    return new GeoPoint(this, φ2, λ2);
  }

  /**
   * Utility class for encoding / decoding from lat/lon (decimal degrees) into sortable doc value
   * numerics (integers)
   */
  public static class DocValueEncoder {
    private final PlanetModel planetModel;

    // These are the multiplicative constants we need to use to arrive at values that fit in 21
    // bits. The formula we use to go from double to encoded value is:
    // Math.floor((value - minimum) * factor + 0.5)
    // If we plug in maximum for value, we should get 0x1FFFFF.
    // So, 0x1FFFFF = Math.floor((maximum - minimum) * factor + 0.5)
    // We factor out the 0.5 and Math.floor by stating instead:
    // 0x1FFFFF = (maximum - minimum) * factor
    // So, factor = 0x1FFFFF / (maximum - minimum)

    private static final double inverseMaximumValue = 1.0 / (double) (0x1FFFFF);

    private final double inverseXFactor;
    private final double inverseYFactor;
    private final double inverseZFactor;

    private final double xFactor;
    private final double yFactor;
    private final double zFactor;

    // Fudge factor for step adjustments.  This is here solely to handle inaccuracies in bounding
    // boxes that occur because of quantization.  For unknown reasons, the fudge factor needs to
    // be 10.0 rather than 1.0.  See LUCENE-7430.

    private static final double STEP_FUDGE = 10.0;

    // These values are the delta between a value and the next value in each specific dimension

    private final double xStep;
    private final double yStep;
    private final double zStep;

    /** construct an encoder/decoder instance from the provided PlanetModel definition */
    private DocValueEncoder(final PlanetModel planetModel) {
      this.planetModel = planetModel;

      this.inverseXFactor =
          (planetModel.getMaximumXValue() - planetModel.getMinimumXValue()) * inverseMaximumValue;
      this.inverseYFactor =
          (planetModel.getMaximumYValue() - planetModel.getMinimumYValue()) * inverseMaximumValue;
      this.inverseZFactor =
          (planetModel.getMaximumZValue() - planetModel.getMinimumZValue()) * inverseMaximumValue;

      this.xFactor = 1.0 / inverseXFactor;
      this.yFactor = 1.0 / inverseYFactor;
      this.zFactor = 1.0 / inverseZFactor;

      this.xStep = inverseXFactor * STEP_FUDGE;
      this.yStep = inverseYFactor * STEP_FUDGE;
      this.zStep = inverseZFactor * STEP_FUDGE;
    }

    /**
     * Encode a point.
     *
     * @param point is the point
     * @return the encoded long
     */
    public long encodePoint(final GeoPoint point) {
      return encodePoint(point.x, point.y, point.z);
    }

    /**
     * Encode a point.
     *
     * @param x is the x value
     * @param y is the y value
     * @param z is the z value
     * @return the encoded long
     */
    public long encodePoint(final double x, final double y, final double z) {
      int XEncoded = encodeX(x);
      int YEncoded = encodeY(y);
      int ZEncoded = encodeZ(z);
      return (((long) (XEncoded & 0x1FFFFF)) << 42)
          | (((long) (YEncoded & 0x1FFFFF)) << 21)
          | ((long) (ZEncoded & 0x1FFFFF));
    }

    /**
     * Decode GeoPoint value from long docvalues value.
     *
     * @param docValue is the doc values value.
     * @return the GeoPoint.
     */
    public GeoPoint decodePoint(final long docValue) {
      return new GeoPoint(
          decodeX(((int) (docValue >> 42)) & 0x1FFFFF),
          decodeY(((int) (docValue >> 21)) & 0x1FFFFF),
          decodeZ(((int) (docValue)) & 0x1FFFFF));
    }

    /**
     * Decode X value from long docvalues value.
     *
     * @param docValue is the doc values value.
     * @return the x value.
     */
    public double decodeXValue(final long docValue) {
      return decodeX(((int) (docValue >> 42)) & 0x1FFFFF);
    }

    /**
     * Decode Y value from long docvalues value.
     *
     * @param docValue is the doc values value.
     * @return the y value.
     */
    public double decodeYValue(final long docValue) {
      return decodeY(((int) (docValue >> 21)) & 0x1FFFFF);
    }

    /**
     * Decode Z value from long docvalues value.
     *
     * @param docValue is the doc values value.
     * @return the z value.
     */
    public double decodeZValue(final long docValue) {
      return decodeZ(((int) (docValue)) & 0x1FFFFF);
    }

    /**
     * Round the provided X value down, by encoding it, decrementing it, and unencoding it.
     *
     * @param startValue is the starting value.
     * @return the rounded value.
     */
    public double roundDownX(final double startValue) {
      return startValue - xStep;
    }

    /**
     * Round the provided X value up, by encoding it, incrementing it, and unencoding it.
     *
     * @param startValue is the starting value.
     * @return the rounded value.
     */
    public double roundUpX(final double startValue) {
      return startValue + xStep;
    }

    /**
     * Round the provided Y value down, by encoding it, decrementing it, and unencoding it.
     *
     * @param startValue is the starting value.
     * @return the rounded value.
     */
    public double roundDownY(final double startValue) {
      return startValue - yStep;
    }

    /**
     * Round the provided Y value up, by encoding it, incrementing it, and unencoding it.
     *
     * @param startValue is the starting value.
     * @return the rounded value.
     */
    public double roundUpY(final double startValue) {
      return startValue + yStep;
    }

    /**
     * Round the provided Z value down, by encoding it, decrementing it, and unencoding it.
     *
     * @param startValue is the starting value.
     * @return the rounded value.
     */
    public double roundDownZ(final double startValue) {
      return startValue - zStep;
    }

    /**
     * Round the provided Z value up, by encoding it, incrementing it, and unencoding it.
     *
     * @param startValue is the starting value.
     * @return the rounded value.
     */
    public double roundUpZ(final double startValue) {
      return startValue + zStep;
    }

    // For encoding/decoding, we generally want the following behavior:
    // (1) If you encode the maximum value or the minimum value, the resulting int fits in 21 bits.
    // (2) If you decode an encoded value, you get back the original value for both the minimum and
    // maximum planet model values.
    // (3) Rounding occurs such that a small delta from the minimum and maximum planet model values
    // still returns the same values -- that is, these are in the center of the range of input
    // values that should return the minimum or maximum when decoded

    private int encodeX(final double x) {
      if (x > planetModel.getMaximumXValue()) {
        throw new IllegalArgumentException("x value exceeds planet model maximum");
      } else if (x < planetModel.getMinimumXValue()) {
        throw new IllegalArgumentException("x value less than planet model minimum");
      }
      return (int) Math.floor((x - planetModel.getMinimumXValue()) * xFactor + 0.5);
    }

    private double decodeX(final int x) {
      return x * inverseXFactor + planetModel.getMinimumXValue();
    }

    private int encodeY(final double y) {
      if (y > planetModel.getMaximumYValue()) {
        throw new IllegalArgumentException("y value exceeds planet model maximum");
      } else if (y < planetModel.getMinimumYValue()) {
        throw new IllegalArgumentException("y value less than planet model minimum");
      }
      return (int) Math.floor((y - planetModel.getMinimumYValue()) * yFactor + 0.5);
    }

    private double decodeY(final int y) {
      return y * inverseYFactor + planetModel.getMinimumYValue();
    }

    private int encodeZ(final double z) {
      if (z > planetModel.getMaximumZValue()) {
        throw new IllegalArgumentException("z value exceeds planet model maximum");
      } else if (z < planetModel.getMinimumZValue()) {
        throw new IllegalArgumentException("z value less than planet model minimum");
      }
      return (int) Math.floor((z - planetModel.getMinimumZValue()) * zFactor + 0.5);
    }

    private double decodeZ(final int z) {
      return z * inverseZFactor + planetModel.getMinimumZValue();
    }
  }

  @Override
  public boolean equals(final Object o) {
    if (!(o instanceof PlanetModel)) {
      return false;
    }
    final PlanetModel other = (PlanetModel) o;
    return a == other.a && b == other.b;
  }

  @Override
  public int hashCode() {
    return Double.hashCode(a) + Double.hashCode(b);
  }

  @Override
  public String toString() {
    if (this.equals(SPHERE)) {
      return "PlanetModel.SPHERE";
    } else if (this.equals(WGS84)) {
      return "PlanetModel.WGS84";
    } else if (this.equals(CLARKE_1866)) {
      return "PlanetModel.CLARKE_1866";
    } else {
      return "PlanetModel(xyScaling=" + a + " zScaling=" + b + ")";
    }
  }
}
