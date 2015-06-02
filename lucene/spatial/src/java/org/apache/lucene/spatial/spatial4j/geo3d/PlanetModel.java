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
 * Holds mathematical constants associated with the model of a planet.
 * @lucene.experimental
 */
public class PlanetModel {
  
  /** Planet model corresponding to sphere. */
  public static final PlanetModel SPHERE = new PlanetModel(1.0,1.0);

  /** Mean radius */
  public static final double WGS84_MEAN = 6371009.0;
  /** Polar radius */
  public static final double WGS84_POLAR = 6356752.3;
  /** Equatorial radius */
  public static final double WGS84_EQUATORIAL = 6378137.0;
  /** Planet model corresponding to WGS84 */
  public static final PlanetModel WGS84 = new PlanetModel(WGS84_EQUATORIAL/WGS84_MEAN,
    WGS84_POLAR/WGS84_MEAN);

  // Surface of the planet:
  // x^2/a^2 + y^2/b^2 + z^2/c^2 = 1.0
  // Scaling factors are a,b,c.  geo3d can only support models where a==b, so use ab instead.
  public final double ab;
  public final double c;
  public final double inverseAb;
  public final double inverseC;
  public final double inverseAbSquared;
  public final double inverseCSquared;
  // We do NOT include radius, because all computations in geo3d are in radians, not meters.
  
  // Compute north and south pole for planet model, since these are commonly used.
  public final GeoPoint NORTH_POLE;
  public final GeoPoint SOUTH_POLE;
  
  public PlanetModel(final double ab, final double c) {
    this.ab = ab;
    this.c = c;
    this.inverseAb = 1.0 / ab;
    this.inverseC = 1.0 / c;
    this.inverseAbSquared = inverseAb * inverseAb;
    this.inverseCSquared = inverseC * inverseC;
    this.NORTH_POLE = new GeoPoint(c, 0.0, 0.0, 1.0);
    this.SOUTH_POLE = new GeoPoint(c, 0.0, 0.0, -1.0);
  }
  
  /** Find the minimum magnitude of all points on the ellipsoid.
   */
  public double getMinimumMagnitude() {
    return Math.min(this.ab, this.c);
  }

  /** Find the maximum magnitude of all points on the ellipsoid.
   */
  public double getMaximumMagnitude() {
    return Math.max(this.ab, this.c);
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


