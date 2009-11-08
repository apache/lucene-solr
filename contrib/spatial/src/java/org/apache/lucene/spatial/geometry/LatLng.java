/**
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

package org.apache.lucene.spatial.geometry;


/**
 * Abstract base lat-lng class which can manipulate fixed point or floating
 * point based coordinates. Instances are immutable.
 * 
 * @see FloatLatLng
 *
 * <p><font color="red"><b>NOTE:</b> This API is still in
 * flux and might change in incompatible ways in the next
 * release.</font>
 */
public abstract class LatLng {

  public abstract boolean isNormalized();

  public abstract boolean isFixedPoint();

  public abstract LatLng normalize();

  public abstract int getFixedLat();

  public abstract int getFixedLng();

  public abstract double getLat();

  public abstract double getLng();

  public abstract LatLng copy();

  public abstract FixedLatLng toFixed();

  public abstract FloatLatLng toFloat();
  
  /**
   * Convert the lat/lng into the cartesian coordinate plane such that all
   * world coordinates are represented in the first quadrant.
   * The x dimension corresponds to latitude and y corresponds to longitude.
   * The translation starts with the normalized latlng and adds 180 to the latitude and 
   * 90 to the longitude (subject to fixed point scaling).
   */
  public CartesianPoint toCartesian() {
    LatLng ll=normalize();
    
    int lat=ll.getFixedLat();
    int lng=ll.getFixedLng();
    
    return new CartesianPoint(
        lng+180*FixedLatLng.SCALE_FACTOR_INT,
        lat+90*FixedLatLng.SCALE_FACTOR_INT
    );
  }
  
  /**
   * The inverse of toCartesian().  Always returns a FixedLatLng.
   * @param pt
   */
  public static LatLng fromCartesian(CartesianPoint pt) {
    int lat=pt.getY() - 90 * FixedLatLng.SCALE_FACTOR_INT;
    int lng=pt.getX() - 180 * FixedLatLng.SCALE_FACTOR_INT;
    
    return new FixedLatLng(lat, lng);
  }
  
  /**
   * Calculates the distance between two lat/lng's in miles.
   * Imported from mq java client.
   * 
   * @param ll2
   *            Second lat,lng position to calculate distance to.
   * 
   * @return Returns the distance in miles.
   */
  public double arcDistance(LatLng ll2) {
    return arcDistance(ll2, DistanceUnits.MILES);
  }

  /**
   * Calculates the distance between two lat/lng's in miles or meters.
   * Imported from mq java client.  Variable references changed to match.
   * 
   * @param ll2
   *            Second lat,lng position to calculate distance to.
   * @param lUnits
   *            Units to calculate distance, defaults to miles
   * 
   * @return Returns the distance in meters or miles.
   */
  public double arcDistance(LatLng ll2, DistanceUnits lUnits) {
    LatLng ll1 = normalize();
    ll2 = ll2.normalize();

    double lat1 = ll1.getLat(), lng1 = ll1.getLng();
    double lat2 = ll2.getLat(), lng2 = ll2.getLng();

    // Check for same position
    if (lat1 == lat2 && lng1 == lng2)
      return 0.0;

    // Get the m_dLongitude difference. Don't need to worry about
    // crossing 180 since cos(x) = cos(-x)
    double dLon = lng2 - lng1;

    double a = radians(90.0 - lat1);
    double c = radians(90.0 - lat2);
    double cosB = (Math.cos(a) * Math.cos(c))
        + (Math.sin(a) * Math.sin(c) * Math.cos(radians(dLon)));

    double radius = (lUnits == DistanceUnits.MILES) ? 3963.205/* MILERADIUSOFEARTH */
    : 6378.160187/* KMRADIUSOFEARTH */;

    // Find angle subtended (with some bounds checking) in radians and
    // multiply by earth radius to find the arc distance
    if (cosB < -1.0)
      return 3.14159265358979323846/* PI */* radius;
    else if (cosB >= 1.0)
      return 0;
    else
      return Math.acos(cosB) * radius;
  }

  private double radians(double a) {
    return a * 0.01745329251994;
  }

  @Override
  public String toString() {
    return "[" + getLat() + "," + getLng() + "]";
  }

  /**
   * Calculate the midpoint between this point an another.  Respects fixed vs floating point
   * @param other
   */
  public abstract LatLng calculateMidpoint(LatLng other);
  
  @Override
  public abstract int hashCode();

  @Override
  public abstract boolean equals(Object obj);
}
