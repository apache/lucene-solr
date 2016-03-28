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
package org.apache.lucene.spatial.util;

import java.util.Random;

import com.carrotsearch.randomizedtesting.RandomizedContext;

/** static methods for testing geo */
public class GeoTestUtil {

  /** returns next pseudorandom latitude (anywhere) */
  public static double nextLatitude() {
    return -90 + 180.0 * random().nextDouble();
  }

  
  /** returns next pseudorandom longitude (anywhere) */
  public static double nextLongitude() {
    return -180 + 360.0 * random().nextDouble();
  }
  
  /** returns next pseudorandom latitude, kinda close to {@code otherLatitude} */
  public static double nextLatitudeNear(double otherLatitude) {
    GeoUtils.checkLatitude(otherLatitude);
    return normalizeLatitude(otherLatitude + random().nextDouble() - 0.5);
  }
  
  /** returns next pseudorandom longitude, kinda close to {@code otherLongitude} */
  public static double nextLongitudeNear(double otherLongitude) {
    GeoUtils.checkLongitude(otherLongitude);
    return normalizeLongitude(otherLongitude + random().nextDouble() - 0.5);
  }
  
  /** 
   * returns next pseudorandom latitude, kinda close to {@code minLatitude/maxLatitude}
   * <b>NOTE:</b>minLatitude/maxLatitude are merely guidelines. the returned value is sometimes
   * outside of that range! this is to facilitate edge testing.
   */
  public static double nextLatitudeAround(double minLatitude, double maxLatitude) {
    GeoUtils.checkLatitude(minLatitude);
    GeoUtils.checkLatitude(maxLatitude);
    return normalizeLatitude(randomRangeMaybeSlightlyOutside(minLatitude, maxLatitude));
  }
  
  /** 
   * returns next pseudorandom longitude, kinda close to {@code minLongitude/maxLongitude}
   * <b>NOTE:</b>minLongitude/maxLongitude are merely guidelines. the returned value is sometimes
   * outside of that range! this is to facilitate edge testing.
   */
  public static double nextLongitudeAround(double minLongitude, double maxLongitude) {
    GeoUtils.checkLongitude(minLongitude);
    GeoUtils.checkLongitude(maxLongitude);
    return normalizeLongitude(randomRangeMaybeSlightlyOutside(minLongitude, maxLongitude));
  }
  
  /** Returns random double min to max or up to 1% outside of that range */
  private static double randomRangeMaybeSlightlyOutside(double min, double max) {
    return min + (random().nextDouble() + (0.5 - random().nextDouble()) * .02) * (max - min);
  }

  /** Puts latitude in range of -90 to 90. */
  private static double normalizeLatitude(double latitude) {
    if (latitude >= -90 && latitude <= 90) {
      return latitude; //common case, and avoids slight double precision shifting
    }
    double off = Math.abs((latitude + 90) % 360);
    return (off <= 180 ? off : 360-off) - 90;
  }
  
  /** Puts longitude in range of -180 to +180. */
  private static double normalizeLongitude(double longitude) {
    if (longitude >= -180 && longitude <= 180) {
      return longitude; //common case, and avoids slight double precision shifting
    }
    double off = (longitude + 180) % 360;
    if (off < 0) {
      return 180 + off;
    } else if (off == 0 && longitude > 0) {
      return 180;
    } else {
      return -180 + off;
    }
  }
  
  /** Keep it simple, we don't need to take arbitrary Random for geo tests */
  private static Random random() {
   return RandomizedContext.current().getRandom();
  }
}
