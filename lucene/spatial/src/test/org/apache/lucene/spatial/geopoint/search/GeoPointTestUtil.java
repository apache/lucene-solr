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
package org.apache.lucene.spatial.geopoint.search;

import java.util.ArrayList;
import java.util.Random;

import org.apache.lucene.geo.GeoUtils;
import org.apache.lucene.geo.Polygon;
import org.apache.lucene.geo.Rectangle;
import org.apache.lucene.util.SloppyMath;

import com.carrotsearch.randomizedtesting.RandomizedContext;

// HACK: this is a wimpier version of GeoTestUtil used for now until we can
// get all tests passing with new random number generator!

final class GeoPointTestUtil {

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

  /** returns next pseudorandom box: can cross the 180th meridian */
  public static Rectangle nextBox() {
    return nextBoxInternal(nextLatitude(), nextLatitude(), nextLongitude(), nextLongitude(), true);
  }
  
  /** returns next pseudorandom box: will not cross the 180th meridian */
  public static Rectangle nextSimpleBox() {
    return nextBoxInternal(nextLatitude(), nextLatitude(), nextLongitude(), nextLongitude(), false);
  }

  /** returns next pseudorandom box, can cross the 180th meridian, kinda close to {@code otherLatitude} and {@code otherLongitude} */
  public static Rectangle nextBoxNear(double otherLatitude, double otherLongitude) {
    GeoUtils.checkLongitude(otherLongitude);
    GeoUtils.checkLongitude(otherLongitude);
    return nextBoxInternal(nextLatitudeNear(otherLatitude), nextLatitudeNear(otherLatitude),
                           nextLongitudeNear(otherLongitude), nextLongitudeNear(otherLongitude), true);
  }
  
  /** returns next pseudorandom box, will not cross the 180th meridian, kinda close to {@code otherLatitude} and {@code otherLongitude} */
  public static Rectangle nextSimpleBoxNear(double otherLatitude, double otherLongitude) {
    GeoUtils.checkLongitude(otherLongitude);
    GeoUtils.checkLongitude(otherLongitude);
    return nextBoxInternal(nextLatitudeNear(otherLatitude), nextLatitudeNear(otherLatitude),
                           nextLongitudeNear(otherLongitude), nextLongitudeNear(otherLongitude), false);
  }

  /** returns next pseudorandom polygon */
  public static Polygon nextPolygon() {
    if (random().nextBoolean()) {
      return surpriseMePolygon(null, null);
    }

    Rectangle box = nextBoxInternal(nextLatitude(), nextLatitude(), nextLongitude(), nextLongitude(), false);
    if (random().nextBoolean()) {
      // box
      return boxPolygon(box);
    } else {
      // triangle
      return trianglePolygon(box);
    }
  }

  /** returns next pseudorandom polygon, kinda close to {@code otherLatitude} and {@code otherLongitude} */
  public static Polygon nextPolygonNear(double otherLatitude, double otherLongitude) {
    if (random().nextBoolean()) {
      return surpriseMePolygon(otherLatitude, otherLongitude);
    }

    Rectangle box = nextBoxInternal(nextLatitudeNear(otherLatitude), nextLatitudeNear(otherLatitude),
                                  nextLongitudeNear(otherLongitude), nextLongitudeNear(otherLongitude), false);
    if (random().nextBoolean()) {
      // box
      return boxPolygon(box);
    } else {
      // triangle
      return trianglePolygon(box);
    }
  }

  private static Rectangle nextBoxInternal(double lat0, double lat1, double lon0, double lon1, boolean canCrossDateLine) {
    if (lat1 < lat0) {
      double x = lat0;
      lat0 = lat1;
      lat1 = x;
    }

    if (canCrossDateLine == false && lon1 < lon0) {
      double x = lon0;
      lon0 = lon1;
      lon1 = x;
    }

    return new Rectangle(lat0, lat1, lon0, lon1);
  }

  private static Polygon boxPolygon(Rectangle box) {
    assert box.crossesDateline() == false;
    final double[] polyLats = new double[5];
    final double[] polyLons = new double[5];
    polyLats[0] = box.minLat;
    polyLons[0] = box.minLon;
    polyLats[1] = box.maxLat;
    polyLons[1] = box.minLon;
    polyLats[2] = box.maxLat;
    polyLons[2] = box.maxLon;
    polyLats[3] = box.minLat;
    polyLons[3] = box.maxLon;
    polyLats[4] = box.minLat;
    polyLons[4] = box.minLon;
    return new Polygon(polyLats, polyLons);
  }

  private static Polygon trianglePolygon(Rectangle box) {
    assert box.crossesDateline() == false;
    final double[] polyLats = new double[4];
    final double[] polyLons = new double[4];
    polyLats[0] = box.minLat;
    polyLons[0] = box.minLon;
    polyLats[1] = box.maxLat;
    polyLons[1] = box.minLon;
    polyLats[2] = box.maxLat;
    polyLons[2] = box.maxLon;
    polyLats[3] = box.minLat;
    polyLons[3] = box.minLon;
    return new Polygon(polyLats, polyLons);
  }

  private static Polygon surpriseMePolygon(Double otherLatitude, Double otherLongitude) {
    // repeat until we get a poly that doesn't cross dateline:
    newPoly:
    while (true) {
      //System.out.println("\nPOLY ITER");
      final double centerLat;
      final double centerLon;
      if (otherLatitude == null) {
        centerLat = nextLatitude();
        centerLon = nextLongitude();
      } else {
        GeoUtils.checkLatitude(otherLatitude);
        GeoUtils.checkLongitude(otherLongitude);
        centerLat = nextLatitudeNear(otherLatitude);
        centerLon = nextLongitudeNear(otherLongitude);
      }

      double radius = 0.1 + 20 * random().nextDouble();
      double radiusDelta = random().nextDouble();

      ArrayList<Double> lats = new ArrayList<>();
      ArrayList<Double> lons = new ArrayList<>();
      double angle = 0.0;
      while (true) {
        angle += random().nextDouble()*40.0;
        //System.out.println("  angle " + angle);
        if (angle > 360) {
          break;
        }
        double len = radius * (1.0 - radiusDelta + radiusDelta * random().nextDouble());
        //System.out.println("    len=" + len);
        double lat = centerLat + len * Math.cos(SloppyMath.toRadians(angle));
        double lon = centerLon + len * Math.sin(SloppyMath.toRadians(angle));
        if (lon <= GeoUtils.MIN_LON_INCL || lon >= GeoUtils.MAX_LON_INCL) {
          // cannot cross dateline: try again!
          continue newPoly;
        }
        if (lat > 90) {
          // cross the north pole
          lat = 180 - lat;
          lon = 180 - lon;
        } else if (lat < -90) {
          // cross the south pole
          lat = -180 - lat;
          lon = 180 - lon;
        }
        if (lon <= GeoUtils.MIN_LON_INCL || lon >= GeoUtils.MAX_LON_INCL) {
          // cannot cross dateline: try again!
          continue newPoly;
        }
        lats.add(lat);
        lons.add(lon);

        //System.out.println("    lat=" + lats.get(lats.size()-1) + " lon=" + lons.get(lons.size()-1));
      }

      // close it
      lats.add(lats.get(0));
      lons.add(lons.get(0));

      double[] latsArray = new double[lats.size()];
      double[] lonsArray = new double[lons.size()];
      for(int i=0;i<lats.size();i++) {
        latsArray[i] = lats.get(i);
        lonsArray[i] = lons.get(i);
      }
      return new Polygon(latsArray, lonsArray);
    }
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
