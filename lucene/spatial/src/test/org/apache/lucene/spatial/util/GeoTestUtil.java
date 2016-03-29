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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.lucene.util.SloppyMath;

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
  
  /** returns next pseudorandom box: can cross the 180th meridian */
  public static GeoRect nextBox() {
    return nextBoxInternal(nextLatitude(), nextLatitude(), nextLongitude(), nextLongitude(), true);
  }
  
  /** returns next pseudorandom box, can cross the 180th meridian, kinda close to {@code otherLatitude} and {@code otherLongitude} */
  public static GeoRect nextBoxNear(double otherLatitude, double otherLongitude) {
    GeoUtils.checkLongitude(otherLongitude);
    GeoUtils.checkLongitude(otherLongitude);
    return nextBoxInternal(nextLatitudeNear(otherLatitude), nextLatitudeNear(otherLatitude), 
                           nextLongitudeNear(otherLongitude), nextLongitudeNear(otherLongitude), true);
  }
  
  /** returns next pseudorandom polygon */
  public static double[][] nextPolygon() {
    if (random().nextBoolean()) {
      return surpriseMePolygon(null, null);
    }

    GeoRect box = nextBoxInternal(nextLatitude(), nextLatitude(), nextLongitude(), nextLongitude(), false);
    if (random().nextBoolean()) {
      // box
      return boxPolygon(box);
    } else {
      // triangle
      return trianglePolygon(box);
    }
  }
  
  /** returns next pseudorandom polygon, kinda close to {@code otherLatitude} and {@code otherLongitude} */
  public static double[][] nextPolygonNear(double otherLatitude, double otherLongitude) {
    if (random().nextBoolean()) {
      return surpriseMePolygon(otherLatitude, otherLongitude);
    }

    GeoRect box = nextBoxInternal(nextLatitudeNear(otherLatitude), nextLatitudeNear(otherLatitude), 
                                  nextLongitudeNear(otherLongitude), nextLongitudeNear(otherLongitude), false);
    if (random().nextBoolean()) {
      // box
      return boxPolygon(box);
    } else {
      // triangle
      return trianglePolygon(box);
    }
  }

  private static GeoRect nextBoxInternal(double lat0, double lat1, double lon0, double lon1, boolean canCrossDateLine) {
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

    return new GeoRect(lat0, lat1, lon0, lon1);
  }
  
  private static double[][] boxPolygon(GeoRect box) {
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
    return new double[][] { polyLats, polyLons };
  }
  
  private static double[][] trianglePolygon(GeoRect box) {
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
    return new double[][] { polyLats, polyLons };
  }
  
  /** Returns {polyLats, polyLons} double[] array */
  private static double[][] surpriseMePolygon(Double otherLatitude, Double otherLongitude) {
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
        double lat = centerLat + len * Math.cos(Math.toRadians(angle));
        double lon = centerLon + len * Math.sin(Math.toRadians(angle));
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
      return new double[][] {latsArray, lonsArray};
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
  
  // craziness for plotting stuff :)
  
  private static double wrapLat(double lat) {
    //System.out.println("wrapLat " + lat);
    if (lat > 90) {
      //System.out.println("  " + (180 - lat));
      return 180 - lat;
    } else if (lat < -90) {
      //System.out.println("  " + (-180 - lat));
      return -180 - lat;
    } else {
      //System.out.println("  " + lat);
      return lat;
    }
  }

  private static double wrapLon(double lon) {
    //System.out.println("wrapLon " + lon);
    if (lon > 180) {
      //System.out.println("  " + (lon - 360));
      return lon - 360;
    } else if (lon < -180) {
      //System.out.println("  " + (lon + 360));
      return lon + 360;
    } else {
      //System.out.println("  " + lon);
      return lon;
    }
  }
  
  private static void drawRectApproximatelyOnEarthSurface(String name, String color, double minLat, double maxLat, double minLon, double maxLon) {
    int steps = 20;
    System.out.println("        var " + name + " = WE.polygon([");
    System.out.println("          // min -> max lat, min lon");
    for(int i=0;i<steps;i++) {
      System.out.println("          [" + (minLat + (maxLat - minLat) * i / steps) + ", " + minLon + "],");
    }
    System.out.println("          // max lat, min -> max lon");
    for(int i=0;i<steps;i++) {
      System.out.println("          [" + (maxLat + ", " + (minLon + (maxLon - minLon) * i / steps)) + "],");
    }
    System.out.println("          // max -> min lat, max lon");
    for(int i=0;i<steps;i++) {
      System.out.println("          [" + (minLat + (maxLat - minLat) * (steps-i) / steps) + ", " + maxLon + "],");
    }
    System.out.println("          // min lat, max -> min lon");
    for(int i=0;i<steps;i++) {
      System.out.println("          [" + minLat + ", " + (minLon + (maxLon - minLon) * (steps-i) / steps) + "],");
    }
    System.out.println("          // min lat, min lon");
    System.out.println("          [" + minLat + ", " + minLon + "]");
    System.out.println("        ], {color: \"" + color + "\", fillColor: \"" + color + "\"});");
    System.out.println("        " + name + ".addTo(earth);");
  }
  
  private static void plotLatApproximatelyOnEarthSurface(String name, String color, double lat, double minLon, double maxLon) {
    System.out.println("        var " + name + " = WE.polygon([");
    double lon;
    for(lon = minLon;lon<=maxLon;lon += (maxLon-minLon)/36) {
      System.out.println("          [" + lat + ", " + lon + "],");
    }
    System.out.println("          [" + lat + ", " + maxLon + "],");
    lon -= (maxLon-minLon)/36;
    for(;lon>=minLon;lon -= (maxLon-minLon)/36) {
      System.out.println("          [" + lat + ", " + lon + "],");
    }
    System.out.println("        ], {color: \"" + color + "\", fillColor: \"#ffffff\", opacity: " + (color.equals("#ffffff") ? "0.3" : "1") + ", fillOpacity: 0.0001});");
    System.out.println("        " + name + ".addTo(earth);");
  }

  private static void plotLonApproximatelyOnEarthSurface(String name, String color, double lon, double minLat, double maxLat) {
    System.out.println("        var " + name + " = WE.polygon([");
    double lat;
    for(lat = minLat;lat<=maxLat;lat += (maxLat-minLat)/36) {
      System.out.println("          [" + lat + ", " + lon + "],");
    }
    System.out.println("          [" + maxLat + ", " + lon + "],");
    lat -= (maxLat-minLat)/36;
    for(;lat>=minLat;lat -= (maxLat-minLat)/36) {
      System.out.println("          [" + lat + ", " + lon + "],");
    }
    System.out.println("        ], {color: \"" + color + "\", fillColor: \"#ffffff\", opacity: " + (color.equals("#ffffff") ? "0.3" : "1") + ", fillOpacity: 0.0001});");
    System.out.println("        " + name + ".addTo(earth);");
  }

  // http://www.webglearth.org has API details:
  public static void polysToWebGLEarth(List<double[][]> polys) {
    System.out.println("<!DOCTYPE HTML>");
    System.out.println("<html>");
    System.out.println("  <head>");
    System.out.println("    <script src=\"http://www.webglearth.com/v2/api.js\"></script>");
    System.out.println("    <script>");
    System.out.println("      function initialize() {");
    System.out.println("        var earth = new WE.map('earth_div');");

    int count = 0;
    for (double[][] poly : polys) {
      System.out.println("        var poly" + count + " = WE.polygon([");
      for(int i=0;i<poly[0].length;i++) {
        double lat = poly[0][i];
        double lon = poly[1][i];
        System.out.println("          [" + lat + ", " + lon + "],");
      }
      System.out.println("        ], {color: '#00ff00'});");    
      System.out.println("        poly" + count + ".addTo(earth);");
    }

    System.out.println("        WE.tileLayer('http://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png',{");
    System.out.println("          attribution: '© OpenStreetMap contributors'");
    System.out.println("        }).addTo(earth);");
    System.out.println("      }");
    System.out.println("    </script>");
    System.out.println("    <style>");
    System.out.println("      html, body{padding: 0; margin: 0;}");
    System.out.println("      #earth_div{top: 0; right: 0; bottom: 0; left: 0; position: absolute !important;}");
    System.out.println("    </style>");
    System.out.println("    <title>WebGL Earth API: Hello World</title>");
    System.out.println("  </head>");
    System.out.println("  <body onload=\"initialize()\">");
    System.out.println("    <div id=\"earth_div\"></div>");
    System.out.println("  </body>");
    System.out.println("</html>");
  }

  // http://www.webglearth.org has API details:
  public static void toWebGLEarth(double rectMinLatitude, double rectMaxLatitude,
                                   double rectMinLongitude, double rectMaxLongitude,
                                   double centerLatitude, double centerLongitude,
                                   double radiusMeters) {
    GeoRect box = GeoUtils.circleToBBox(centerLatitude, centerLongitude, radiusMeters);
    System.out.println("<!DOCTYPE HTML>");
    System.out.println("<html>");
    System.out.println("  <head>");
    System.out.println("    <script src=\"http://www.webglearth.com/v2/api.js\"></script>");
    System.out.println("    <script>");
    System.out.println("      function initialize() {");
    System.out.println("        var earth = new WE.map('earth_div', {center: [" + centerLatitude + ", " + centerLongitude + "]});");
    System.out.println("        var marker = WE.marker([" + centerLatitude + ", " + centerLongitude + "]).addTo(earth);");
    drawRectApproximatelyOnEarthSurface("cell", "#ff0000", rectMinLatitude, rectMaxLatitude, rectMinLongitude, rectMaxLongitude);
    System.out.println("        var polygonB = WE.polygon([");
    StringBuilder b = new StringBuilder();
    inverseHaversin(b, centerLatitude, centerLongitude, radiusMeters);
    System.out.println(b);
    System.out.println("        ], {color: '#00ff00'});");    
    System.out.println("        polygonB.addTo(earth);");
    drawRectApproximatelyOnEarthSurface("bbox", "#00ff00", box.minLat, box.maxLat, box.minLon, box.maxLon);
    System.out.println("        WE.tileLayer('http://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png',{");
    System.out.println("          attribution: '© OpenStreetMap contributors'");
    System.out.println("        }).addTo(earth);");
    plotLatApproximatelyOnEarthSurface("lat0", "#ffffff", 4.68, 0.0, 360.0);
    plotLatApproximatelyOnEarthSurface("lat1", "#ffffff", 180-93.09, 0.0, 360.0);
    plotLatApproximatelyOnEarthSurface("axisLat", "#00ff00", GeoUtils.axisLat(centerLatitude, radiusMeters), box.minLon, box.maxLon);
    plotLonApproximatelyOnEarthSurface("axisLon", "#00ff00", centerLongitude, box.minLat, box.maxLat);
    System.out.println("      }");
    System.out.println("    </script>");
    System.out.println("    <style>");
    System.out.println("      html, body{padding: 0; margin: 0;}");
    System.out.println("      #earth_div{top: 0; right: 0; bottom: 0; left: 0; position: absolute !important;}");
    System.out.println("    </style>");
    System.out.println("    <title>WebGL Earth API: Hello World</title>");
    System.out.println("  </head>");
    System.out.println("  <body onload=\"initialize()\">");
    System.out.println("    <div id=\"earth_div\"></div>");
    System.out.println("  </body>");
    System.out.println("</html>");
  }

  private static void inverseHaversin(StringBuilder b, double centerLat, double centerLon, double radiusMeters) {
    double angle = 0;
    int steps = 100;

    newAngle:
    while (angle < 360) {
      double x = Math.cos(Math.toRadians(angle));
      double y = Math.sin(Math.toRadians(angle));
      double factor = 2.0;
      double step = 1.0;
      int last = 0;
      double lastDistanceMeters = 0.0;
      //System.out.println("angle " + angle + " slope=" + slope);
      while (true) {
        double lat = wrapLat(centerLat + y * factor);
        double lon = wrapLon(centerLon + x * factor);
        double distanceMeters = SloppyMath.haversinMeters(centerLat, centerLon, lat, lon);

        if (last == 1 && distanceMeters < lastDistanceMeters) {
          // For large enough circles, some angles are not possible:
          //System.out.println("  done: give up on angle " + angle);
          angle += 360./steps;
          continue newAngle;
        }
        if (last == -1 && distanceMeters > lastDistanceMeters) {
          // For large enough circles, some angles are not possible:
          //System.out.println("  done: give up on angle " + angle);
          angle += 360./steps;
          continue newAngle;
        }
        lastDistanceMeters = distanceMeters;

        //System.out.println("  iter lat=" + lat + " lon=" + lon + " distance=" + distanceMeters + " vs " + radiusMeters);
        if (Math.abs(distanceMeters - radiusMeters) < 0.1) {
          b.append("          [" + lat + ", " + lon + "],\n");
          break;
        }
        if (distanceMeters > radiusMeters) {
          // too big
          //System.out.println("    smaller");
          factor -= step;
          if (last == 1) {
            //System.out.println("      half-step");
            step /= 2.0;
          }
          last = -1;
        } else if (distanceMeters < radiusMeters) {
          // too small
          //System.out.println("    bigger");
          factor += step;
          if (last == -1) {
            //System.out.println("      half-step");
            step /= 2.0;
          }
          last = 1;
        }
      }
      angle += 360./steps;
    }
  }
}
