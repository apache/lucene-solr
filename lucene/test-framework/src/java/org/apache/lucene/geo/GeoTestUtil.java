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
package org.apache.lucene.geo;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.zip.GZIPInputStream;

import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.SloppyMath;
import org.apache.lucene.util.TestUtil;

import com.carrotsearch.randomizedtesting.RandomizedContext;

/** static methods for testing geo */
public class GeoTestUtil {

  /** returns next pseudorandom latitude (anywhere) */
  public static double nextLatitude() {
    return nextDoubleInternal(-90, 90);
  }

  /** returns next pseudorandom longitude (anywhere) */
  public static double nextLongitude() {
    return nextDoubleInternal(-180, 180);
  }
  
  /**
   * Returns next double within range.
   * <p>
   * Don't pass huge numbers or infinity or anything like that yet. may have bugs!
   */
  // the goal is to adjust random number generation to test edges, create more duplicates, create "one-offs" in floating point space, etc.
  // we do this by first picking a good "base value" (explicitly targeting edges, zero if allowed, or "discrete values"). but it also
  // ensures we pick any double in the range and generally still produces randomish looking numbers.
  // then we sometimes perturb that by one ulp.
  private static double nextDoubleInternal(double low, double high) {
    assert low >= Integer.MIN_VALUE;
    assert high <= Integer.MAX_VALUE;
    assert Double.isFinite(low);
    assert Double.isFinite(high);
    assert high >= low : "low=" + low + " high=" + high;
    
    // if they are equal, not much we can do
    if (low == high) {
      return low;
    }

    // first pick a base value.
    final double baseValue;
    int surpriseMe = random().nextInt(17);
    if (surpriseMe == 0) {
      // random bits
      long lowBits = NumericUtils.doubleToSortableLong(low);
      long highBits = NumericUtils.doubleToSortableLong(high);
      baseValue = NumericUtils.sortableLongToDouble(TestUtil.nextLong(random(), lowBits, highBits));
    } else if (surpriseMe == 1) {
      // edge case
      baseValue = low;
    } else if (surpriseMe == 2) {
      // edge case
      baseValue = high;
    } else if (surpriseMe == 3 && low <= 0 && high >= 0) {
      // may trigger divide by 0
      baseValue = 0.0;
    } else if (surpriseMe == 4) {
      // divide up space into block of 360
      double delta = (high - low) / 360;
      int block = random().nextInt(360);
      baseValue = low + delta * block;
    } else {
      // distributed ~ evenly
      baseValue = low + (high - low) * random().nextDouble();
    }

    assert baseValue >= low;
    assert baseValue <= high;

    // either return the base value or adjust it by 1 ulp in a random direction (if possible)
    int adjustMe = random().nextInt(17);
    if (adjustMe == 0) {
      return Math.nextAfter(adjustMe, high);
    } else if (adjustMe == 1) {
      return Math.nextAfter(adjustMe, low);
    } else {
      return baseValue;
    }
  }

  /** returns next pseudorandom latitude, kinda close to {@code otherLatitude} */
  private static double nextLatitudeNear(double otherLatitude, double delta) {
    delta = Math.abs(delta);
    GeoUtils.checkLatitude(otherLatitude);
    int surpriseMe = random().nextInt(97);
    if (surpriseMe == 0) {
      // purely random
      return nextLatitude();
    } else if (surpriseMe < 49) {
      // upper half of region (the exact point or 1 ulp difference is still likely)
      return nextDoubleInternal(otherLatitude, Math.min(90, otherLatitude + delta));
    } else {
      // lower half of region (the exact point or 1 ulp difference is still likely)
      return nextDoubleInternal(Math.max(-90, otherLatitude - delta), otherLatitude);
    }
  }

  /** returns next pseudorandom longitude, kinda close to {@code otherLongitude} */
  private static double nextLongitudeNear(double otherLongitude, double delta) {
    delta = Math.abs(delta);
    GeoUtils.checkLongitude(otherLongitude);
    int surpriseMe = random().nextInt(97);
    if (surpriseMe == 0) {
      // purely random
      return nextLongitude();
    } else if (surpriseMe < 49) {
      // upper half of region (the exact point or 1 ulp difference is still likely)
      return nextDoubleInternal(otherLongitude, Math.min(180, otherLongitude + delta));
    } else {
      // lower half of region (the exact point or 1 ulp difference is still likely)
      return nextDoubleInternal(Math.max(-180, otherLongitude - delta), otherLongitude);
    }
  }

  /**
   * returns next pseudorandom latitude, kinda close to {@code minLatitude/maxLatitude}
   * <b>NOTE:</b>minLatitude/maxLatitude are merely guidelines. the returned value is sometimes
   * outside of that range! this is to facilitate edge testing of lines
   */
  private static double nextLatitudeBetween(double minLatitude, double maxLatitude) {
    assert maxLatitude >= minLatitude;
    GeoUtils.checkLatitude(minLatitude);
    GeoUtils.checkLatitude(maxLatitude);
    if (random().nextInt(47) == 0) {
      // purely random
      return nextLatitude();
    } else {
      // extend the range by 1%
      double difference = (maxLatitude - minLatitude) / 100;
      double lower = Math.max(-90, minLatitude - difference);
      double upper = Math.min(90, maxLatitude + difference);
      return nextDoubleInternal(lower, upper);
    }
  }

  /**
   * returns next pseudorandom longitude, kinda close to {@code minLongitude/maxLongitude}
   * <b>NOTE:</b>minLongitude/maxLongitude are merely guidelines. the returned value is sometimes
   * outside of that range! this is to facilitate edge testing of lines
   */
  private static double nextLongitudeBetween(double minLongitude, double maxLongitude) {
    assert maxLongitude >= minLongitude;
    GeoUtils.checkLongitude(minLongitude);
    GeoUtils.checkLongitude(maxLongitude);
    if (random().nextInt(47) == 0) {
      // purely random
      return nextLongitude();
    } else {
      // extend the range by 1%
      double difference = (maxLongitude - minLongitude) / 100;
      double lower = Math.max(-180, minLongitude - difference);
      double upper = Math.min(180, maxLongitude + difference);
      return nextDoubleInternal(lower, upper);
    }
  }
  
  /** Returns the next point around a line (more or less) */
  private static double[] nextPointAroundLine(double lat1, double lon1, double lat2, double lon2) {
    double x1 = lon1;
    double x2 = lon2;
    double y1 = lat1;
    double y2 = lat2;
    double minX = Math.min(x1, x2);
    double maxX = Math.max(x1, x2);
    double minY = Math.min(y1, y2);
    double maxY = Math.max(y1, y2);
    if (minX == maxX) {
      return new double[] { nextLatitudeBetween(minY, maxY), nextLongitudeNear(minX, 0.01 * (maxY - minY)) };
    } else if (minY == maxY) {
      return new double[] { nextLatitudeNear(minY, 0.01 * (maxX - minX)), nextLongitudeBetween(minX, maxX) };
    } else {
      double x = nextLongitudeBetween(minX, maxX);
      double y = (y1 - y2) / (x1 - x2) * (x-x1) + y1;
      if (Double.isFinite(y) == false) {
        // this can happen due to underflow when delta between x values is wonderfully tiny!
        y = Math.copySign(90, x1);
      }
      double delta = (maxY - minY) * 0.01;
      // our formula may put the targeted Y out of bounds
      y = Math.min(90, y);
      y = Math.max(-90, y);
      return new double[] { nextLatitudeNear(y, delta), x };
    }
  }
  
  /** Returns next point (lat/lon) for testing near a Box. It may cross the dateline */
  public static double[] nextPointNear(Rectangle rectangle) {
    if (rectangle.crossesDateline()) {
      // pick a "side" of the two boxes we really are
      if (random().nextBoolean()) {
        return nextPointNear(new Rectangle(rectangle.minLat, rectangle.maxLat, -180, rectangle.maxLon));
      } else {
        return nextPointNear(new Rectangle(rectangle.minLat, rectangle.maxLat, rectangle.minLon, 180));
      }
    } else {
      return nextPointNear(boxPolygon(rectangle));
    }
  }

  /** Returns next point (lat/lon) for testing near a Polygon */
  // see http://www-ma2.upc.es/geoc/Schirra-pointPolygon.pdf for more info on some of these strategies
  public static double[] nextPointNear(Polygon polygon) {
    double polyLats[] = polygon.getPolyLats();
    double polyLons[] = polygon.getPolyLons();
    Polygon holes[] = polygon.getHoles();

    // if there are any holes, target them aggressively
    if (holes.length > 0 && random().nextInt(3) == 0) {
      return nextPointNear(holes[random().nextInt(holes.length)]);
    }

    int surpriseMe = random().nextInt(97);
    if (surpriseMe == 0) {
      // purely random
      return new double[] { nextLatitude(), nextLongitude() };
    } else if (surpriseMe < 5) {
      // purely random within bounding box
      return new double[] { nextLatitudeBetween(polygon.minLat, polygon.maxLat), nextLongitudeBetween(polygon.minLon, polygon.maxLon) };
    } else if (surpriseMe < 20) {
      // target a vertex
      int vertex = random().nextInt(polyLats.length - 1);
      return new double[] { nextLatitudeNear(polyLats[vertex], polyLats[vertex+1] - polyLats[vertex]), 
                            nextLongitudeNear(polyLons[vertex], polyLons[vertex+1] - polyLons[vertex]) };
    } else if (surpriseMe < 30) {
      // target points around the bounding box edges
      Polygon container = boxPolygon(new Rectangle(polygon.minLat, polygon.maxLat, polygon.minLon, polygon.maxLon));
      double containerLats[] = container.getPolyLats();
      double containerLons[] = container.getPolyLons();
      int startVertex = random().nextInt(containerLats.length - 1);
      return nextPointAroundLine(containerLats[startVertex], containerLons[startVertex], 
                                 containerLats[startVertex+1], containerLons[startVertex+1]);
    } else {
      // target points around diagonals between vertices
      int startVertex = random().nextInt(polyLats.length - 1);
      // but favor edges heavily
      int endVertex = random().nextBoolean() ? startVertex + 1 : random().nextInt(polyLats.length - 1);
      return nextPointAroundLine(polyLats[startVertex], polyLons[startVertex], 
                                 polyLats[endVertex],   polyLons[endVertex]);
    }
  }
  
  /** Returns next box for testing near a Polygon */
  public static Rectangle nextBoxNear(Polygon polygon) {
    final double point1[];
    final double point2[];
    
    // if there are any holes, target them aggressively
    Polygon holes[] = polygon.getHoles();
    if (holes.length > 0 && random().nextInt(3) == 0) {
      return nextBoxNear(holes[random().nextInt(holes.length)]);
    }
    
    int surpriseMe = random().nextInt(97);
    if (surpriseMe == 0) {
      // formed from two interesting points
      point1 = nextPointNear(polygon);
      point2 = nextPointNear(polygon);
    } else {
      // formed from one interesting point: then random within delta.
      point1 = nextPointNear(polygon);
      point2 = new double[2];
      // now figure out a good delta: we use a rough heuristic, up to the length of an edge
      double polyLats[] = polygon.getPolyLats();
      double polyLons[] = polygon.getPolyLons();
      int vertex = random().nextInt(polyLats.length - 1);
      double deltaX = polyLons[vertex+1] - polyLons[vertex];
      double deltaY = polyLats[vertex+1] - polyLats[vertex];
      double edgeLength = Math.sqrt(deltaX * deltaX + deltaY * deltaY);
      point2[0] = nextLatitudeNear(point1[0], edgeLength);
      point2[1] = nextLongitudeNear(point1[1], edgeLength);
    }
    
    // form a box from the two points
    double minLat = Math.min(point1[0], point2[0]);
    double maxLat = Math.max(point1[0], point2[0]);
    double minLon = Math.min(point1[1], point2[1]);
    double maxLon = Math.max(point1[1], point2[1]);
    return new Rectangle(minLat, maxLat, minLon, maxLon);
  }

  /** returns next pseudorandom box: can cross the 180th meridian */
  public static Rectangle nextBox() {
    return nextBoxInternal(true);
  }

  /** returns next pseudorandom box: does not cross the 180th meridian */
  public static Rectangle nextBoxNotCrossingDateline() {
    return nextBoxInternal( false);
  }

  /** Makes an n-gon, centered at the provided lat/lon, and each vertex approximately
   *  distanceMeters away from the center.
   *
   * Do not invoke me across the dateline or a pole!! */
  public static Polygon createRegularPolygon(double centerLat, double centerLon, double radiusMeters, int gons) {

    // System.out.println("MAKE POLY: centerLat=" + centerLat + " centerLon=" + centerLon + " radiusMeters=" + radiusMeters + " gons=" + gons);

    double[][] result = new double[2][];
    result[0] = new double[gons+1];
    result[1] = new double[gons+1];
    //System.out.println("make gon=" + gons);
    for(int i=0;i<gons;i++) {
      double angle = 360.0-i*(360.0/gons);
      //System.out.println("  angle " + angle);
      double x = Math.cos(SloppyMath.toRadians(angle));
      double y = Math.sin(SloppyMath.toRadians(angle));
      double factor = 2.0;
      double step = 1.0;
      int last = 0;

      //System.out.println("angle " + angle + " slope=" + slope);
      // Iterate out along one spoke until we hone in on the point that's nearly exactly radiusMeters from the center:
      while (true) {

        // TODO: we could in fact cross a pole?  Just do what surpriseMePolygon does?
        double lat = centerLat + y * factor;
        GeoUtils.checkLatitude(lat);
        double lon = centerLon + x * factor;
        GeoUtils.checkLongitude(lon);
        double distanceMeters = SloppyMath.haversinMeters(centerLat, centerLon, lat, lon);

        //System.out.println("  iter lat=" + lat + " lon=" + lon + " distance=" + distanceMeters + " vs " + radiusMeters);
        if (Math.abs(distanceMeters - radiusMeters) < 0.1) {
          // Within 10 cm: close enough!
          result[0][i] = lat;
          result[1][i] = lon;
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
    }

    // close poly
    result[0][gons] = result[0][0];
    result[1][gons] = result[1][0];

    //System.out.println("  polyLats=" + Arrays.toString(result[0]));
    //System.out.println("  polyLons=" + Arrays.toString(result[1]));

    return new Polygon(result[0], result[1]);
  }

  public static Point nextPoint() {
    double lat = nextLatitude();
    double lon = nextLongitude();
    return new Point(lat, lon);
  }

  public static Line nextLine() {
    Polygon p = nextPolygon();
    double[] lats = new double[p.numPoints() - 1];
    double[] lons = new double[lats.length];
    for (int i = 0; i < lats.length; ++i) {
      lats[i] = p.getPolyLat(i);
      lons[i] = p.getPolyLon(i);
    }
    return new Line(lats, lons);
  }
  
  public static Circle nextCircle() {
    double lat = nextLatitude();
    double lon = nextLongitude();
    double radiusMeters = random().nextDouble() * GeoUtils.EARTH_MEAN_RADIUS_METERS * Math.PI / 2.0 + 1.0;
    return new Circle(lat, lon, radiusMeters);
  }

  /** returns next pseudorandom polygon */
  public static Polygon nextPolygon() {
    if (random().nextBoolean()) {
      return surpriseMePolygon();
    } else if (random().nextInt(10) == 1) {
      // this poly is slow to create ... only do it 10% of the time:
      while (true) {
        int gons = TestUtil.nextInt(random(), 4, 500);
        // So the poly can cover at most 50% of the earth's surface:
        double radiusMeters = random().nextDouble() * GeoUtils.EARTH_MEAN_RADIUS_METERS * Math.PI / 2.0 + 1.0;
        try {
          return createRegularPolygon(nextLatitude(), nextLongitude(), radiusMeters, gons);
        } catch (IllegalArgumentException iae) {
          // we tried to cross dateline or pole ... try again
        }
      }
    }

    Rectangle box = nextBoxInternal(false);
    if (random().nextBoolean()) {
      // box
      return boxPolygon(box);
    } else {
      // triangle
      return trianglePolygon(box);
    }
  }

  private static Rectangle nextBoxInternal(boolean canCrossDateLine) {
    // prevent lines instead of boxes
    double lat0 = nextLatitude();
    double lat1 = nextLatitude();
    while (lat0 == lat1) {
      lat1 = nextLatitude();
    }
    // prevent lines instead of boxes
    double lon0 = nextLongitude();
    double lon1 = nextLongitude();
    while (lon0 == lon1) {
      lon1 = nextLongitude();
    }

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

  private static Polygon surpriseMePolygon() {
    // repeat until we get a poly that doesn't cross dateline:
    newPoly:
    while (true) {
      //System.out.println("\nPOLY ITER");
      double centerLat = nextLatitude();
      double centerLon = nextLongitude();
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
        if (lon <= GeoUtils.MIN_LON_INCL || lon >= GeoUtils.MAX_LON_INCL ||
            lat > 90 || lat < -90) {
          // cannot cross dateline or pole: try again!
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

  /** Keep it simple, we don't need to take arbitrary Random for geo tests */
  private static Random random() {
   return RandomizedContext.current().getRandom();
  }

  /** 
   * Returns svg of polygon for debugging. 
   * <p>
   * You can pass any number of objects:
   * Polygon: polygon with optional holes
   * Polygon[]: arrays of polygons for convenience
   * Rectangle: for a box
   * double[2]: as latitude,longitude for a point
   * <p>
   * At least one object must be a polygon. The viewBox is formed around all polygons
   * found in the arguments.
   */
  public static String toSVG(Object ...objects) {
    List<Object> flattened = new ArrayList<>();
    for (Object o : objects) {
      if (o instanceof Polygon[]) {
        flattened.addAll(Arrays.asList((Polygon[]) o));
      } else {
        flattened.add(o);
      }
    }
    // first compute bounding area of all the objects
    double minLat = Double.POSITIVE_INFINITY;
    double maxLat = Double.NEGATIVE_INFINITY;
    double minLon = Double.POSITIVE_INFINITY;
    double maxLon = Double.NEGATIVE_INFINITY;
    for (Object o : flattened) {
      final Rectangle r;
      if (o instanceof Polygon) {
        r = Rectangle.fromPolygon(new Polygon[] { (Polygon) o });
        minLat = Math.min(minLat, r.minLat);
        maxLat = Math.max(maxLat, r.maxLat);
        minLon = Math.min(minLon, r.minLon);
        maxLon = Math.max(maxLon, r.maxLon);
      }
    }
    if (Double.isFinite(minLat) == false || Double.isFinite(maxLat) == false ||
        Double.isFinite(minLon) == false || Double.isFinite(maxLon) == false) {
      throw new IllegalArgumentException("you must pass at least one polygon");
    }
    
    // add some additional padding so we can really see what happens on the edges too
    double xpadding = (maxLon - minLon) / 64;
    double ypadding = (maxLat - minLat) / 64;
    // expand points to be this large
    double pointX = xpadding * 0.1;
    double pointY = ypadding * 0.1;
    StringBuilder sb = new StringBuilder();
    sb.append("<svg xmlns=\"http://www.w3.org/2000/svg\" height=\"640\" width=\"480\" viewBox=\"");
    sb.append(minLon - xpadding)
      .append(" ")
      .append(90 - maxLat - ypadding)
      .append(" ")
      .append(maxLon - minLon + (2*xpadding))
      .append(" ")
      .append(maxLat - minLat + (2*ypadding));
    sb.append("\">\n");

    // encode each object
    for (Object o : flattened) {
      // tostring
      if (o instanceof double[]) {
        double point[] = (double[]) o;
        sb.append("<!-- point: ");
        sb.append(point[0]).append(',').append(point[1]);
        sb.append(" -->\n");
      } else {
        sb.append("<!-- ").append(o.getClass().getSimpleName()).append(": \n");
        sb.append(o.toString());
        sb.append("\n-->\n");
      }
      final Polygon gon;
      final String style;
      final String opacity;
      if (o instanceof Rectangle) {
        gon = boxPolygon((Rectangle) o);
        style = "fill:lightskyblue;stroke:black;stroke-width:0.2%;stroke-dasharray:0.5%,1%;";
        opacity = "0.3";
      } else if (o instanceof double[]) {
        double point[] = (double[]) o;
        gon = boxPolygon(new Rectangle(Math.max(-90, point[0]-pointY), 
                                      Math.min(90, point[0]+pointY), 
                                      Math.max(-180, point[1]-pointX), 
                                      Math.min(180, point[1]+pointX)));
        style = "fill:red;stroke:red;stroke-width:0.1%;";
        opacity = "0.7";
      } else {
        gon = (Polygon) o;
        style = "fill:lawngreen;stroke:black;stroke-width:0.3%;";
        opacity = "0.5";
      }
      // polygon
      double polyLats[] = gon.getPolyLats();
      double polyLons[] = gon.getPolyLons();
      sb.append("<polygon fill-opacity=\"").append(opacity).append("\" points=\"");
      for (int i = 0; i < polyLats.length; i++) {
        if (i > 0) {
          sb.append(" ");
        }
        sb.append(polyLons[i])
        .append(",")
        .append(90 - polyLats[i]);
      }
      sb.append("\" style=\"").append(style).append("\"/>\n");
      for (Polygon hole : gon.getHoles()) {
        double holeLats[] = hole.getPolyLats();
        double holeLons[] = hole.getPolyLons();
        sb.append("<polygon points=\"");
        for (int i = 0; i < holeLats.length; i++) {
          if (i > 0) {
            sb.append(" ");
          }
          sb.append(holeLons[i])
          .append(",")
          .append(90 - holeLats[i]);
        }
        sb.append("\" style=\"fill:lightgray\"/>\n");
      }
    }
    sb.append("</svg>\n");
    return sb.toString();
  }

  /**
   * Simple slow point in polygon check (for testing)
   */
  // direct port of PNPOLY C code (https://www.ecse.rpi.edu/~wrf/Research/Short_Notes/pnpoly.html)
  // this allows us to improve the code yet still ensure we have its properties
  // it is under the BSD license (https://www.ecse.rpi.edu/~wrf/Research/Short_Notes/pnpoly.html#License%20to%20Use)
  //
  // Copyright (c) 1970-2003, Wm. Randolph Franklin
  //
  // Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated 
  // documentation files (the "Software"), to deal in the Software without restriction, including without limitation 
  // the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and 
  // to permit persons to whom the Software is furnished to do so, subject to the following conditions:
  //
  // 1. Redistributions of source code must retain the above copyright 
  //    notice, this list of conditions and the following disclaimers.
  // 2. Redistributions in binary form must reproduce the above copyright 
  //    notice in the documentation and/or other materials provided with 
  //    the distribution.
  // 3. The name of W. Randolph Franklin may not be used to endorse or 
  //    promote products derived from this Software without specific 
  //    prior written permission. 
  //
  // THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED 
  // TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL 
  // THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF 
  // CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS 
  // IN THE SOFTWARE. 
  public static boolean containsSlowly(Polygon polygon, double latitude, double longitude) {
    if (polygon.getHoles().length > 0) {
      throw new UnsupportedOperationException("this testing method does not support holes");
    }
    double polyLats[] = polygon.getPolyLats();
    double polyLons[] = polygon.getPolyLons();
    // bounding box check required due to rounding errors (we don't solve that problem)
    if (latitude < polygon.minLat || latitude > polygon.maxLat || longitude < polygon.minLon || longitude > polygon.maxLon) {
      return false;
    }
    
    boolean c = false;
    int i, j;
    int nvert = polyLats.length;
    double verty[] = polyLats;
    double vertx[] = polyLons;
    double testy = latitude;
    double testx = longitude;
    for (i = 0, j = 1; j < nvert; ++i, ++j) {
      if (testy == verty[j] && testy == verty[i] ||
          ((testy <= verty[j] && testy >= verty[i]) != (testy >= verty[j] && testy <= verty[i]))) {
        if ((testx == vertx[j] && testx == vertx[i]) ||
            ((testx <= vertx[j] && testx >= vertx[i]) != (testx >= vertx[j] && testx <= vertx[i]) &&
            GeoUtils.orient(vertx[i], verty[i], vertx[j], verty[j], testx, testy) == 0)) {
          // return true if point is on boundary
          return true;
        } else if ( ((verty[i] > testy) != (verty[j] > testy)) &&
            (testx < (vertx[j]-vertx[i]) * (testy-verty[i]) / (verty[j]-verty[i]) + vertx[i]) ) {
          c = !c;
        }
      }
    }
    return c;
  }

  /** reads a shape from file */
  public static String readShape(String name) throws IOException {
    return Loader.LOADER.readShape(name);
  }

  private static class Loader {

    static Loader LOADER = new Loader();

    String readShape(String name) throws IOException {
      InputStream is = getClass().getResourceAsStream(name);
      if (is == null) {
        throw new FileNotFoundException("classpath resource not found: " + name);
      }
      if (name.endsWith(".gz")) {
        is = new GZIPInputStream(is);
      }
      BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
      return reader.readLine();
    }
  }
}
