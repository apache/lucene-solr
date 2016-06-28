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
package org.apache.lucene.spatial3d;

import org.apache.lucene.spatial3d.geom.PlanetModel;
import org.apache.lucene.spatial3d.geom.GeoPolygonFactory;
import org.apache.lucene.spatial3d.geom.GeoPathFactory;
import org.apache.lucene.spatial3d.geom.GeoCircleFactory;
import org.apache.lucene.spatial3d.geom.GeoBBoxFactory;
import org.apache.lucene.spatial3d.geom.GeoPath;
import org.apache.lucene.spatial3d.geom.GeoPolygon;
import org.apache.lucene.spatial3d.geom.GeoCircle;
import org.apache.lucene.spatial3d.geom.GeoBBox;
import org.apache.lucene.spatial3d.geom.GeoCompositePolygon;
import org.apache.lucene.spatial3d.geom.GeoPoint;

import org.apache.lucene.geo.Polygon;
import org.apache.lucene.geo.GeoUtils;

import java.util.List;
import java.util.ArrayList;

class Geo3DUtil {

  /** How many radians are in one earth surface meter */
  final static double RADIANS_PER_METER = 1.0 / PlanetModel.WGS84_MEAN;
  /** How many radians are in one degree */
  final static double RADIANS_PER_DEGREE = Math.PI / 180.0;
  
  private static final double MAX_VALUE = PlanetModel.WGS84.getMaximumMagnitude();
  private static final int BITS = 32;
  private static final double MUL = (0x1L<<BITS)/(2*MAX_VALUE);
  static final double DECODE = getNextSafeDouble(1/MUL);
  static final int MIN_ENCODED_VALUE = encodeValue(-MAX_VALUE);
  static final int MAX_ENCODED_VALUE = encodeValue(MAX_VALUE);

  public static int encodeValue(double x) {
    if (x > MAX_VALUE) {
      throw new IllegalArgumentException("value=" + x + " is out-of-bounds (greater than WGS84's planetMax=" + MAX_VALUE + ")");
    }
    if (x < -MAX_VALUE) {
      throw new IllegalArgumentException("value=" + x + " is out-of-bounds (less than than WGS84's -planetMax=" + -MAX_VALUE + ")");
    }
    long result = (long) Math.floor(x / DECODE);
    assert result >= Integer.MIN_VALUE;
    assert result <= Integer.MAX_VALUE;
    return (int) result;
  }

  public static double decodeValue(int x) {
    double result;
    if (x == MIN_ENCODED_VALUE) {
      // We must special case this, because -MAX_VALUE is not guaranteed to land precisely at a floor value, and we don't ever want to
      // return a value outside of the planet's range (I think?).  The max value is "safe" because we floor during encode:
      result = -MAX_VALUE;
    } else if (x == MAX_ENCODED_VALUE) {
      result = MAX_VALUE;
    } else {
      // We decode to the center value; this keeps the encoding stable
      result = (x+0.5) * DECODE;
    }
    assert result >= -MAX_VALUE && result <= MAX_VALUE;
    return result;
  }

  /** Returns smallest double that would encode to int x. */
  // NOTE: keep this package private!!
  static double decodeValueFloor(int x) {
    return x * DECODE;
  }
  
  /** Returns a double value >= x such that if you multiply that value by an int, and then
   *  divide it by that int again, you get precisely the same value back */
  private static double getNextSafeDouble(double x) {

    // Move to double space:
    long bits = Double.doubleToLongBits(x);

    // Make sure we are beyond the actual maximum value:
    bits += Integer.MAX_VALUE;

    // Clear the bottom 32 bits:
    bits &= ~((long) Integer.MAX_VALUE);

    // Convert back to double:
    double result = Double.longBitsToDouble(bits);
    assert result > x;
    return result;
  }

  /** Returns largest double that would encode to int x. */
  // NOTE: keep this package private!!
  static double decodeValueCeil(int x) {
    assert x < Integer.MAX_VALUE;
    return Math.nextDown((x+1) * DECODE);
  }
  
  /** Converts degress to radians */
  static double fromDegrees(final double degrees) {
    return degrees * RADIANS_PER_DEGREE;
  }
  
  /** Converts earth-surface meters to radians */
  static double fromMeters(final double meters) {
    return meters * RADIANS_PER_METER;
  }

  /**
    * Convert a set of Polygon objects into a GeoPolygon.
    * @param polygons are the Polygon objects.
    * @return the GeoPolygon.
    */
  static GeoPolygon fromPolygon(final Polygon... polygons) {
    //System.err.println("Creating polygon...");
    if (polygons.length < 1) {
      throw new IllegalArgumentException("need at least one polygon");
    }
    final GeoPolygon shape;
    if (polygons.length == 1) {
      final GeoPolygon component = fromPolygon(polygons[0]);
      if (component == null) {
        // Polygon is degenerate
        shape = new GeoCompositePolygon();
      } else {
        shape = component;
      }
    } else {
      final GeoCompositePolygon poly = new GeoCompositePolygon();
      for (final Polygon p : polygons) {
        final GeoPolygon component = fromPolygon(p);
        if (component != null) {
          poly.addShape(component);
        }
      }
      shape = poly;
    }
    return shape;
    //System.err.println("...done");
  }
  
  
  /**
   * Convert a Polygon object to a large GeoPolygon.
   * @param polygons is the list of polygons to convert.
   * @return the large GeoPolygon.
   */
  static GeoPolygon fromLargePolygon(final Polygon... polygons) {
    if (polygons.length < 1) {
      throw new IllegalArgumentException("need at least one polygon");
    }
    return GeoPolygonFactory.makeLargeGeoPolygon(PlanetModel.WGS84, convertToDescription(polygons));
  }
  
  /**
   * Convert input parameters to a path.
   * @param pathLatitudes latitude values for points of the path: must be within standard +/-90 coordinate bounds.
   * @param pathLongitudes longitude values for points of the path: must be within standard +/-180 coordinate bounds.
   * @param pathWidthMeters width of the path in meters.
   * @return the path.
   */
  static GeoPath fromPath(final double[] pathLatitudes, final double[] pathLongitudes, final double pathWidthMeters) {
    if (pathLatitudes.length != pathLongitudes.length) {
      throw new IllegalArgumentException("same number of latitudes and longitudes required");
    }
    final GeoPoint[] points = new GeoPoint[pathLatitudes.length];
    for (int i = 0; i < pathLatitudes.length; i++) {
      final double latitude = pathLatitudes[i];
      final double longitude = pathLongitudes[i];
      GeoUtils.checkLatitude(latitude);
      GeoUtils.checkLongitude(longitude);
      points[i] = new GeoPoint(PlanetModel.WGS84, fromDegrees(latitude), fromDegrees(longitude));
    }
    return GeoPathFactory.makeGeoPath(PlanetModel.WGS84, fromMeters(pathWidthMeters), points);
  }
  
  /**
   * Convert input parameters to a circle.
   * @param latitude latitude at the center: must be within standard +/-90 coordinate bounds.
   * @param longitude longitude at the center: must be within standard +/-180 coordinate bounds.
   * @param radiusMeters maximum distance from the center in meters: must be non-negative and finite.
   * @return the circle.
   */
  static GeoCircle fromDistance(final double latitude, final double longitude, final double radiusMeters) {
    GeoUtils.checkLatitude(latitude);
    GeoUtils.checkLongitude(longitude);
    return GeoCircleFactory.makeGeoCircle(PlanetModel.WGS84, fromDegrees(latitude), fromDegrees(longitude), fromMeters(radiusMeters));
  }
  
  /**
   * Convert input parameters to a box.
   * @param minLatitude latitude lower bound: must be within standard +/-90 coordinate bounds.
   * @param maxLatitude latitude upper bound: must be within standard +/-90 coordinate bounds.
   * @param minLongitude longitude lower bound: must be within standard +/-180 coordinate bounds.
   * @param maxLongitude longitude upper bound: must be within standard +/-180 coordinate bounds.
   * @return the box.
   */
  static GeoBBox fromBox(final double minLatitude, final double maxLatitude, final double minLongitude, final double maxLongitude) {
    GeoUtils.checkLatitude(minLatitude);
    GeoUtils.checkLongitude(minLongitude);
    GeoUtils.checkLatitude(maxLatitude);
    GeoUtils.checkLongitude(maxLongitude);
    return GeoBBoxFactory.makeGeoBBox(PlanetModel.WGS84, 
      Geo3DUtil.fromDegrees(maxLatitude), Geo3DUtil.fromDegrees(minLatitude), Geo3DUtil.fromDegrees(minLongitude), Geo3DUtil.fromDegrees(maxLongitude));
  }

  /**
    * Convert a Polygon object into a GeoPolygon.
    * This method uses
    * @param polygon is the Polygon object.
    * @return the GeoPolygon.
    */
  private static GeoPolygon fromPolygon(final Polygon polygon) {
    // First, assemble the "holes".  The geo3d convention is to use the same polygon sense on the inner ring as the
    // outer ring, so we process these recursively with reverseMe flipped.
    final Polygon[] theHoles = polygon.getHoles();
    final List<GeoPolygon> holeList = new ArrayList<>(theHoles.length);
    for (final Polygon hole : theHoles) {
      //System.out.println("Hole: "+hole);
      final GeoPolygon component = fromPolygon(hole);
      if (component != null) {
        holeList.add(component);
      }
    }
    
    // Now do the polygon itself
    final double[] polyLats = polygon.getPolyLats();
    final double[] polyLons = polygon.getPolyLons();
    
    // I presume the arguments have already been checked
    final List<GeoPoint> points = new ArrayList<>(polyLats.length-1);
    // We skip the last point anyway because the API requires it to be repeated, and geo3d doesn't repeat it.
    for (int i = 0; i < polyLats.length - 1; i++) {
      final int index = polyLats.length - 2 - i;
      points.add(new GeoPoint(PlanetModel.WGS84, fromDegrees(polyLats[index]), fromDegrees(polyLons[index])));
    }
    //System.err.println(" building polygon with "+points.size()+" points...");
    final GeoPolygon rval = GeoPolygonFactory.makeGeoPolygon(PlanetModel.WGS84, points, holeList);
    //System.err.println(" ...done");
    return rval;
  }

  /**
   * Convert a list of polygons to a list of polygon descriptions.
   * @param polygons is the list of polygons to convert.
   * @return the list of polygon descriptions.
   */
  private static List<GeoPolygonFactory.PolygonDescription> convertToDescription(final Polygon... polygons) {
    final List<GeoPolygonFactory.PolygonDescription> descriptions = new ArrayList<>(polygons.length);
    for (final Polygon polygon : polygons) {
      final Polygon[] theHoles = polygon.getHoles();
      final List<GeoPolygonFactory.PolygonDescription> holes = convertToDescription(theHoles);
      
      // Now do the polygon itself
      final double[] polyLats = polygon.getPolyLats();
      final double[] polyLons = polygon.getPolyLons();
      
      // I presume the arguments have already been checked
      final List<GeoPoint> points = new ArrayList<>(polyLats.length-1);
      // We skip the last point anyway because the API requires it to be repeated, and geo3d doesn't repeat it.
      for (int i = 0; i < polyLats.length - 1; i++) {
        final int index = polyLats.length - 2 - i;
        points.add(new GeoPoint(PlanetModel.WGS84, fromDegrees(polyLats[index]), fromDegrees(polyLons[index])));
      }
      
      descriptions.add(new GeoPolygonFactory.PolygonDescription(points, holes));
    }
    return descriptions;
  }
  

}
