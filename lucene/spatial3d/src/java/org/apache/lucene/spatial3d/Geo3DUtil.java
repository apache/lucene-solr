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

  /** How many radians are in one degree */
  final static double RADIANS_PER_DEGREE = Math.PI / 180.0;

  /** Returns smallest double that would encode to int x. */
  // NOTE: remains in this class to keep method package private!!
  static double decodeValueFloor(int x, PlanetModel planetModel) {
    assert x <= planetModel.MAX_ENCODED_VALUE && x >= planetModel.MIN_ENCODED_VALUE;
    if (x == planetModel.MIN_ENCODED_VALUE) {
      return -planetModel.MAX_VALUE;
    }
    return x * planetModel.DECODE;
  }

  /** Returns largest double that would encode to int x. */
  // NOTE: keep this package private!!
  static double decodeValueCeil(int x, PlanetModel planetModel) {
    assert x <= planetModel.MAX_ENCODED_VALUE && x >= planetModel.MIN_ENCODED_VALUE;
    if (x == planetModel.MAX_ENCODED_VALUE) {
      return planetModel.MAX_VALUE;
    }
    return Math.nextDown((x+1) * planetModel.DECODE);
  }
  
  /** Converts degress to radians */
  static double fromDegrees(final double degrees) {
    return degrees * RADIANS_PER_DEGREE;
  }

  /**
    * Convert a set of Polygon objects into a GeoPolygon.
    * @param polygons are the Polygon objects.
    * @return the GeoPolygon.
    */
  static GeoPolygon fromPolygon(final PlanetModel planetModel, final Polygon... polygons) {
    //System.err.println("Creating polygon...");
    if (polygons.length < 1) {
      throw new IllegalArgumentException("need at least one polygon");
    }
    final GeoPolygon shape;
    if (polygons.length == 1) {
      final GeoPolygon component = fromPolygon(planetModel, polygons[0]);
      if (component == null) {
        // Polygon is degenerate
        shape = new GeoCompositePolygon(planetModel);
      } else {
        shape = component;
      }
    } else {
      final GeoCompositePolygon poly = new GeoCompositePolygon(planetModel);
      for (final Polygon p : polygons) {
        final GeoPolygon component = fromPolygon(planetModel, p);
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
  static GeoPolygon fromLargePolygon(final PlanetModel planetModel, final Polygon... polygons) {
    if (polygons.length < 1) {
      throw new IllegalArgumentException("need at least one polygon");
    }
    return GeoPolygonFactory.makeLargeGeoPolygon(planetModel, convertToDescription(planetModel, polygons));
  }
  
  /**
   * Convert input parameters to a path.
   * @param pathLatitudes latitude values for points of the path: must be within standard +/-90 coordinate bounds.
   * @param pathLongitudes longitude values for points of the path: must be within standard +/-180 coordinate bounds.
   * @param pathWidthMeters width of the path in meters.
   * @return the path.
   */
  static GeoPath fromPath(final PlanetModel planetModel, final double[] pathLatitudes, final double[] pathLongitudes, final double pathWidthMeters) {
    if (pathLatitudes.length != pathLongitudes.length) {
      throw new IllegalArgumentException("same number of latitudes and longitudes required");
    }
    final GeoPoint[] points = new GeoPoint[pathLatitudes.length];
    for (int i = 0; i < pathLatitudes.length; i++) {
      final double latitude = pathLatitudes[i];
      final double longitude = pathLongitudes[i];
      GeoUtils.checkLatitude(latitude);
      GeoUtils.checkLongitude(longitude);
      points[i] = new GeoPoint(planetModel, fromDegrees(latitude), fromDegrees(longitude));
    }
    double radiusRadians = pathWidthMeters / (planetModel.getMeanRadius() * planetModel.xyScaling);
    return GeoPathFactory.makeGeoPath(planetModel, radiusRadians, points);
  }
  
  /**
   * Convert input parameters to a circle.
   * @param latitude latitude at the center: must be within standard +/-90 coordinate bounds.
   * @param longitude longitude at the center: must be within standard +/-180 coordinate bounds.
   * @param radiusMeters maximum distance from the center in meters: must be non-negative and finite.
   * @return the circle.
   */
  static GeoCircle fromDistance(final PlanetModel planetModel, final double latitude, final double longitude, final double radiusMeters) {
    GeoUtils.checkLatitude(latitude);
    GeoUtils.checkLongitude(longitude);
    double radiusRadians = radiusMeters / (planetModel.getMeanRadius());
    return GeoCircleFactory.makeGeoCircle(planetModel, fromDegrees(latitude), fromDegrees(longitude), radiusRadians);
  }
  
  /**
   * Convert input parameters to a box.
   * @param minLatitude latitude lower bound: must be within standard +/-90 coordinate bounds.
   * @param maxLatitude latitude upper bound: must be within standard +/-90 coordinate bounds.
   * @param minLongitude longitude lower bound: must be within standard +/-180 coordinate bounds.
   * @param maxLongitude longitude upper bound: must be within standard +/-180 coordinate bounds.
   * @return the box.
   */
  static GeoBBox fromBox(final PlanetModel planetModel, final double minLatitude, final double maxLatitude, final double minLongitude, final double maxLongitude) {
    GeoUtils.checkLatitude(minLatitude);
    GeoUtils.checkLongitude(minLongitude);
    GeoUtils.checkLatitude(maxLatitude);
    GeoUtils.checkLongitude(maxLongitude);
    return GeoBBoxFactory.makeGeoBBox(planetModel,
      Geo3DUtil.fromDegrees(maxLatitude), Geo3DUtil.fromDegrees(minLatitude), Geo3DUtil.fromDegrees(minLongitude), Geo3DUtil.fromDegrees(maxLongitude));
  }

  /**
    * Convert a Polygon object into a GeoPolygon.
    * This method uses
    * @param polygon is the Polygon object.
    * @return the GeoPolygon.
    */
  private static GeoPolygon fromPolygon(final PlanetModel planetModel, final Polygon polygon) {
    // First, assemble the "holes".  The geo3d convention is to use the same polygon sense on the inner ring as the
    // outer ring, so we process these recursively with reverseMe flipped.
    final Polygon[] theHoles = polygon.getHoles();
    final List<GeoPolygon> holeList = new ArrayList<>(theHoles.length);
    for (final Polygon hole : theHoles) {
      //System.out.println("Hole: "+hole);
      final GeoPolygon component = fromPolygon(planetModel, hole);
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
      points.add(new GeoPoint(planetModel, fromDegrees(polyLats[index]), fromDegrees(polyLons[index])));
    }
    //System.err.println(" building polygon with "+points.size()+" points...");
    final GeoPolygon rval = GeoPolygonFactory.makeGeoPolygon(planetModel, points, holeList);
    //System.err.println(" ...done");
    return rval;
  }

  /**
   * Convert a list of polygons to a list of polygon descriptions.
   * @param polygons is the list of polygons to convert.
   * @return the list of polygon descriptions.
   */
  private static List<GeoPolygonFactory.PolygonDescription> convertToDescription(final PlanetModel planetModel, final Polygon... polygons) {
    final List<GeoPolygonFactory.PolygonDescription> descriptions = new ArrayList<>(polygons.length);
    for (final Polygon polygon : polygons) {
      final Polygon[] theHoles = polygon.getHoles();
      final List<GeoPolygonFactory.PolygonDescription> holes = convertToDescription(planetModel, theHoles);
      
      // Now do the polygon itself
      final double[] polyLats = polygon.getPolyLats();
      final double[] polyLons = polygon.getPolyLons();
      
      // I presume the arguments have already been checked
      final List<GeoPoint> points = new ArrayList<>(polyLats.length-1);
      // We skip the last point anyway because the API requires it to be repeated, and geo3d doesn't repeat it.
      for (int i = 0; i < polyLats.length - 1; i++) {
        final int index = polyLats.length - 2 - i;
        points.add(new GeoPoint(planetModel, fromDegrees(polyLats[index]), fromDegrees(polyLons[index])));
      }
      
      descriptions.add(new GeoPolygonFactory.PolygonDescription(points, holes));
    }
    return descriptions;
  }
  

}
