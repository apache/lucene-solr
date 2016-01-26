package org.apache.lucene.search;

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

import org.apache.lucene.document.LatLonPoint;
import org.apache.lucene.document.Document;
import org.apache.lucene.util.BaseGeoPointTestCase;
import org.apache.lucene.util.GeoDistanceUtils;
import org.apache.lucene.util.GeoRect;

public class TestLatLonPointQueries extends BaseGeoPointTestCase {

  @Override
  protected void addPointToDoc(String field, Document doc, double lat, double lon) {
    doc.add(new LatLonPoint(field, lat, lon));
  }

  @Override
  protected Query newRectQuery(String field, GeoRect rect) {
    return new PointInRectQuery(field, rect.minLat, rect.maxLat, rect.minLon, rect.maxLon);
  }

  @Override
  protected Query newDistanceQuery(String field, double centerLat, double centerLon, double radiusMeters) {
    // return new BKDDistanceQuery(field, centerLat, centerLon, radiusMeters);
    return null;
  }

  @Override
  protected Query newDistanceRangeQuery(String field, double centerLat, double centerLon, double minRadiusMeters, double radiusMeters) {
    return null;
  }

  @Override
  protected Query newPolygonQuery(String field, double[] lats, double[] lons) {
    return new PointInPolygonQuery(FIELD_NAME, lats, lons);
  }

  @Override
  protected Boolean rectContainsPoint(GeoRect rect, double pointLat, double pointLon) {

    assert Double.isNaN(pointLat) == false;

    int rectLatMinEnc = LatLonPoint.encodeLat(rect.minLat);
    int rectLatMaxEnc = LatLonPoint.encodeLat(rect.maxLat);
    int rectLonMinEnc = LatLonPoint.encodeLon(rect.minLon);
    int rectLonMaxEnc = LatLonPoint.encodeLon(rect.maxLon);

    int pointLatEnc = LatLonPoint.encodeLat(pointLat);
    int pointLonEnc = LatLonPoint.encodeLon(pointLon);

    if (rect.minLon < rect.maxLon) {
      return pointLatEnc >= rectLatMinEnc &&
        pointLatEnc <= rectLatMaxEnc &&
        pointLonEnc >= rectLonMinEnc &&
        pointLonEnc <= rectLonMaxEnc;
    } else {
      // Rect crosses dateline:
      return pointLatEnc >= rectLatMinEnc &&
        pointLatEnc <= rectLatMaxEnc &&
        (pointLonEnc >= rectLonMinEnc ||
         pointLonEnc <= rectLonMaxEnc);
    }
  }

  private static final double POLY_TOLERANCE = 1e-7;

  @Override
  protected Boolean polyRectContainsPoint(GeoRect rect, double pointLat, double pointLon) {
    if (Math.abs(rect.minLat-pointLat) < POLY_TOLERANCE ||
        Math.abs(rect.maxLat-pointLat) < POLY_TOLERANCE ||
        Math.abs(rect.minLon-pointLon) < POLY_TOLERANCE ||
        Math.abs(rect.maxLon-pointLon) < POLY_TOLERANCE) {
      // The poly check quantizes slightly differently, so we allow for boundary cases to disagree
      return null;
    } else {
      return rectContainsPoint(rect, pointLat, pointLon);
    }
  }

  @Override
  protected Boolean circleContainsPoint(double centerLat, double centerLon, double radiusMeters, double pointLat, double pointLon) {
    double distanceMeters = GeoDistanceUtils.haversin(centerLat, centerLon, pointLat, pointLon);
    boolean result = distanceMeters <= radiusMeters;
    //System.out.println("  shouldMatch?  centerLon=" + centerLon + " centerLat=" + centerLat + " pointLon=" + pointLon + " pointLat=" + pointLat + " result=" + result + " distanceMeters=" + (distanceKM * 1000));
    return result;
  }

  @Override
  protected Boolean distanceRangeContainsPoint(double centerLat, double centerLon, double minRadiusMeters, double radiusMeters, double pointLat, double pointLon) {
    final double d = GeoDistanceUtils.haversin(centerLat, centerLon, pointLat, pointLon);
    return d >= minRadiusMeters && d <= radiusMeters;
  }

  public void testEncodeDecode() throws Exception {
    int iters = atLeast(10000);
    boolean small = random().nextBoolean();
    for(int iter=0;iter<iters;iter++) {
      double lat = randomLat(small);
      double latEnc = LatLonPoint.decodeLat(LatLonPoint.encodeLat(lat));
      assertEquals("lat=" + lat + " latEnc=" + latEnc + " diff=" + (lat - latEnc), lat, latEnc, LatLonPoint.TOLERANCE);

      double lon = randomLon(small);
      double lonEnc = LatLonPoint.decodeLon(LatLonPoint.encodeLon(lon));
      assertEquals("lon=" + lon + " lonEnc=" + lonEnc + " diff=" + (lon - lonEnc), lon, lonEnc, LatLonPoint.TOLERANCE);
    }
  }

  public void testScaleUnscaleIsStable() throws Exception {
    int iters = atLeast(1000);
    boolean small = random().nextBoolean();
    for(int iter=0;iter<iters;iter++) {
      double lat = randomLat(small);
      double lon = randomLon(small);

      double latEnc = LatLonPoint.decodeLat(LatLonPoint.encodeLat(lat));
      double lonEnc = LatLonPoint.decodeLon(LatLonPoint.encodeLon(lon));

      double latEnc2 = LatLonPoint.decodeLat(LatLonPoint.encodeLat(latEnc));
      double lonEnc2 = LatLonPoint.decodeLon(LatLonPoint.encodeLon(lonEnc));
      assertEquals(latEnc, latEnc2, 0.0);
      assertEquals(lonEnc, lonEnc2, 0.0);
    }
  }
}
