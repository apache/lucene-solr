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

import org.apache.lucene.document.DimensionalLatLonField;
import org.apache.lucene.document.Document;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BaseGeoPointTestCase;
import org.apache.lucene.util.GeoRect;
import org.apache.lucene.util.SloppyMath;

public class TestDimensionalQueries extends BaseGeoPointTestCase {

  @Override
  protected void addPointToDoc(String field, Document doc, double lat, double lon) {
    doc.add(new DimensionalLatLonField(field, lat, lon));
  }

  @Override
  protected Query newRectQuery(String field, GeoRect rect) {
    return new DimensionalPointInRectQuery(field, rect.minLat, rect.maxLat, rect.minLon, rect.maxLon);
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
    return new DimensionalPointInPolygonQuery(FIELD_NAME, lats, lons);
  }

  @Override
  protected Boolean rectContainsPoint(GeoRect rect, double pointLat, double pointLon) {

    assert Double.isNaN(pointLat) == false;

    int rectLatMinEnc = DimensionalLatLonField.encodeLat(rect.minLat);
    int rectLatMaxEnc = DimensionalLatLonField.encodeLat(rect.maxLat);
    int rectLonMinEnc = DimensionalLatLonField.encodeLon(rect.minLon);
    int rectLonMaxEnc = DimensionalLatLonField.encodeLon(rect.maxLon);

    int pointLatEnc = DimensionalLatLonField.encodeLat(pointLat);
    int pointLonEnc = DimensionalLatLonField.encodeLon(pointLon);

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
    double distanceKM = SloppyMath.haversin(centerLat, centerLon, pointLat, pointLon);
    boolean result = distanceKM*1000.0 <= radiusMeters;
    //System.out.println("  shouldMatch?  centerLon=" + centerLon + " centerLat=" + centerLat + " pointLon=" + pointLon + " pointLat=" + pointLat + " result=" + result + " distanceMeters=" + (distanceKM * 1000));
    return result;
  }

  @Override
  protected Boolean distanceRangeContainsPoint(double centerLat, double centerLon, double minRadiusMeters, double radiusMeters, double pointLat, double pointLon) {
    final double d = SloppyMath.haversin(centerLat, centerLon, pointLat, pointLon)*1000.0;
    return d >= minRadiusMeters && d <= radiusMeters;
  }

  public void testEncodeDecode() throws Exception {
    int iters = atLeast(10000);
    boolean small = random().nextBoolean();
    for(int iter=0;iter<iters;iter++) {
      double lat = randomLat(small);
      double latQuantized = DimensionalLatLonField.decodeLat(DimensionalLatLonField.encodeLat(lat));
      assertEquals(lat, latQuantized, DimensionalLatLonField.TOLERANCE);

      double lon = randomLon(small);
      double lonQuantized = DimensionalLatLonField.decodeLon(DimensionalLatLonField.encodeLon(lon));
      assertEquals(lon, lonQuantized, DimensionalLatLonField.TOLERANCE);
    }
  }
}
