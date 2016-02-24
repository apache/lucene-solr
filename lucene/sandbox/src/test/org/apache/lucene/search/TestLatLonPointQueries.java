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
package org.apache.lucene.search;

import org.apache.lucene.document.LatLonPoint;
import org.apache.lucene.document.Document;
import org.apache.lucene.spatial.util.BaseGeoPointTestCase;
import org.apache.lucene.spatial.util.GeoDistanceUtils;
import org.apache.lucene.spatial.util.GeoRect;

public class TestLatLonPointQueries extends BaseGeoPointTestCase {
  // todo deconflict GeoPoint and BKD encoding methods and error tolerance
  public static final double BKD_TOLERANCE = 1e-7;
  public static final double ENCODING_TOLERANCE = 1e-7;

  @Override
  protected void addPointToDoc(String field, Document doc, double lat, double lon) {
    doc.add(new LatLonPoint(field, lat, lon));
  }

  @Override
  protected Query newRectQuery(String field, GeoRect rect) {
    return LatLonPoint.newBoxQuery(field, rect.minLat, rect.maxLat, rect.minLon, rect.maxLon);
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
    return LatLonPoint.newPolygonQuery(FIELD_NAME, lats, lons);
  }

  @Override
  protected Boolean rectContainsPoint(GeoRect rect, double pointLat, double pointLon) {

    assert Double.isNaN(pointLat) == false;

    // false positive/negatives due to quantization error exist for both rectangles and polygons
    if (compare(pointLat, rect.minLat) == 0
        || compare(pointLat, rect.maxLat) == 0
        || compare(pointLon, rect.minLon) == 0
        || compare(pointLon, rect.maxLon) == 0) {
      return null;
    }

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

  // todo reconcile with GeoUtils (see LUCENE-6996)
  public static double compare(final double v1, final double v2) {
    final double delta = v1-v2;
    return Math.abs(delta) <= BKD_TOLERANCE ? 0 : delta;
  }

  @Override
  protected Boolean polyRectContainsPoint(GeoRect rect, double pointLat, double pointLon) {
    // TODO write better random polygon tests
    return rectContainsPoint(rect, pointLat, pointLon);
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
      assertEquals("lat=" + lat + " latEnc=" + latEnc + " diff=" + (lat - latEnc), lat, latEnc, ENCODING_TOLERANCE);

      double lon = randomLon(small);
      double lonEnc = LatLonPoint.decodeLon(LatLonPoint.encodeLon(lon));
      assertEquals("lon=" + lon + " lonEnc=" + lonEnc + " diff=" + (lon - lonEnc), lon, lonEnc, ENCODING_TOLERANCE);
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
