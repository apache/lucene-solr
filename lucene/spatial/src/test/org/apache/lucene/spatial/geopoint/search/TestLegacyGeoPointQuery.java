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

import org.apache.lucene.document.Document;
import org.apache.lucene.search.Query;
import org.apache.lucene.spatial.util.GeoEncodingUtils;
import org.apache.lucene.spatial.geopoint.document.GeoPointField;
import org.apache.lucene.spatial.geopoint.document.GeoPointField.TermEncoding;
import org.apache.lucene.spatial.util.BaseGeoPointTestCase;
import org.apache.lucene.spatial.util.GeoRect;
import org.apache.lucene.spatial.util.GeoRelationUtils;
import org.apache.lucene.util.SloppyMath;

import static org.apache.lucene.spatial.util.GeoDistanceUtils.DISTANCE_PCT_ERR;

/**
 * random testing for GeoPoint query logic (with deprecated numeric encoding)
 * @deprecated remove this when TermEncoding.NUMERIC is removed
 */
@Deprecated
public class TestLegacyGeoPointQuery extends BaseGeoPointTestCase {

  @Override
  protected boolean forceSmall() {
    return false;
  }

  @Override
  protected void addPointToDoc(String field, Document doc, double lat, double lon) {
    doc.add(new GeoPointField(field, lat, lon, GeoPointField.NUMERIC_TYPE_NOT_STORED));
  }

  @Override
  protected Query newRectQuery(String field, GeoRect rect) {
    return new GeoPointInBBoxQuery(field, TermEncoding.NUMERIC, rect.minLat, rect.maxLat, rect.minLon, rect.maxLon);
  }

  @Override
  protected Query newDistanceQuery(String field, double centerLat, double centerLon, double radiusMeters) {
    return new GeoPointDistanceQuery(field, TermEncoding.NUMERIC, centerLat, centerLon, radiusMeters);
  }

  @Override
  protected Query newDistanceRangeQuery(String field, double centerLat, double centerLon, double minRadiusMeters, double radiusMeters) {
    return new GeoPointDistanceRangeQuery(field, TermEncoding.NUMERIC, centerLat, centerLon, minRadiusMeters, radiusMeters);
  }

  @Override
  protected Query newPolygonQuery(String field, double[] lats, double[] lons) {
    return new GeoPointInPolygonQuery(field, TermEncoding.NUMERIC, lats, lons);
  }

  @Override
  protected Boolean rectContainsPoint(GeoRect rect, double pointLat, double pointLon) {
    if (GeoEncodingUtils.compare(pointLon, rect.minLon) == 0.0 ||
        GeoEncodingUtils.compare(pointLon, rect.maxLon) == 0.0 ||
        GeoEncodingUtils.compare(pointLat, rect.minLat) == 0.0 ||
        GeoEncodingUtils.compare(pointLat, rect.maxLat) == 0.0) {
      // Point is very close to rect boundary
      return null;
    }

    if (rect.minLon < rect.maxLon) {
      return GeoRelationUtils.pointInRectPrecise(pointLat, pointLon, rect.minLat, rect.maxLat, rect.minLon, rect.maxLon);
    } else {
      // Rect crosses dateline:
      return GeoRelationUtils.pointInRectPrecise(pointLat, pointLon, rect.minLat, rect.maxLat, -180.0, rect.maxLon)
        || GeoRelationUtils.pointInRectPrecise(pointLat, pointLon, rect.minLat, rect.maxLat, rect.minLon, 180.0);
    }
  }

  @Override
  protected Boolean polyRectContainsPoint(GeoRect rect, double pointLat, double pointLon) {
    return rectContainsPoint(rect, pointLat, pointLon);
  }

  @Override
  protected Boolean circleContainsPoint(double centerLat, double centerLon, double radiusMeters, double pointLat, double pointLon) {
    if (radiusQueryCanBeWrong(centerLat, centerLon, pointLon, pointLat, radiusMeters)) {
      return null;
    } else {
      return SloppyMath.haversinMeters(centerLat, centerLon, pointLat, pointLon) <= radiusMeters;
    }
  }

  @Override
  protected Boolean distanceRangeContainsPoint(double centerLat, double centerLon, double minRadiusMeters, double radiusMeters, double pointLat, double pointLon) {
    if (radiusQueryCanBeWrong(centerLat, centerLon, pointLon, pointLat, minRadiusMeters)
        || radiusQueryCanBeWrong(centerLat, centerLon, pointLon, pointLat, radiusMeters)) {
      return null;
    } else {
      final double d = SloppyMath.haversinMeters(centerLat, centerLon, pointLat, pointLon);
      return d >= minRadiusMeters && d <= radiusMeters;
    }
  }

  private static boolean radiusQueryCanBeWrong(double centerLat, double centerLon, double ptLon, double ptLat,
                                               final double radius) {
    final long hashedCntr = GeoEncodingUtils.mortonHash(centerLat, centerLon);
    centerLon = GeoEncodingUtils.mortonUnhashLon(hashedCntr);
    centerLat = GeoEncodingUtils.mortonUnhashLat(hashedCntr);
    final long hashedPt = GeoEncodingUtils.mortonHash(ptLat, ptLon);
    ptLon = GeoEncodingUtils.mortonUnhashLon(hashedPt);
    ptLat = GeoEncodingUtils.mortonUnhashLat(hashedPt);

    double ptDistance = SloppyMath.haversinMeters(centerLat, centerLon, ptLat, ptLon);
    double delta = StrictMath.abs(ptDistance - radius);

    // if its within the distance error then it can be wrong
    return delta < (ptDistance*DISTANCE_PCT_ERR);
  }
}
