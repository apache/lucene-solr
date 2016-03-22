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

import org.apache.lucene.document.Document;
import org.apache.lucene.document.LatLonPoint;
import org.apache.lucene.spatial.util.BaseGeoPointTestCase;
import org.apache.lucene.spatial.util.GeoRect;
import org.apache.lucene.spatial.util.GeoRelationUtils;
import org.apache.lucene.spatial.util.GeoUtils;
import org.apache.lucene.util.SloppyMath;

public class TestLatLonPointQueries extends BaseGeoPointTestCase {

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
    return LatLonPoint.newDistanceQuery(field, centerLat, centerLon, radiusMeters);
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

    if (rect.minLon < rect.maxLon) {
      return GeoRelationUtils.pointInRectPrecise(pointLon, pointLat, rect.minLon, rect.minLat, rect.maxLon, rect.maxLat);
    } else {
      // Rect crosses dateline:
      return GeoRelationUtils.pointInRectPrecise(pointLon, pointLat, -180.0, rect.minLat, rect.maxLon, rect.maxLat)
          || GeoRelationUtils.pointInRectPrecise(pointLon, pointLat, rect.minLon, rect.minLat, 180.0, rect.maxLat);
    }
  }

  @Override
  protected double quantizeLat(double latRaw) {
    return LatLonPoint.decodeLatitude(LatLonPoint.encodeLatitude(latRaw));
  }

  @Override
  protected double quantizeLon(double lonRaw) {
    return LatLonPoint.decodeLongitude(LatLonPoint.encodeLongitude(lonRaw));
  }

  @Override
  protected Boolean polyRectContainsPoint(GeoRect rect, double pointLat, double pointLon) {
    // TODO write better random polygon tests
    
    // note: logic must be slightly different than rectContainsPoint, to satisfy
    // insideness for cases exactly on boundaries.
    
    assert Double.isNaN(pointLat) == false;
    assert rect.crossesDateline() == false;
    double y[] = new double[] { rect.minLat, rect.maxLat, rect.maxLat, rect.minLat, rect.minLat };
    double x[] = new double[] { rect.minLon, rect.minLon, rect.maxLon, rect.maxLon, rect.minLon };

    // TODO: separately test this method is 100% correct, here treat it like a black box (like haversin)
    return GeoRelationUtils.pointInPolygon(x, y, pointLat, pointLon);
  }

  @Override
  protected Boolean circleContainsPoint(double centerLat, double centerLon, double radiusMeters, double pointLat, double pointLon) {
    double distanceMeters = SloppyMath.haversinMeters(centerLat, centerLon, pointLat, pointLon);
    boolean result = distanceMeters <= radiusMeters;
    //System.out.println("  shouldMatch?  centerLon=" + centerLon + " centerLat=" + centerLat + " pointLon=" + pointLon + " pointLat=" + pointLat + " result=" + result + " distanceMeters=" + (distanceKM * 1000));
    return result;
  }

  @Override
  protected Boolean distanceRangeContainsPoint(double centerLat, double centerLon, double minRadiusMeters, double radiusMeters, double pointLat, double pointLon) {
    final double d = SloppyMath.haversinMeters(centerLat, centerLon, pointLat, pointLon);
    return d >= minRadiusMeters && d <= radiusMeters;
  }

  /** Returns random double min to max or up to 1% outside of that range */
  private double randomRangeMaybeSlightlyOutside(double min, double max) {
    return min + (random().nextDouble() + (0.5 - random().nextDouble()) * .02) * (max - min);
  }

  // We rely heavily on GeoUtils.circleToBBox so we test it here:
  public void testRandomCircleToBBox() throws Exception {
    int iters = atLeast(1000);
    for(int iter=0;iter<iters;iter++) {

      boolean useSmallRanges = random().nextBoolean();

      double radiusMeters;

      double centerLat = randomLat(useSmallRanges);
      double centerLon = randomLon(useSmallRanges);

      if (useSmallRanges) {
        // Approx 4 degrees lon at the equator:
        radiusMeters = random().nextDouble() * 444000;
      } else {
        radiusMeters = random().nextDouble() * 50000000;
      }

      // TODO: randomly quantize radius too, to provoke exact math errors?

      GeoRect bbox = GeoUtils.circleToBBox(centerLon, centerLat, radiusMeters);

      int numPointsToTry = 1000;
      for(int i=0;i<numPointsToTry;i++) {

        double lat;
        double lon;

        if (random().nextBoolean()) {
          lat = randomLat(useSmallRanges);
          lon = randomLon(useSmallRanges);
        } else {
          // pick a lat/lon within the bbox or "slightly" outside it to try to improve test efficiency
          lat = quantizeLat(GeoUtils.normalizeLat(randomRangeMaybeSlightlyOutside(bbox.minLat, bbox.maxLat)));
          if (bbox.crossesDateline()) {
            if (random().nextBoolean()) {
              lon = quantizeLon(GeoUtils.normalizeLon(randomRangeMaybeSlightlyOutside(bbox.maxLon, -180)));
            } else {
              lon = quantizeLon(GeoUtils.normalizeLon(randomRangeMaybeSlightlyOutside(0, bbox.minLon)));
            }
          } else {
            lon = quantizeLon(GeoUtils.normalizeLon(randomRangeMaybeSlightlyOutside(bbox.minLon, bbox.maxLon)));
          }
        }

        double distanceMeters = SloppyMath.haversinMeters(centerLat, centerLon, lat, lon);

        // Haversin says it's within the circle:
        boolean haversinSays = distanceMeters <= radiusMeters;

        // BBox says its within the box:
        boolean bboxSays;
        if (bbox.crossesDateline()) {
          if (lat >= bbox.minLat && lat <= bbox.maxLat) {
            bboxSays = lon <= bbox.maxLon || lon >= bbox.minLon;
          } else {
            bboxSays = false;
          }
        } else {
          bboxSays = lat >= bbox.minLat && lat <= bbox.maxLat && lon >= bbox.minLon && lon <= bbox.maxLon;
        }

        if (haversinSays) {
          if (bboxSays == false) {
            System.out.println("small=" + useSmallRanges + " centerLat=" + centerLat + " cetnerLon=" + centerLon + " radiusMeters=" + radiusMeters);
            System.out.println("  bbox: lat=" + bbox.minLat + " to " + bbox.maxLat + " lon=" + bbox.minLon + " to " + bbox.maxLon);
            System.out.println("  point: lat=" + lat + " lon=" + lon);
            System.out.println("  haversin: " + distanceMeters);
            fail("point was within the distance according to haversin, but the bbox doesn't contain it");
          }
        } else {
          // it's fine if haversin said it was outside the radius and bbox said it was inside the box
        }
      }
    }
  }
}
