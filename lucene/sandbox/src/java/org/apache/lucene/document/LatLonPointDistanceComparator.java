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
package org.apache.lucene.document;

import java.io.IOException;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.spatial.util.GeoDistanceUtils;
import org.apache.lucene.spatial.util.GeoRect;
import org.apache.lucene.spatial.util.GeoUtils;

/**
 * Compares documents by distance from an origin point
 * <p>
 * When the least competitive item on the priority queue changes (setBottom), we recompute
 * a bounding box representing competitive distance to the top-N. Then in compareBottom, we can
 * quickly reject hits based on bounding box alone without computing distance for every element.
 */
class LatLonPointDistanceComparator extends FieldComparator<Double> implements LeafFieldComparator {
  final String field;
  final double latitude;
  final double longitude;
  final double missingValue;

  final double[] values;
  double bottom;
  double topValue;
  SortedNumericDocValues currentDocs;
  
  // current bounding box(es) for the bottom distance on the PQ.
  // these are pre-encoded with LatLonPoint's encoding and 
  // used to exclude uncompetitive hits faster.
  int minLon;
  int maxLon;
  int minLat;
  int maxLat;

  // crossesDateLine is true, then we have a second box to check
  boolean crossesDateLine;
  int minLon2;
  int maxLon2;
  int minLat2;
  int maxLat2;

  // the number of times setBottom has been called (adversary protection)
  int setBottomCounter = 0;

  public LatLonPointDistanceComparator(String field, double latitude, double longitude, int numHits, double missingValue) {
    this.field = field;
    this.latitude = latitude;
    this.longitude = longitude;
    this.values = new double[numHits];
    this.missingValue = missingValue;
  }
  
  @Override
  public void setScorer(Scorer scorer) {}

  @Override
  public int compare(int slot1, int slot2) {
    return Double.compare(values[slot1], values[slot2]);
  }
  
  @Override
  public void setBottom(int slot) {
    bottom = values[slot];
    // make bounding box(es) to exclude non-competitive hits, but start
    // sampling if we get called way too much: don't make gobs of bounding
    // boxes if comparator hits a worst case order (e.g. backwards distance order)
    if (setBottomCounter < 1024 || (setBottomCounter & 0x3F) == 0x3F) {
      // don't pass infinite values to circleToBBox: just make a complete box.
      if (bottom == missingValue) {
        minLat = minLon = Integer.MIN_VALUE;
        maxLat = maxLon = Integer.MAX_VALUE;
        crossesDateLine = false;
      } else {
        assert Double.isFinite(bottom);
        GeoRect box = GeoUtils.circleToBBox(longitude, latitude, bottom);
        // pre-encode our box to our integer encoding, so we don't have to decode 
        // to double values for uncompetitive hits. This has some cost!
        int minLatEncoded = LatLonPoint.encodeLatitude(box.minLat);
        int maxLatEncoded = LatLonPoint.encodeLatitude(box.maxLat);
        int minLonEncoded = LatLonPoint.encodeLongitude(box.minLon);
        int maxLonEncoded = LatLonPoint.encodeLongitude(box.maxLon);
        // be sure to not introduce quantization error in our optimization, just 
        // round up our encoded box safely in all directions.
        if (minLatEncoded != Integer.MIN_VALUE) {
          minLatEncoded--;
        }
        if (minLonEncoded != Integer.MIN_VALUE) {
          minLonEncoded--;
        }
        if (maxLatEncoded != Integer.MAX_VALUE) {
          maxLatEncoded++;
        }
        if (maxLonEncoded != Integer.MAX_VALUE) {
          maxLonEncoded++;
        }
        crossesDateLine = box.crossesDateline();
        // crosses dateline: split
        if (crossesDateLine) {
          // box1
          minLon = Integer.MIN_VALUE;
          maxLon = maxLonEncoded;
          minLat = minLatEncoded;
          maxLat = maxLatEncoded;
          // box2
          minLon2 = minLonEncoded;
          maxLon2 = Integer.MAX_VALUE;
          minLat2 = minLatEncoded;
          maxLat2 = maxLatEncoded;
        } else {
          minLon = minLonEncoded;
          maxLon = maxLonEncoded;
          minLat = minLatEncoded;
          maxLat = maxLatEncoded;
        }
      }
    }
    setBottomCounter++;
  }
  
  @Override
  public void setTopValue(Double value) {
    topValue = value.doubleValue();
  }
  
  @Override
  public int compareBottom(int doc) throws IOException {
    currentDocs.setDocument(doc);

    int numValues = currentDocs.count();
    if (numValues == 0) {
      return Double.compare(bottom, missingValue);
    }

    double minValue = Double.POSITIVE_INFINITY;
    for (int i = 0; i < numValues; i++) {
      long encoded = currentDocs.valueAt(i);
      int latitudeBits = (int)(encoded >> 32);
      int longitudeBits = (int)(encoded & 0xFFFFFFFF);
      boolean outsideBox = ((latitudeBits < minLat || longitudeBits < minLon || latitudeBits > maxLat || longitudeBits > maxLon) &&
            (crossesDateLine == false || latitudeBits < minLat2 || longitudeBits < minLon2 || latitudeBits > maxLat2 || longitudeBits > maxLon2));
      // only compute actual distance if its inside "competitive bounding box"
      if (outsideBox == false) {
        double docLatitude = LatLonPoint.decodeLatitude(latitudeBits);
        double docLongitude = LatLonPoint.decodeLongitude(longitudeBits);
        minValue = Math.min(minValue, GeoDistanceUtils.haversin(latitude, longitude, docLatitude, docLongitude));
      }
    }
    return Double.compare(bottom, minValue);
  }
  
  @Override
  public void copy(int slot, int doc) throws IOException {
    values[slot] = distance(doc);
  }
  
  @Override
  public LeafFieldComparator getLeafComparator(LeafReaderContext context) throws IOException {
    LeafReader reader = context.reader();
    FieldInfo info = reader.getFieldInfos().fieldInfo(field);
    if (info != null) {
      LatLonPoint.checkCompatible(info);
    }
    currentDocs = DocValues.getSortedNumeric(reader, field);
    return this;
  }
  
  @Override
  public Double value(int slot) {
    return Double.valueOf(values[slot]);
  }
  
  @Override
  public int compareTop(int doc) throws IOException {
    return Double.compare(topValue, distance(doc));
  }
  
  // TODO: optimize for single-valued case?
  // TODO: do all kinds of other optimizations!
  double distance(int doc) {
    currentDocs.setDocument(doc);

    int numValues = currentDocs.count();
    if (numValues == 0) {
      return missingValue;
    }

    double minValue = Double.POSITIVE_INFINITY;
    for (int i = 0; i < numValues; i++) {
      long encoded = currentDocs.valueAt(i);
      double docLatitude = LatLonPoint.decodeLatitude((int)(encoded >> 32));
      double docLongitude = LatLonPoint.decodeLongitude((int)(encoded & 0xFFFFFFFF));
      minValue = Math.min(minValue, GeoDistanceUtils.haversin(latitude, longitude, docLatitude, docLongitude));
    }
    return minValue;
  }
}
