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

import static org.apache.lucene.geo.GeoEncodingUtils.decodeLatitude;
import static org.apache.lucene.geo.GeoEncodingUtils.decodeLongitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLatitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLongitude;

import java.io.IOException;
import org.apache.lucene.geo.Rectangle;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.SloppyMath;

/**
 * Compares documents by distance from an origin point
 *
 * <p>When the least competitive item on the priority queue changes (setBottom), we recompute a
 * bounding box representing competitive distance to the top-N. Then in compareBottom, we can
 * quickly reject hits based on bounding box alone without computing distance for every element.
 */
class LatLonPointDistanceComparator extends FieldComparator<Double> implements LeafFieldComparator {
  final String field;
  final double latitude;
  final double longitude;

  final double[] values;
  double bottom;
  double topValue;
  SortedNumericDocValues currentDocs;

  // current bounding box(es) for the bottom distance on the PQ.
  // these are pre-encoded with LatLonPoint's encoding and
  // used to exclude uncompetitive hits faster.
  int minLon = Integer.MIN_VALUE;
  int maxLon = Integer.MAX_VALUE;
  int minLat = Integer.MIN_VALUE;
  int maxLat = Integer.MAX_VALUE;

  // second set of longitude ranges to check (for cross-dateline case)
  int minLon2 = Integer.MAX_VALUE;

  // the number of times setBottom has been called (adversary protection)
  int setBottomCounter = 0;

  private long[] currentValues = new long[4];
  private int valuesDocID = -1;

  public LatLonPointDistanceComparator(
      String field, double latitude, double longitude, int numHits) {
    this.field = field;
    this.latitude = latitude;
    this.longitude = longitude;
    this.values = new double[numHits];
  }

  @Override
  public void setScorer(Scorable scorer) {}

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
      Rectangle box = Rectangle.fromPointDistance(latitude, longitude, haversin2(bottom));
      // pre-encode our box to our integer encoding, so we don't have to decode
      // to double values for uncompetitive hits. This has some cost!
      minLat = encodeLatitude(box.minLat);
      maxLat = encodeLatitude(box.maxLat);
      if (box.crossesDateline()) {
        // box1
        minLon = Integer.MIN_VALUE;
        maxLon = encodeLongitude(box.maxLon);
        // box2
        minLon2 = encodeLongitude(box.minLon);
      } else {
        minLon = encodeLongitude(box.minLon);
        maxLon = encodeLongitude(box.maxLon);
        // disable box2
        minLon2 = Integer.MAX_VALUE;
      }
    }
    setBottomCounter++;
  }

  @Override
  public void setTopValue(Double value) {
    topValue = value.doubleValue();
  }

  private void setValues() throws IOException {
    if (valuesDocID != currentDocs.docID()) {
      assert valuesDocID < currentDocs.docID()
          : " valuesDocID=" + valuesDocID + " vs " + currentDocs.docID();
      valuesDocID = currentDocs.docID();
      int count = currentDocs.docValueCount();
      if (count > currentValues.length) {
        currentValues = new long[ArrayUtil.oversize(count, Long.BYTES)];
      }
      for (int i = 0; i < count; i++) {
        currentValues[i] = currentDocs.nextValue();
      }
    }
  }

  @Override
  public int compareBottom(int doc) throws IOException {
    if (doc > currentDocs.docID()) {
      currentDocs.advance(doc);
    }
    if (doc < currentDocs.docID()) {
      return Double.compare(bottom, Double.POSITIVE_INFINITY);
    }

    setValues();

    int numValues = currentDocs.docValueCount();

    int cmp = -1;
    for (int i = 0; i < numValues; i++) {
      long encoded = currentValues[i];

      // test bounding box
      int latitudeBits = (int) (encoded >> 32);
      if (latitudeBits < minLat || latitudeBits > maxLat) {
        continue;
      }
      int longitudeBits = (int) (encoded & 0xFFFFFFFF);
      if ((longitudeBits < minLon || longitudeBits > maxLon) && (longitudeBits < minLon2)) {
        continue;
      }

      // only compute actual distance if its inside "competitive bounding box"
      double docLatitude = decodeLatitude(latitudeBits);
      double docLongitude = decodeLongitude(longitudeBits);
      cmp =
          Math.max(
              cmp,
              Double.compare(
                  bottom,
                  SloppyMath.haversinSortKey(latitude, longitude, docLatitude, docLongitude)));
      // once we compete in the PQ, no need to continue.
      if (cmp > 0) {
        return cmp;
      }
    }
    return cmp;
  }

  @Override
  public void copy(int slot, int doc) throws IOException {
    values[slot] = sortKey(doc);
  }

  @Override
  public LeafFieldComparator getLeafComparator(LeafReaderContext context) throws IOException {
    LeafReader reader = context.reader();
    FieldInfo info = reader.getFieldInfos().fieldInfo(field);
    if (info != null) {
      LatLonDocValuesField.checkCompatible(info);
    }
    currentDocs = DocValues.getSortedNumeric(reader, field);
    valuesDocID = -1;
    return this;
  }

  @Override
  public Double value(int slot) {
    return Double.valueOf(haversin2(values[slot]));
  }

  @Override
  public int compareTop(int doc) throws IOException {
    return Double.compare(topValue, haversin2(sortKey(doc)));
  }

  // TODO: optimize for single-valued case?
  // TODO: do all kinds of other optimizations!
  double sortKey(int doc) throws IOException {
    if (doc > currentDocs.docID()) {
      currentDocs.advance(doc);
    }
    double minValue = Double.POSITIVE_INFINITY;
    if (doc == currentDocs.docID()) {
      setValues();
      int numValues = currentDocs.docValueCount();
      for (int i = 0; i < numValues; i++) {
        long encoded = currentValues[i];
        double docLatitude = decodeLatitude((int) (encoded >> 32));
        double docLongitude = decodeLongitude((int) (encoded & 0xFFFFFFFF));
        minValue =
            Math.min(
                minValue,
                SloppyMath.haversinSortKey(latitude, longitude, docLatitude, docLongitude));
      }
    }
    return minValue;
  }

  // second half of the haversin calculation, used to convert results from haversin1 (used
  // internally
  // for sorting) for display purposes.
  static double haversin2(double partial) {
    if (Double.isInfinite(partial)) {
      return partial;
    }
    return SloppyMath.haversinMeters(partial);
  }
}
