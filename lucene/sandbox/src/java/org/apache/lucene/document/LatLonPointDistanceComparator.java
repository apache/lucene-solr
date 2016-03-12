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

/** Compares docs by distance from an origin */
class LatLonPointDistanceComparator extends FieldComparator<Double> implements LeafFieldComparator {
  final String field;
  final double latitude;
  final double longitude;
  final double missingValue;

  final double[] values;
  double bottom;
  double topValue;
  SortedNumericDocValues currentDocs;
  
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
  }
  
  @Override
  public void setTopValue(Double value) {
    topValue = value.doubleValue();
  }
  
  @Override
  public int compareBottom(int doc) throws IOException {
    return Double.compare(bottom, distance(doc));
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
