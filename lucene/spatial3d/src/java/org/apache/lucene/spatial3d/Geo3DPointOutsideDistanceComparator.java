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

import java.io.IOException;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.Scorer;

import org.apache.lucene.spatial3d.geom.GeoOutsideDistance;
import org.apache.lucene.spatial3d.geom.DistanceStyle;
import org.apache.lucene.spatial3d.geom.PlanetModel;

/**
 * Compares documents by outside distance, using a GeoOutsideDistance to compute the distance
 */
class Geo3DPointOutsideDistanceComparator extends FieldComparator<Double> implements LeafFieldComparator {
  final String field;
  
  final GeoOutsideDistance distanceShape;

  final double[] values;
  double bottomDistance;
  double topValue;
  SortedNumericDocValues currentDocs;
  
  public Geo3DPointOutsideDistanceComparator(String field, final GeoOutsideDistance distanceShape, int numHits) {
    this.field = field;
    this.distanceShape = distanceShape;
    this.values = new double[numHits];
  }
  
  @Override
  public void setScorer(Scorer scorer) {}

  @Override
  public int compare(int slot1, int slot2) {
    return Double.compare(values[slot1], values[slot2]);
  }
  
  @Override
  public void setBottom(int slot) {
    bottomDistance = values[slot];
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
      return Double.compare(bottomDistance, Double.POSITIVE_INFINITY);
    }

    int cmp = -1;
    for (int i = 0; i < numValues; i++) {
      long encoded = currentDocs.valueAt(i);

      // Test against bounds.
      // First we need to decode...
      final double x = Geo3DDocValuesField.decodeXValue(encoded);
      final double y = Geo3DDocValuesField.decodeYValue(encoded);
      final double z = Geo3DDocValuesField.decodeZValue(encoded);
      
      cmp = Math.max(cmp, Double.compare(bottomDistance, distanceShape.computeOutsideDistance(DistanceStyle.ARC, x, y, z)));
    }
    return cmp;
  }
  
  @Override
  public void copy(int slot, int doc) throws IOException {
    values[slot] = computeMinimumDistance(doc);
  }
  
  @Override
  public LeafFieldComparator getLeafComparator(LeafReaderContext context) throws IOException {
    LeafReader reader = context.reader();
    FieldInfo info = reader.getFieldInfos().fieldInfo(field);
    if (info != null) {
      Geo3DDocValuesField.checkCompatible(info);
    }
    currentDocs = DocValues.getSortedNumeric(reader, field);
    return this;
  }
  
  @Override
  public Double value(int slot) {
    // Return the arc distance
    return Double.valueOf(values[slot] * PlanetModel.WGS84_MEAN);
  }
  
  @Override
  public int compareTop(int doc) throws IOException {
    return Double.compare(topValue, computeMinimumDistance(doc));
  }

  double computeMinimumDistance(final int doc) {
    currentDocs.setDocument(doc);
    double minValue = Double.POSITIVE_INFINITY;
    final int numValues = currentDocs.count();
    for (int i = 0; i < numValues; i++) {
      final long encoded = currentDocs.valueAt(i);
      final double distance = distanceShape.computeOutsideDistance(DistanceStyle.ARC,
        Geo3DDocValuesField.decodeXValue(encoded),
        Geo3DDocValuesField.decodeYValue(encoded),
        Geo3DDocValuesField.decodeZValue(encoded));
      minValue = Math.min(minValue, distance);
    }
    return minValue;
  }
  
}
