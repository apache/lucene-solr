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
import org.apache.lucene.search.Scorable;
import org.apache.lucene.spatial3d.geom.DistanceStyle;
import org.apache.lucene.spatial3d.geom.GeoOutsideDistance;
import org.apache.lucene.spatial3d.geom.PlanetModel;

/**
 * Compares documents by outside distance, using a GeoOutsideDistance to compute the distance
 */
class Geo3DPointOutsideDistanceComparator extends FieldComparator<Double> implements LeafFieldComparator {
  final String field;
  
  final GeoOutsideDistance distanceShape;
  final private PlanetModel planetModel;

  final double[] values;
  double bottomDistance;
  double topValue;
  SortedNumericDocValues currentDocs;
  
  public Geo3DPointOutsideDistanceComparator(String field, final PlanetModel planetModel, final GeoOutsideDistance distanceShape, int numHits) {
    this.field = field;
    this.planetModel = planetModel;
    this.distanceShape = distanceShape;
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
    bottomDistance = values[slot];
  }
  
  @Override
  public void setTopValue(Double value) {
    topValue = value.doubleValue();
  }
  
  @Override
  public int compareBottom(int doc) throws IOException {
    if (doc > currentDocs.docID()) {
      currentDocs.advance(doc);
    }
    if (doc < currentDocs.docID()) {
      return Double.compare(bottomDistance, Double.POSITIVE_INFINITY);
    }

    int numValues = currentDocs.docValueCount();
    assert numValues > 0;

    int cmp = -1;
    for (int i = 0; i < numValues; i++) {
      long encoded = currentDocs.nextValue();

      // Test against bounds.
      // First we need to decode...
      final double x = planetModel.getDocValueEncoder().decodeXValue(encoded);
      final double y = planetModel.getDocValueEncoder().decodeYValue(encoded);
      final double z = planetModel.getDocValueEncoder().decodeZValue(encoded);
      
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
    return Double.valueOf(values[slot] * planetModel.getMeanRadius());
  }
  
  @Override
  public int compareTop(int doc) throws IOException {
    return Double.compare(topValue, computeMinimumDistance(doc));
  }

  double computeMinimumDistance(final int doc) throws IOException {
    if (doc > currentDocs.docID()) {
      currentDocs.advance(doc);
    }
    double minValue = Double.POSITIVE_INFINITY;
    if (doc == currentDocs.docID()) {
      final int numValues = currentDocs.docValueCount();
      for (int i = 0; i < numValues; i++) {
        final long encoded = currentDocs.nextValue();
        final double distance = distanceShape.computeOutsideDistance(DistanceStyle.ARC,
                                                                     planetModel.getDocValueEncoder().decodeXValue(encoded),
                                                                     planetModel.getDocValueEncoder().decodeYValue(encoded),
                                                                     planetModel.getDocValueEncoder().decodeZValue(encoded));
        minValue = Math.min(minValue, distance);
      }
    }
    return minValue;
  }
}
