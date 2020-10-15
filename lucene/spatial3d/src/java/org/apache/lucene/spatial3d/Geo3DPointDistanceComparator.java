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
import org.apache.lucene.spatial3d.geom.GeoDistanceShape;
import org.apache.lucene.spatial3d.geom.PlanetModel;
import org.apache.lucene.spatial3d.geom.XYZBounds;

/**
 * Compares documents by distance from an origin point, using a GeoDistanceShape to compute the distance
 * <p>
 * When the least competitive item on the priority queue changes (setBottom), we recompute
 * a bounding box representing competitive distance to the top-N. Then in compareBottom, we can
 * quickly reject hits based on bounding box alone without computing distance for every element.
 */
class Geo3DPointDistanceComparator extends FieldComparator<Double> implements LeafFieldComparator {
  final String field;
  
  final GeoDistanceShape distanceShape;
  final private PlanetModel planetModel;

  final double[] values;
  double bottomDistance;
  double topValue;
  SortedNumericDocValues currentDocs;
  
  XYZBounds priorityQueueBounds;
  
  // the number of times setBottom has been called (adversary protection)
  int setBottomCounter = 0;

  public Geo3DPointDistanceComparator(String field, final PlanetModel planetModel, final GeoDistanceShape distanceShape, int numHits) {
    this.field = field;
    this.distanceShape = distanceShape;
    this.planetModel = planetModel;
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
    // make bounding box(es) to exclude non-competitive hits, but start
    // sampling if we get called way too much: don't make gobs of bounding
    // boxes if comparator hits a worst case order (e.g. backwards distance order)
    if (setBottomCounter < 1024 || (setBottomCounter & 0x3F) == 0x3F) {
      // Update bounds
      final XYZBounds bounds = new XYZBounds();
      distanceShape.getDistanceBounds(bounds, DistanceStyle.ARC, bottomDistance);
      priorityQueueBounds = bounds;
    }
    setBottomCounter++;
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
      
      if (x > priorityQueueBounds.getMaximumX() ||
        x < priorityQueueBounds.getMinimumX() ||
        y > priorityQueueBounds.getMaximumY() ||
        y < priorityQueueBounds.getMinimumY() ||
        z > priorityQueueBounds.getMaximumZ() ||
        z < priorityQueueBounds.getMinimumZ()) {
        continue;
      }

      cmp = Math.max(cmp, Double.compare(bottomDistance, distanceShape.computeDistance(DistanceStyle.ARC, x, y, z)));
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
        final double distance = distanceShape.computeDistance(DistanceStyle.ARC,
                                                              planetModel.getDocValueEncoder().decodeXValue(encoded),
                                                              planetModel.getDocValueEncoder().decodeYValue(encoded),
                                                              planetModel.getDocValueEncoder().decodeZValue(encoded));
        minValue = Math.min(minValue, distance);
      }
    }
    return minValue;
  }
}
