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

import org.apache.lucene.geo.XYEncodingUtils;
import org.apache.lucene.geo.XYRectangle;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.util.ArrayUtil;

/**
 * Compares documents by distance from an origin point
 * <p>
 * When the least competitive item on the priority queue changes (setBottom), we recompute
 * a bounding box representing competitive distance to the top-N. Then in compareBottom, we can
 * quickly reject hits based on bounding box alone without computing distance for every element.
 */
class XYPointDistanceComparator extends FieldComparator<Double> implements LeafFieldComparator {
  final String field;
  final double x;
  final double y;

  // distances needs to be calculated with square root to
  // avoid numerical issues (square distances are different but
  // actual distances are equal)
  final double[] values;
  double bottom;
  double topValue;
  SortedNumericDocValues currentDocs;

  // current bounding box(es) for the bottom distance on the PQ.
  // these are pre-encoded with XYPoint's encoding and
  // used to exclude uncompetitive hits faster.
  int minX = Integer.MIN_VALUE;
  int maxX = Integer.MAX_VALUE;
  int minY = Integer.MIN_VALUE;
  int maxY = Integer.MAX_VALUE;

  // the number of times setBottom has been called (adversary protection)
  int setBottomCounter = 0;

  private long[] currentValues = new long[4];
  private int valuesDocID = -1;

  public XYPointDistanceComparator(String field, float x, float y, int numHits) {
    this.field = field;
    this.x = x;
    this.y = y;
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
    if (bottom < Float.MAX_VALUE && (setBottomCounter < 1024 || (setBottomCounter & 0x3F) == 0x3F)) {

      XYRectangle rectangle = XYRectangle.fromPointDistance((float) x, (float) y, (float) bottom);
      // pre-encode our box to our integer encoding, so we don't have to decode
      // to double values for uncompetitive hits. This has some cost!
      this.minX = XYEncodingUtils.encode(rectangle.minX);
      this.maxX = XYEncodingUtils.encode(rectangle.maxX);
      this.minY = XYEncodingUtils.encode(rectangle.minY);
      this.maxY = XYEncodingUtils.encode(rectangle.maxY);
    }
    setBottomCounter++;
  }
  
  @Override
  public void setTopValue(Double value) {
    topValue = value.doubleValue();
  }

  private void setValues() throws IOException {
    if (valuesDocID != currentDocs.docID()) {
      assert valuesDocID < currentDocs.docID(): " valuesDocID=" + valuesDocID + " vs " + currentDocs.docID();
      valuesDocID = currentDocs.docID();
      int count = currentDocs.docValueCount();
      if (count > currentValues.length) {
        currentValues = new long[ArrayUtil.oversize(count, Long.BYTES)];
      }
      for(int i=0;i<count;i++) {
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
      int xBits = (int)(encoded >> 32);
      if (xBits < minX || xBits > maxX) {
        continue;
      }
      int yBits = (int)(encoded & 0xFFFFFFFF);
      if (yBits < minY || yBits > maxY) {
        continue;
      }

      // only compute actual distance if its inside "competitive bounding box"
      double docX = XYEncodingUtils.decode(xBits);
      double docY = XYEncodingUtils.decode(yBits);
      final double diffX = x - docX;
      final double diffY = y - docY;
      double distance =  Math.sqrt(diffX * diffX + diffY * diffY);
      cmp = Math.max(cmp, Double.compare(bottom, distance));
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
      XYDocValuesField.checkCompatible(info);
    }
    currentDocs = DocValues.getSortedNumeric(reader, field);
    valuesDocID = -1;
    return this;
  }
  
  @Override
  public Double value(int slot) {
    return values[slot];
  }
  
  @Override
  public int compareTop(int doc) throws IOException {
    return Double.compare(topValue, sortKey(doc));
  }

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
        double docX = XYEncodingUtils.decode((int)(encoded >> 32));
        double docY = XYEncodingUtils.decode((int)(encoded & 0xFFFFFFFF));
        final double diffX = x - docX;
        final double diffY = y - docY;
        double distance =  Math.sqrt(diffX * diffX + diffY * diffY);
        minValue = Math.min(minValue, distance);
      }
    }
    return minValue;
  }
}
