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
package org.apache.lucene.util.bkd;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

import org.apache.lucene.codecs.MutablePointsReader;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.TestUtil;

public class TestMutablePointsReaderUtils extends LuceneTestCase {

  public void testSort() {
    for (int iter = 0; iter < 5; ++iter) {
      doTestSort();
    }
  }

  private void doTestSort() {
    final int bytesPerDim = TestUtil.nextInt(random(), 1, 16);
    final int maxDoc = TestUtil.nextInt(random(), 1, 1 << random().nextInt(30));
    Point[] points = createRandomPoints(1, bytesPerDim, maxDoc);
    DummyPointsReader reader = new DummyPointsReader(points);
    MutablePointsReaderUtils.sort(maxDoc, bytesPerDim, reader, 0, points.length);
    Arrays.sort(points, new Comparator<Point>() {
      @Override
      public int compare(Point o1, Point o2) {
        int cmp = o1.packedValue.compareTo(o2.packedValue);
        if (cmp == 0) {
          cmp = Integer.compare(o1.doc, o2.doc);
        }
        return cmp;
      }
    });
    assertNotSame(points, reader.points);
    assertArrayEquals(points, reader.points);
  }

  public void testSortByDim() {
    for (int iter = 0; iter < 5; ++iter) {
      doTestSortByDim();
    }
  }

  private void doTestSortByDim() {
    final int numDims = TestUtil.nextInt(random(), 1, 8);
    final int bytesPerDim = TestUtil.nextInt(random(), 1, 16);
    final int maxDoc = TestUtil.nextInt(random(), 1, 1 << random().nextInt(30));
    Point[] points = createRandomPoints(numDims, bytesPerDim, maxDoc);
    int[] commonPrefixLengths = new int[numDims];
    for (int i = 0; i < commonPrefixLengths.length; ++i) {
      commonPrefixLengths[i] = TestUtil.nextInt(random(), 0, bytesPerDim);
    }
    BytesRef firstValue = points[0].packedValue;
    for (int i = 1; i < points.length; ++i) {
      for (int dim = 0; dim < numDims; ++dim) {
        int offset = dim * bytesPerDim;
        BytesRef packedValue = points[i].packedValue;
        System.arraycopy(firstValue.bytes, firstValue.offset + offset, packedValue.bytes, packedValue.offset + offset, commonPrefixLengths[dim]);
      }
    }
    DummyPointsReader reader = new DummyPointsReader(points);
    final int sortedDim = random().nextInt(numDims);
    MutablePointsReaderUtils.sortByDim(sortedDim, bytesPerDim, commonPrefixLengths, reader, 0, points.length,
        new BytesRef(), new BytesRef());
    for (int i = 1; i < points.length; ++i) {
      final int offset = sortedDim * bytesPerDim;
      BytesRef previousValue = reader.points[i-1].packedValue;
      BytesRef currentValue = reader.points[i].packedValue;
      int cmp = StringHelper.compare(bytesPerDim,
          previousValue.bytes, previousValue.offset + offset,
          currentValue.bytes, currentValue.offset + offset);
      if (cmp == 0) {
        cmp = reader.points[i - 1].doc - reader.points[i].doc;
      }
      assertTrue(cmp <= 0);
    }
  }

  public void testPartition() {
    for (int iter = 0; iter < 5; ++iter) {
      doTestPartition();
    }
  }

  private void doTestPartition() {
    final int numDims = TestUtil.nextInt(random(), 1, 8);
    final int bytesPerDim = TestUtil.nextInt(random(), 1, 16);
    final int maxDoc = TestUtil.nextInt(random(), 1, 1 << random().nextInt(30));
    Point[] points = createRandomPoints(numDims, bytesPerDim, maxDoc);
    int commonPrefixLength = TestUtil.nextInt(random(), 0, bytesPerDim);
    final int splitDim =  random().nextInt(numDims);
    BytesRef firstValue = points[0].packedValue;
    for (int i = 1; i < points.length; ++i) {
      BytesRef packedValue = points[i].packedValue;
      int offset = splitDim * bytesPerDim;
      System.arraycopy(firstValue.bytes, firstValue.offset + offset, packedValue.bytes, packedValue.offset + offset, commonPrefixLength);
    }
    DummyPointsReader reader = new DummyPointsReader(points);
    final int pivot = TestUtil.nextInt(random(), 0, points.length - 1);
    MutablePointsReaderUtils.partition(maxDoc, splitDim, bytesPerDim, commonPrefixLength, reader, 0, points.length, pivot,
        new BytesRef(), new BytesRef());
    BytesRef pivotValue = reader.points[pivot].packedValue;
    int offset = splitDim * bytesPerDim;
    for (int i = 0; i < points.length; ++i) {
      BytesRef value = reader.points[i].packedValue;
      int cmp = StringHelper.compare(bytesPerDim,
          value.bytes, value.offset + offset,
          pivotValue.bytes, pivotValue.offset + offset);
      if (cmp == 0) {
        cmp = reader.points[i].doc - reader.points[pivot].doc;
      }
      if (i < pivot) {
        assertTrue(cmp <= 0);
      } else if (i > pivot) {
        assertTrue(cmp >= 0);
      } else {
        assertEquals(0, cmp);
      }
    }
  }

  private static Point[] createRandomPoints(int numDims, int bytesPerDim, int maxDoc) {
    final int packedBytesLength = numDims * bytesPerDim;
    final int numPoints = TestUtil.nextInt(random(), 1, 100000);
    Point[] points = new Point[numPoints];
    for (int i = 0; i < numPoints; ++i) {
       byte[] value = new byte[packedBytesLength];
       random().nextBytes(value);
       points[i] = new Point(value, random().nextInt(maxDoc));
    }
    return points;
  }

  private static class Point {
    final BytesRef packedValue;
    final int doc;

    Point(byte[] packedValue, int doc) {
      // use a non-null offset to make sure MutablePointsReaderUtils does not ignore it
      this.packedValue = new BytesRef(packedValue.length + 1);
      this.packedValue.bytes[0] = (byte) random().nextInt(256);
      this.packedValue.offset = 1;
      this.packedValue.length = packedValue.length;
      this.doc = doc;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null || obj instanceof Point == false) {
        return false;
      }
      Point that = (Point) obj;
      return packedValue.equals(that.packedValue) && doc == that.doc;
    }

    @Override
    public int hashCode() {
      return 31 * packedValue.hashCode() + doc;
    }

    @Override
    public String toString() {
      return "value=" + packedValue + " doc=" + doc;
    }
  }

  private static class DummyPointsReader extends MutablePointsReader {

    private final Point[] points;

    DummyPointsReader(Point[] points) {
      this.points = points.clone();
    }

    @Override
    public void close() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public long ramBytesUsed() {
      return 0;
    }

    @Override
    public void getValue(int i, BytesRef packedValue) {
      packedValue.bytes = points[i].packedValue.bytes;
      packedValue.offset = points[i].packedValue.offset;
      packedValue.length = points[i].packedValue.length;
    }

    @Override
    public byte getByteAt(int i, int k) {
      BytesRef packedValue = points[i].packedValue;
      return packedValue.bytes[packedValue.offset + k];
    }

    @Override
    public int getDocID(int i) {
      return points[i].doc;
    }

    @Override
    public void swap(int i, int j) {
      ArrayUtil.swap(points, i, j);
    }

    @Override
    public void checkIntegrity() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void intersect(String fieldName, IntersectVisitor visitor) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public long estimatePointCount(String fieldName, IntersectVisitor visitor) {
      throw new UnsupportedOperationException();
    }

    @Override
    public byte[] getMinPackedValue(String fieldName) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public byte[] getMaxPackedValue(String fieldName) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getNumDimensions(String fieldName) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getBytesPerDimension(String fieldName) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public long size(String fieldName) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getDocCount(String fieldName) {
      throw new UnsupportedOperationException();
    }

  }

}
