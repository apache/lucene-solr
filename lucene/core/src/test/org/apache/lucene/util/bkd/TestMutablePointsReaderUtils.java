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
        int cmp = StringHelper.compare(bytesPerDim, o1.packedValue, 0, o2.packedValue, 0);
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
    for (int i = 1; i < points.length; ++i) {
      for (int dim = 0; dim < numDims; ++dim) {
        int offset = dim * bytesPerDim;
        System.arraycopy(points[0].packedValue, offset, points[i].packedValue, offset, commonPrefixLengths[dim]);
      }
    }
    DummyPointsReader reader = new DummyPointsReader(points);
    final int sortedDim = random().nextInt(numDims);
    MutablePointsReaderUtils.sortByDim(sortedDim, bytesPerDim, commonPrefixLengths, reader, 0, points.length,
        new byte[numDims * bytesPerDim], new byte[numDims * bytesPerDim]);
    for (int i = 1; i < points.length; ++i) {
      final int offset = sortedDim * bytesPerDim;
      int cmp = StringHelper.compare(bytesPerDim, reader.points[i-1].packedValue, offset, reader.points[i].packedValue, offset);
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
    for (int i = 1; i < points.length; ++i) {
      int offset = splitDim * bytesPerDim;
      System.arraycopy(points[0].packedValue, offset, points[i].packedValue, offset, commonPrefixLength);
    }
    DummyPointsReader reader = new DummyPointsReader(points);
    final int pivot = TestUtil.nextInt(random(), 0, points.length - 1);
    MutablePointsReaderUtils.partition(maxDoc, splitDim, bytesPerDim, commonPrefixLength, reader, 0, points.length, pivot,
        new byte[numDims * bytesPerDim], new byte[numDims * bytesPerDim]);
    int offset = splitDim * bytesPerDim;
    for (int i = 0; i < points.length; ++i) {
      int cmp = StringHelper.compare(bytesPerDim, reader.points[i].packedValue, offset, reader.points[pivot].packedValue, offset);
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
    final byte[] packedValue;
    final int doc;

    Point(byte[] packedValue, int doc) {
      this.packedValue = packedValue;
      this.doc = doc;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null || obj instanceof Point == false) {
        return false;
      }
      Point that = (Point) obj;
      return Arrays.equals(packedValue, that.packedValue) && doc == that.doc;
    }

    @Override
    public int hashCode() {
      return 31 * Arrays.hashCode(packedValue) + doc;
    }

    @Override
    public String toString() {
      return "value=" + new BytesRef(packedValue) + " doc=" + doc;
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
    public void getValue(int i, byte[] packedValue) {
      System.arraycopy(points[i].packedValue, 0, packedValue, 0, points[i].packedValue.length);
    }

    @Override
    public byte getByteAt(int i, int k) {
      return points[i].packedValue[k];
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
