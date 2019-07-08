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

import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

public class TestBKDRadixSort extends LuceneTestCase {

  public void testRandom() throws IOException {
    int numPoints = TestUtil.nextInt(random(), 1, BKDWriter.DEFAULT_MAX_POINTS_IN_LEAF_NODE);
    int indexDimensions = TestUtil.nextInt(random(), 1, 8);
    int dataDimensions = TestUtil.nextInt(random(), indexDimensions, 8);
    int bytesPerDim = TestUtil.nextInt(random(), 2, 30);
    int packedBytesLength = dataDimensions * bytesPerDim;
    HeapPointWriter points = new HeapPointWriter(numPoints, packedBytesLength);
    byte[] value = new byte[packedBytesLength];
    for (int i = 0; i < numPoints; i++) {
      random().nextBytes(value);
      points.append(value, i);
    }
    verifySort(points, dataDimensions, indexDimensions, 0, numPoints, bytesPerDim);
  }

  public void testRandomAllEquals() throws IOException {
    int numPoints = TestUtil.nextInt(random(), 1, BKDWriter.DEFAULT_MAX_POINTS_IN_LEAF_NODE);
    int indexDimensions = TestUtil.nextInt(random(), 1, 8);
    int dataDimensions = TestUtil.nextInt(random(), indexDimensions, 8);
    int bytesPerDim = TestUtil.nextInt(random(), 2, 30);
    int packedBytesLength = dataDimensions * bytesPerDim;
    HeapPointWriter points = new HeapPointWriter(numPoints, packedBytesLength);
    byte[] value = new byte[packedBytesLength];
    random().nextBytes(value);
    for (int i = 0; i < numPoints; i++) {
      points.append(value, random().nextInt(numPoints));
    }
    verifySort(points, dataDimensions, indexDimensions, 0, numPoints, bytesPerDim);
  }

  public void testRandomLastByteTwoValues() throws IOException {
    int numPoints = TestUtil.nextInt(random(), 1, BKDWriter.DEFAULT_MAX_POINTS_IN_LEAF_NODE);
    int indexDimensions = TestUtil.nextInt(random(), 1, 8);
    int dataDimensions = TestUtil.nextInt(random(), indexDimensions, 8);
    int bytesPerDim = TestUtil.nextInt(random(), 2, 30);
    int packedBytesLength = dataDimensions * bytesPerDim;
    HeapPointWriter points = new HeapPointWriter(numPoints, packedBytesLength);
    byte[] value = new byte[packedBytesLength];
    random().nextBytes(value);
    for (int i = 0; i < numPoints; i++) {
      if (random().nextBoolean()) {
        points.append(value, 1);
      } else {
        points.append(value, 2);
      }
    }
    verifySort(points, dataDimensions, indexDimensions, 0, numPoints, bytesPerDim);
  }

  public void testRandomFewDifferentValues() throws IOException {
    int numPoints = TestUtil.nextInt(random(), 1, BKDWriter.DEFAULT_MAX_POINTS_IN_LEAF_NODE);
    int indexDimensions = TestUtil.nextInt(random(), 1, 8);
    int dataDimensions = TestUtil.nextInt(random(), indexDimensions, 8);
    int bytesPerDim = TestUtil.nextInt(random(), 2, 30);
    int packedBytesLength = dataDimensions * bytesPerDim;
    HeapPointWriter points = new HeapPointWriter(numPoints, packedBytesLength);
    int numberValues = random().nextInt(8) + 2;
    byte[][] differentValues = new byte[numberValues][packedBytesLength];
    for (int i = 0; i < numberValues; i++) {
      random().nextBytes(differentValues[i]);
    }
    for (int i = 0; i < numPoints; i++) {
      points.append(differentValues[random().nextInt(numberValues)], i);
    }
    verifySort(points, dataDimensions, indexDimensions, 0, numPoints, bytesPerDim);
  }

  public void testRandomDataDimDifferent() throws IOException {
    int numPoints = TestUtil.nextInt(random(), 1, BKDWriter.DEFAULT_MAX_POINTS_IN_LEAF_NODE);
    int indexDimensions = TestUtil.nextInt(random(), 1, 8);
    int dataDimensions = TestUtil.nextInt(random(), indexDimensions, 8);
    int bytesPerDim = TestUtil.nextInt(random(), 2, 30);
    int packedBytesLength = dataDimensions * bytesPerDim;
    HeapPointWriter points = new HeapPointWriter(numPoints, packedBytesLength);
    byte[] value = new byte[packedBytesLength];
    int totalDataDimension = dataDimensions - indexDimensions;
    byte[] dataDimensionValues = new byte[totalDataDimension * bytesPerDim];
    random().nextBytes(value);
    for (int i = 0; i < numPoints; i++) {
      random().nextBytes(dataDimensionValues);
      System.arraycopy(dataDimensionValues, 0, value, indexDimensions * bytesPerDim, totalDataDimension * bytesPerDim);
      points.append(value, random().nextInt(numPoints));
    }
    verifySort(points, dataDimensions, indexDimensions, 0, numPoints, bytesPerDim);
  }

  private void verifySort(HeapPointWriter points, int dataDimensions, int indexDimensions, int start, int end, int bytesPerDim) throws IOException{
    int packedBytesLength = dataDimensions * bytesPerDim;
    Directory dir = newDirectory();
    BKDRadixSelector radixSelector = new BKDRadixSelector(dataDimensions, indexDimensions, bytesPerDim, 1000, dir, "test");
    // we check for each dimension
    for (int splitDim = 0; splitDim < dataDimensions; splitDim++) {
      radixSelector.heapRadixSort(points, start, end, splitDim, getRandomCommonPrefix(points, start, end, bytesPerDim, splitDim));
      byte[] previous = new byte[bytesPerDim * dataDimensions];
      int previousDocId = -1;
      Arrays.fill(previous, (byte) 0);
      int dimOffset = splitDim * bytesPerDim;
      for (int j = start; j < end; j++) {
        PointValue pointValue = points.getPackedValueSlice(j);
        BytesRef value = pointValue.packedValue();
        int cmp = Arrays.compareUnsigned(value.bytes, value.offset + dimOffset, value.offset + dimOffset + bytesPerDim, previous, dimOffset, dimOffset + bytesPerDim);
        assertTrue(cmp >= 0);
        if (cmp == 0) {
          int dataOffset = indexDimensions * bytesPerDim;
          cmp = Arrays.compareUnsigned(value.bytes, value.offset + dataOffset, value.offset + packedBytesLength, previous, dataOffset, packedBytesLength);
          assertTrue(cmp >= 0);
        }
        if (cmp == 0) {
          assertTrue(pointValue.docID() >= previousDocId);
        }
        System.arraycopy(value.bytes, value.offset, previous, 0, packedBytesLength);
        previousDocId = pointValue.docID();
      }
    }
    dir.close();
  }

  /** returns a common prefix length equal or lower than the current one */
  private int getRandomCommonPrefix(HeapPointWriter points, int start, int end, int bytesPerDimension, int sortDim)  {
    int commonPrefixLength = bytesPerDimension;
    PointValue value = points.getPackedValueSlice(start);
    BytesRef bytesRef = value.packedValue();
    byte[] firstValue = new byte[bytesPerDimension];
    int offset = sortDim * bytesPerDimension;
    System.arraycopy(bytesRef.bytes, bytesRef.offset + offset, firstValue, 0, bytesPerDimension);
    for (int i = start + 1; i < end; i++) {
      value = points.getPackedValueSlice(i);
      bytesRef = value.packedValue();
      int diff = Arrays.mismatch(bytesRef.bytes, bytesRef.offset + offset, bytesRef.offset + offset + bytesPerDimension, firstValue, 0, bytesPerDimension);
      if (diff != -1 && commonPrefixLength > diff) {
        if (diff == 0) {
          return diff;
        }
        commonPrefixLength = diff;
      }
    }
    return (random().nextBoolean()) ? commonPrefixLength : random().nextInt(commonPrefixLength);
  }
}
