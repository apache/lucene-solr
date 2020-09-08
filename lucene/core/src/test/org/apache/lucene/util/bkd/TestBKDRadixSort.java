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
import org.apache.lucene.util.FutureArrays;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

public class TestBKDRadixSort extends LuceneTestCase {

  public void testRandom() throws IOException {
    BKDConfig config = getRandomConfig();
    int numPoints = TestUtil.nextInt(random(), 1, BKDConfig.DEFAULT_MAX_POINTS_IN_LEAF_NODE);
    HeapPointWriter points = new HeapPointWriter(config, numPoints);
    byte[] value = new byte[config.packedBytesLength];
    for (int i = 0; i < numPoints; i++) {
      random().nextBytes(value);
      points.append(value, i);
    }
    verifySort(config, points,  0, numPoints);
  }

  public void testRandomAllEquals() throws IOException {
    BKDConfig config = getRandomConfig();
    int numPoints = TestUtil.nextInt(random(), 1, BKDConfig.DEFAULT_MAX_POINTS_IN_LEAF_NODE);
    HeapPointWriter points = new HeapPointWriter(config, numPoints);
    byte[] value = new byte[config.packedBytesLength];
    random().nextBytes(value);
    for (int i = 0; i < numPoints; i++) {
      points.append(value, random().nextInt(numPoints));
    }
    verifySort(config, points, 0, numPoints);
  }

  public void testRandomLastByteTwoValues() throws IOException {
    BKDConfig config = getRandomConfig();
    int numPoints = TestUtil.nextInt(random(), 1, BKDConfig.DEFAULT_MAX_POINTS_IN_LEAF_NODE);
    HeapPointWriter points = new HeapPointWriter(config, numPoints);
    byte[] value = new byte[config.packedBytesLength];
    random().nextBytes(value);
    for (int i = 0; i < numPoints; i++) {
      if (random().nextBoolean()) {
        points.append(value, 1);
      } else {
        points.append(value, 2);
      }
    }
    verifySort(config, points, 0, numPoints);
  }

  public void testRandomFewDifferentValues() throws IOException {
    BKDConfig config = getRandomConfig();
    int numPoints = TestUtil.nextInt(random(), 1, BKDConfig.DEFAULT_MAX_POINTS_IN_LEAF_NODE);
    HeapPointWriter points = new HeapPointWriter(config, numPoints);
    int numberValues = random().nextInt(8) + 2;
    byte[][] differentValues = new byte[numberValues][config.packedBytesLength];
    for (int i = 0; i < numberValues; i++) {
      random().nextBytes(differentValues[i]);
    }
    for (int i = 0; i < numPoints; i++) {
      points.append(differentValues[random().nextInt(numberValues)], i);
    }
    verifySort(config, points, 0, numPoints);
  }

  public void testRandomDataDimDifferent() throws IOException {
    BKDConfig config = getRandomConfig();
    int numPoints = TestUtil.nextInt(random(), 1, BKDConfig.DEFAULT_MAX_POINTS_IN_LEAF_NODE);
    HeapPointWriter points = new HeapPointWriter(config, numPoints);
    byte[] value = new byte[config.packedBytesLength];
    int totalDataDimension = config.numDims - config.numIndexDims;
    byte[] dataDimensionValues = new byte[totalDataDimension * config.bytesPerDim];
    random().nextBytes(value);
    for (int i = 0; i < numPoints; i++) {
      random().nextBytes(dataDimensionValues);
      System.arraycopy(dataDimensionValues, 0, value, config.packedIndexBytesLength, totalDataDimension * config.bytesPerDim);
      points.append(value, random().nextInt(numPoints));
    }
    verifySort(config, points, 0, numPoints);
  }

  private void verifySort(BKDConfig config, HeapPointWriter points,int start, int end) throws IOException{
    Directory dir = newDirectory();
    BKDRadixSelector radixSelector = new BKDRadixSelector(config, 1000, dir, "test");
    // we check for each dimension
    for (int splitDim = 0; splitDim < config.numDims; splitDim++) {
      radixSelector.heapRadixSort(points, start, end, splitDim, getRandomCommonPrefix(config, points, start, end, splitDim));
      byte[] previous = new byte[config.packedBytesLength];
      int previousDocId = -1;
      Arrays.fill(previous, (byte) 0);
      int dimOffset = splitDim * config.bytesPerDim;
      for (int j = start; j < end; j++) {
        PointValue pointValue = points.getPackedValueSlice(j);
        BytesRef value = pointValue.packedValue();
        int cmp = FutureArrays.compareUnsigned(value.bytes, value.offset + dimOffset, value.offset + dimOffset + config.bytesPerDim, previous, dimOffset, dimOffset + config.bytesPerDim);
        assertTrue(cmp >= 0);
        if (cmp == 0) {
          int dataOffset = config.numIndexDims * config.bytesPerDim;
          cmp = FutureArrays.compareUnsigned(value.bytes, value.offset + dataOffset, value.offset + config.packedBytesLength, previous, dataOffset, config.packedBytesLength);
          assertTrue(cmp >= 0);
        }
        if (cmp == 0) {
          assertTrue(pointValue.docID() >= previousDocId);
        }
        System.arraycopy(value.bytes, value.offset, previous, 0, config.packedBytesLength);
        previousDocId = pointValue.docID();
      }
    }
    dir.close();
  }

  /** returns a common prefix length equal or lower than the current one */
  private int getRandomCommonPrefix(BKDConfig config, HeapPointWriter points, int start, int end, int sortDim)  {
    int commonPrefixLength = config.bytesPerDim;
    PointValue value = points.getPackedValueSlice(start);
    BytesRef bytesRef = value.packedValue();
    byte[] firstValue = new byte[config.bytesPerDim];
    int offset = sortDim * config.bytesPerDim;
    System.arraycopy(bytesRef.bytes, bytesRef.offset + offset, firstValue, 0, config.bytesPerDim);
    for (int i = start + 1; i < end; i++) {
      value = points.getPackedValueSlice(i);
      bytesRef = value.packedValue();
      int diff = FutureArrays.mismatch(bytesRef.bytes, bytesRef.offset + offset, bytesRef.offset + offset + config.bytesPerDim, firstValue, 0, config.bytesPerDim);
      if (diff != -1 && commonPrefixLength > diff) {
        if (diff == 0) {
          return diff;
        }
        commonPrefixLength = diff;
      }
    }
    return (random().nextBoolean()) ? commonPrefixLength : random().nextInt(commonPrefixLength);
  }

  private BKDConfig getRandomConfig() {
    int numIndexDims = TestUtil.nextInt(random(), 1, BKDConfig.MAX_INDEX_DIMS);
    int numDims = TestUtil.nextInt(random(), numIndexDims, BKDConfig.MAX_DIMS);
    int bytesPerDim = TestUtil.nextInt(random(), 2, 30);
    int maxPointsInLeafNode = TestUtil.nextInt(random(), 50, 2000);
    return new BKDConfig(numDims, numIndexDims, bytesPerDim, maxPointsInLeafNode);
  }
}
