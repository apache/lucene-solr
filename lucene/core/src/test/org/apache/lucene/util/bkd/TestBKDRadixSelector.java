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
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.TestUtil;

public class TestBKDRadixSelector extends LuceneTestCase {

  public void testBasic() throws IOException {
    int values = 4;
    Directory dir = getDirectory(values);
    int middle = 2;
    int dimensions =1;
    int bytesPerDimensions = Integer.BYTES;
    int packedLength = dimensions * bytesPerDimensions;
    PointWriter pointWriter = getRandomPointWriter(dir, values, packedLength);
    byte[] bytes = new byte[Integer.BYTES];
    NumericUtils.intToSortableBytes(1, bytes, 0);
    pointWriter.append(bytes, 0);
    NumericUtils.intToSortableBytes(2, bytes, 0);
    pointWriter.append(bytes, 1);
    NumericUtils.intToSortableBytes(3, bytes, 0);
    pointWriter.append(bytes, 2);
    NumericUtils.intToSortableBytes(4, bytes, 0);
    pointWriter.append(bytes, 3);
    pointWriter.close();
    int partitionDim = random().nextInt(dimensions);
    PointWriter leftPointWriter = getRandomPointWriter(dir, middle, packedLength);
    PointWriter rightPointWriter = getRandomPointWriter(dir, values - middle, packedLength);
    BKDRadixSelector radixSelector = new BKDRadixSelector(dimensions, bytesPerDimensions, 1, dir, "test");
    byte[] partitionPoint = radixSelector.select(pointWriter, leftPointWriter, rightPointWriter, 0, values, middle, partitionDim);
    leftPointWriter.close();
    rightPointWriter.close();
    byte[] max = getMax(leftPointWriter, middle, bytesPerDimensions, partitionDim);
    byte[] min = getMin(rightPointWriter, values - middle, bytesPerDimensions, partitionDim);
    int cmp = FutureArrays.compareUnsigned(max, 0, bytesPerDimensions, min, 0, bytesPerDimensions);
    assertTrue(cmp <= 0);
    assertTrue(Arrays.equals(partitionPoint, min));
    dir.close();
  }

  public void testRandomBinaryTiny() throws Exception {
    doTestRandomBinary(10);
  }

  public void testRandomBinaryMedium() throws Exception {
    doTestRandomBinary(25000);
  }

  @Nightly
  public void testRandomBinaryBig() throws Exception {
    doTestRandomBinary(500000);
  }

  private void doTestRandomBinary(int count) throws IOException {
    int values = TestUtil.nextInt(random(), count, count*2);
    Directory dir = getDirectory(values);
    int middle = random().nextInt(values);
    int sortedOnHep = random().nextInt(values /4) + 1;
    int dimensions =  TestUtil.nextInt(random(), 1, 8);
    int bytesPerDimensions = TestUtil.nextInt(random(), 2, 30);
    int packedLength = dimensions * bytesPerDimensions;
    PointWriter pointWriter = getRandomPointWriter(dir, values, packedLength);
    byte[] value = new byte[packedLength];
    for (int i =0; i < values; i++) {
      random().nextBytes(value);
      pointWriter.append(value, i);
    }
    pointWriter.close();
    pointWriter.close();
    for (int splitDim =0; splitDim < dimensions; splitDim++) {
      PointWriter leftPointWriter = getRandomPointWriter(dir, middle, packedLength);
      PointWriter rightPointWriter = getRandomPointWriter(dir, values - middle, packedLength);
      BKDRadixSelector radixSelector = new BKDRadixSelector(dimensions, bytesPerDimensions, sortedOnHep, dir, "test");
      byte[] partitionPoint = radixSelector.select(pointWriter, leftPointWriter, rightPointWriter, 0, values, middle, splitDim);
      leftPointWriter.close();
      rightPointWriter.close();
      byte[] max = getMax(leftPointWriter, middle, bytesPerDimensions, splitDim);
      byte[] min = getMin(rightPointWriter, values - middle, bytesPerDimensions, splitDim);
      int cmp = FutureArrays.compareUnsigned(max, 0, bytesPerDimensions, min, 0, bytesPerDimensions);
      assertTrue(cmp <= 0);
      if (cmp == 0) {
        int maxDocID = getMaxDocId(leftPointWriter, middle, bytesPerDimensions, splitDim, partitionPoint);
        int minDocId = getMinDocId(rightPointWriter, values - middle, bytesPerDimensions, splitDim, partitionPoint);
        assertTrue(minDocId >= maxDocID);
      }
      assertTrue(Arrays.equals(partitionPoint, min));
    }
    dir.close();
  }


  public void testRandomAllDimensionsEquals() throws IOException {
    int values = random().nextInt(15000) + 1;
    Directory dir = getDirectory(values);
    int middle = random().nextInt(values);
    int sortedOnHep = random().nextInt(values /4) + 1;
    int dimensions =  TestUtil.nextInt(random(), 1, 8);
    int bytesPerDimensions = TestUtil.nextInt(random(), 2, 30);
    int packedLength = dimensions * bytesPerDimensions;
    PointWriter pointWriter = getRandomPointWriter(dir, values, packedLength);
    byte[] value = new byte[packedLength];
    random().nextBytes(value);
    for (int i =0; i < values; i++) {
      if (random().nextBoolean()) {
        pointWriter.append(value, i);
      } else {
        pointWriter.append(value, random().nextInt(values));
      }
    }
    pointWriter.close();
    for (int splitDim =0; splitDim < dimensions; splitDim++) {
      PointWriter leftPointWriter = getRandomPointWriter(dir, middle, packedLength);
      PointWriter rightPointWriter = getRandomPointWriter(dir, values - middle, packedLength);
      BKDRadixSelector radixSelector = new BKDRadixSelector(dimensions, bytesPerDimensions, sortedOnHep, dir, "test");
      byte[] partitionPoint = radixSelector.select(pointWriter, leftPointWriter, rightPointWriter, 0, values, middle, splitDim);
      leftPointWriter.close();
      rightPointWriter.close();
      byte[] max = getMax(leftPointWriter, middle, bytesPerDimensions, splitDim);
      byte[] min = getMin(rightPointWriter, values - middle, bytesPerDimensions, splitDim);
      int cmp = FutureArrays.compareUnsigned(max, 0, bytesPerDimensions, min, 0, bytesPerDimensions);
      assertTrue(cmp <= 0);
      if (cmp == 0) {
        int maxDocID = getMaxDocId(leftPointWriter, middle, bytesPerDimensions, splitDim, partitionPoint);
        int minDocId = getMinDocId(rightPointWriter, values - middle, bytesPerDimensions, splitDim, partitionPoint);
        assertTrue(minDocId >= maxDocID);
      }
      assertTrue(Arrays.equals(partitionPoint, max));
    }
    dir.close();
  }

  public void testRandomAllDocsEquals() throws IOException {
    int values = random().nextInt(15000) + 1;
    Directory dir = getDirectory(values);
    int middle = random().nextInt(values);
    int sortedOnHep = random().nextInt(values /4) + 1;
    int dimensions =  TestUtil.nextInt(random(), 1, 8);
    int bytesPerDimensions = TestUtil.nextInt(random(), 2, 30);
    int packedLength = dimensions * bytesPerDimensions;
    PointWriter pointWriter = getRandomPointWriter(dir, values, packedLength);
    byte[] value = new byte[packedLength];
    random().nextBytes(value);
    for (int i =0; i < values; i++) {
      pointWriter.append(value, 0);
    }
    pointWriter.close();
    for (int splitDim =0; splitDim < dimensions; splitDim++) {
      PointWriter leftPointWriter = getRandomPointWriter(dir, middle, packedLength);
      PointWriter rightPointWriter = getRandomPointWriter(dir, values - middle, packedLength);
      BKDRadixSelector radixSelector = new BKDRadixSelector(dimensions, bytesPerDimensions, sortedOnHep, dir, "test");
      byte[] partitionPoint = radixSelector.select(pointWriter, leftPointWriter, rightPointWriter, 0, values, middle, splitDim);
      leftPointWriter.close();
      rightPointWriter.close();
      byte[] max = getMax(leftPointWriter, middle, bytesPerDimensions, splitDim);
      byte[] min = getMin(rightPointWriter, values - middle, bytesPerDimensions, splitDim);
      int cmp = FutureArrays.compareUnsigned(max, 0, bytesPerDimensions, min, 0, bytesPerDimensions);
      assertTrue(cmp <= 0);
      if (cmp == 0) {
        int maxDocID = getMaxDocId(leftPointWriter, middle, bytesPerDimensions, splitDim, partitionPoint);
        int minDocId = getMinDocId(rightPointWriter, values - middle, bytesPerDimensions, splitDim, partitionPoint);
        assertTrue(minDocId >= maxDocID);
      }
      assertTrue(Arrays.equals(partitionPoint, max));
    }
    dir.close();
  }

  public void testRandomFewValues() throws IOException {
    int values = atLeast(15000);
    Directory dir = getDirectory(values);
    int middle = random().nextInt(values);
    int sortedOnHep = random().nextInt(values /4) + 1;
    int dimensions =  TestUtil.nextInt(random(), 1, 8);
    int bytesPerDimensions = TestUtil.nextInt(random(), 2, 30);
    int packedLength = dimensions * bytesPerDimensions;
    PointWriter pointWriter = getRandomPointWriter(dir, values, packedLength);
    int numberValues = random().nextInt(8) + 2;
    byte[][] differentValues = new byte[numberValues][packedLength];
    for (int i =0; i < numberValues; i++) {
      random().nextBytes(differentValues[i]);
    }
    for (int i =0; i < values; i++) {
      pointWriter.append(differentValues[random().nextInt(numberValues)], i);
    }
    pointWriter.close();
    for (int splitDim =0; splitDim < dimensions; splitDim++) {
      PointWriter leftPointWriter = getRandomPointWriter(dir, middle, packedLength);
      PointWriter rightPointWriter = getRandomPointWriter(dir, values - middle, packedLength);
      BKDRadixSelector radixSelector = new BKDRadixSelector(dimensions, bytesPerDimensions, sortedOnHep, dir, "test");
      byte[] partitionPoint = radixSelector.select(pointWriter, leftPointWriter, rightPointWriter, 0, values, middle, splitDim);
      leftPointWriter.close();
      rightPointWriter.close();
      byte[] max = getMax(leftPointWriter, middle, bytesPerDimensions, splitDim);
      byte[] min = getMin(rightPointWriter, values - middle, bytesPerDimensions, splitDim);
      int cmp = FutureArrays.compareUnsigned(max, 0, bytesPerDimensions, min, 0, bytesPerDimensions);
      assertTrue(cmp <= 0);
      if (cmp == 0) {
        int maxDocID = getMaxDocId(leftPointWriter, middle, bytesPerDimensions, splitDim, partitionPoint);
        int minDocId = getMinDocId(rightPointWriter, values - middle, bytesPerDimensions, splitDim, partitionPoint);
        assertTrue(minDocId >= maxDocID);
      }
      assertTrue(Arrays.equals(partitionPoint, max));
    }
    dir.close();
  }



  private PointWriter getRandomPointWriter(Directory dir, int numPoints, int packedBytesLength) throws IOException {
    if (random().nextBoolean() && random().nextBoolean()) {
      return new HeapPointWriter(numPoints, numPoints, packedBytesLength);
    } else {
      return new OfflinePointWriter(dir, "test", packedBytesLength, "data", numPoints);
    }
  }

  private Directory getDirectory(int numPoints) {
    Directory dir;
    if (numPoints > 100000) {
      dir = newFSDirectory(createTempDir("TestBKDTRadixSelector"));
    } else {
      dir = newDirectory();
    }
    return dir;
  }

  private byte[] getMin(PointWriter p, int size, int bytesPerDimension, int dimension) throws  IOException {
    byte[] min = new byte[bytesPerDimension];
    Arrays.fill(min, (byte) 0xff);
    try (PointReader reader = p.getReader(0, size)) {
      byte[] value = new byte[bytesPerDimension];
      while (reader.next()) {
        BytesRef packedValue = reader.packedValue();
        System.arraycopy(packedValue.bytes, packedValue.offset + dimension * bytesPerDimension, value, 0, bytesPerDimension);
        if (min == null || FutureArrays.compareUnsigned(min, 0, bytesPerDimension, value, 0, bytesPerDimension) > 0) {
          System.arraycopy(value, 0, min, 0, bytesPerDimension);
        }
      }
    }
    return min;
  }

  private int getMinDocId(PointWriter p, int size, int bytesPerDimension, int dimension, byte[] partitionPoint) throws  IOException {
   int docID = Integer.MAX_VALUE;
    try (PointReader reader = p.getReader(0, size)) {
      while (reader.next()) {
        BytesRef packedValue = reader.packedValue();
        int offset = dimension * bytesPerDimension;
        if (FutureArrays.compareUnsigned(packedValue.bytes, packedValue.offset + offset, packedValue.offset + offset + bytesPerDimension, partitionPoint, 0, bytesPerDimension) == 0) {
          int newDocID = reader.docID();
          if (newDocID < docID) {
            docID = newDocID;
          }
        }
      }
    }
    return docID;
  }

  private byte[] getMax(PointWriter p, int size, int bytesPerDimension, int dimension) throws  IOException {
    byte[] max = new byte[bytesPerDimension];
    Arrays.fill(max, (byte) 0);
    try (PointReader reader = p.getReader(0, size)) {
      byte[] value = new byte[bytesPerDimension];
      while (reader.next()) {
        BytesRef packedValue = reader.packedValue();
        System.arraycopy(packedValue.bytes, packedValue.offset + dimension * bytesPerDimension, value, 0, bytesPerDimension);
        if (max == null || FutureArrays.compareUnsigned(max, 0, bytesPerDimension, value, 0, bytesPerDimension) < 0) {
          System.arraycopy(value, 0, max, 0, bytesPerDimension);
        }
      }
    }
    return max;
  }

  private int getMaxDocId(PointWriter p, int size, int bytesPerDimension, int dimension, byte[] partitionPoint) throws  IOException {
    int docID = Integer.MIN_VALUE;
    try (PointReader reader = p.getReader(0, size)) {
      while (reader.next()) {
        BytesRef packedValue = reader.packedValue();
        int offset = dimension * bytesPerDimension;
        if (FutureArrays.compareUnsigned(packedValue.bytes, packedValue.offset + offset, packedValue.offset + offset + bytesPerDimension, partitionPoint, 0, bytesPerDimension) == 0) {
          int newDocID = reader.docID();
          if (newDocID > docID) {
            docID = newDocID;
          }
        }
      }
    }
    return docID;
  }

}
