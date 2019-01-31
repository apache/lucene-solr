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

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FutureArrays;
import org.apache.lucene.util.IntroSelector;

/**
 *
 * Offline Radix selector for BKD tree.
 *
 *  @lucene.internal
 * */
public final class BKDRadixSelector {
  //size of the histogram
  private static final int HISTOGRAM_SIZE = 256;
  // we store one histogram per recursion level
  private final int[][] histogram;
  //bytes we are sorting
  private final int bytesPerDim;
  //data size
  private int packedByteLength;
  //flag to when we are moving to sort on heap
  private final int maxPointsSortedOffHeap;
  //holder for partition points
  private int[] partitionBucket;
  //holder for partition bytes
  private byte[] partitionBytes;
  //re-usable on-heap selector
  private HeapSelector heapSelector;
  // scratch object to move bytes around
  BytesRef bytesRef = new BytesRef();

  /**
   * Sole constructor.
   */
  public BKDRadixSelector(int numDim, int bytesPerDim, int maxPointsSortedOffHeap) {
    this.bytesPerDim = bytesPerDim;
    this.packedByteLength = numDim * bytesPerDim;
    this.maxPointsSortedOffHeap = maxPointsSortedOffHeap;
    this.partitionBucket = new int[bytesPerDim];
    this.partitionBytes =  new byte[bytesPerDim];
    this.histogram = new int[bytesPerDim][HISTOGRAM_SIZE];
    this.bytesRef.length = numDim * bytesPerDim;
    this.heapSelector = new HeapSelector(numDim, bytesPerDim);
  }

  /**
   * Method to partition the input data. It returns the value of the dimension where
   * the split happens.
   */
  public byte[] select(PointWriter data, PointWriter left, PointWriter right, int from, int to, int middle, int dim) throws IOException {
    checkArgs(from, to, middle);

    //If we are on heap then we just select on heap
    if (data instanceof HeapPointWriter) {
      return heapSelect((HeapPointWriter) data, left, right, dim, middle, 0, 0);
    }

    //reset histogram
    for (int i = 0; i < bytesPerDim; i++) {
      Arrays.fill(histogram[i], 0);
    }
    //find common prefix, it does already set histogram values if needed
    int commonPrefix = findCommonPrefix(data, from, to, dim);

    //if all equals we just partition the data
    if (commonPrefix == bytesPerDim) {
      return partition(data, left, right, from, to, middle, dim, null, commonPrefix - 1, middle);
    }
    //let's rock'n'roll
    return buildHistogramAndPartition(data, left, right, from, to, middle, 0, commonPrefix, dim,0, 0);
  }

  void checkArgs(int from, int to, int middle) {
    if (middle < from) {
      throw new IllegalArgumentException("middle must be >= from");
    }
    if (middle >= to) {
      throw new IllegalArgumentException("middle must be < to");
    }
  }

  private int findCommonPrefix(PointWriter data, int from, int to, int dim) throws IOException{
    //find common prefix
    byte[] commonPrefix = new byte[bytesPerDim];
    int commonPrefixPosition = bytesPerDim;
    try (PointReader reader = data.getReader(from, to)) {
      reader.next();
      byte[] packedValue = reader.packedValue();
      System.arraycopy(packedValue, dim * bytesPerDim, commonPrefix, 0, bytesPerDim);
      for (int i =from + 1; i< to; i++) {
        reader.next();
        packedValue = reader.packedValue();
        int j = FutureArrays.mismatch(commonPrefix, 0, commonPrefixPosition, packedValue, dim * bytesPerDim, dim * bytesPerDim + commonPrefixPosition);
        if (j == 0) {
          return 0;
        } else if (j != -1) {
          commonPrefixPosition =j;
        }
      }
    }
    //build histogram up to the common prefix
    for (int i=0; i < commonPrefixPosition; i++) {
      partitionBucket[i] = commonPrefix[i] & 0xff;
      partitionBytes[i] = commonPrefix[i];
      histogram[i][partitionBucket[i]] = to - from;
    }
    return commonPrefixPosition;
  }

  private byte[] buildHistogramAndPartition(PointWriter data, PointWriter left, PointWriter right, int from, int to, int middle,
                                            int iteration,  int commonPrefix, int dim, int leftCount, int rightCount) throws IOException {
    //build histogram at the commonPrefix byte
    try (PointReader reader = data.getReader(from, to)) {
      if (iteration == 0) {
        // we specialise this case
        reader.buildHistogram(dim * bytesPerDim + commonPrefix, histogram[commonPrefix]);
      } else {
        while (reader.next()) {
          byte[] packedValue = reader.packedValue();
          if (hasCommonPrefix(packedValue, dim, commonPrefix)) {
            int bucket = packedValue[dim * bytesPerDim + commonPrefix] & 0xff;
            histogram[commonPrefix][bucket]++;
          }
        }
      }
    }
    //Count left points and record the partition point
    for(int i = 0; i < HISTOGRAM_SIZE; i++) {
      int size = histogram[commonPrefix][i];
      if (leftCount + size > middle) {
        partitionBucket[commonPrefix] = i;
        partitionBytes[commonPrefix] = (byte) i;
        break;
      }
      leftCount += size;
    }
    //Count right points
    for(int i = partitionBucket[commonPrefix] + 1; i < HISTOGRAM_SIZE; i++) {
      rightCount += histogram[commonPrefix][i];;
    }

    assert leftCount + rightCount + histogram[commonPrefix][partitionBucket[commonPrefix]] == to - from;

    if (commonPrefix == bytesPerDim - 1) {
      // we are done, lets break data around. No need to sort on heap, maybe we need to sort by docID?
      return partition(data, left, right, from, to, middle, dim, null, commonPrefix, middle - leftCount);
    } else if (histogram[commonPrefix][partitionBucket[commonPrefix]] <= maxPointsSortedOffHeap) {
      // last points are done on heap
      int size = histogram[commonPrefix][partitionBucket[commonPrefix]];
      HeapPointWriter writer = new HeapPointWriter(size, size, packedByteLength);
      return partition(data, left, right, from, to, middle, dim, writer, commonPrefix, 0);
    } else {
      // iterate next common prefix
      return buildHistogramAndPartition(data, left, right, from, to, middle, ++iteration, ++commonPrefix, dim,  leftCount, rightCount);
    }
  }

  private byte[] partition(PointWriter data, PointWriter left, PointWriter right, int from, int to, int middle, int dim,
                           HeapPointWriter sorted, int commonPrefix, int numDocsTiebreak) throws IOException {
    int leftCounter = 0;
    int tiebreakCounter = 0;

    try (PointReader reader = data.getReader(from, to)) {
      while(reader.next()) {
        assert leftCounter <= middle;
        byte[] packedValue = reader.packedValue();
        int docID = reader.docID();
        int thisCommonPrefix = getCommonPrefix(packedValue, dim, commonPrefix);
        int bucket = getBucket(packedValue, dim, thisCommonPrefix);

        if (bucket < this.partitionBucket[thisCommonPrefix]) {
          // to the left side
          left.append(packedValue, docID);
          leftCounter++;
        } else if (bucket > this.partitionBucket[thisCommonPrefix]) {
          // to the right side
          right.append(packedValue, docID);
        } else {
          assert thisCommonPrefix == commonPrefix;
          //we should be at the common prefix, if we are at the end of the array
          //then use the  tie-break value, else store for sorting later
          if (thisCommonPrefix == bytesPerDim - 1) {
            if (tiebreakCounter < numDocsTiebreak) {
              left.append(packedValue, docID);
              tiebreakCounter++;
            } else {
              right.append(packedValue, docID);
            }
          } else {
            sorted.append(packedValue, docID);
          }
        }
      }
    }
    // we might need still have work to do
    if (sorted != null) {
      // make sure we are just not soring all data
      assert sorted.count() != data.count();
      return heapSelect(sorted, left, right, dim, middle, leftCounter, commonPrefix);
    }
    // We did not have any points to sort on memory,
    // compute the partition point from the common prefix
    byte[] partition = new byte[bytesPerDim];
    for (int i = 0; i < bytesPerDim; i++) {
      partition[i] = (byte)partitionBucket[i];
    }
    return partition;
  }

  private byte[] heapSelect(HeapPointWriter writer, PointWriter left, PointWriter right, int dim, int k, int leftCounter, int commonPrefix) throws IOException {

    assert writer.count() != k - leftCounter;
    heapSelector.setHeapPointWriter(writer, dim, commonPrefix);
    heapSelector.select(0, Math.toIntExact(writer.count()), k - leftCounter);

    byte[] partition = new byte[bytesPerDim];
    Arrays.fill(partition, (byte) 0xff);
    for (int i = 0; i < writer.count(); i++) {
      writer.getPackedValueSlice(i, bytesRef);
      int docID = writer.docIDs[i];
      if (leftCounter < k) {
        left.append(bytesRef, docID);
        leftCounter++;
      } else {
        right.append(bytesRef, docID);
        if (FutureArrays.compareUnsigned(bytesRef.bytes, bytesRef.offset + dim * bytesPerDim, bytesRef.offset + (dim + 1) * bytesPerDim, partition, 0, bytesPerDim) < 0) {
          System.arraycopy(bytesRef.bytes, bytesRef.offset + dim * bytesPerDim, partition, 0, bytesPerDim);
        }
      }
    }
    assert k == leftCounter;
    return partition;
  }

  private boolean hasCommonPrefix(byte[] packedValue, int dim, int commonPrefix) {
    return FutureArrays.compareUnsigned(partitionBytes, 0, commonPrefix, packedValue, dim * bytesPerDim, dim * bytesPerDim + commonPrefix) == 0;
  }

  private int getCommonPrefix(byte[] value, int dim, int maxCommmonPrefix) {
    int commonPrefix = FutureArrays.mismatch(value, dim * bytesPerDim, dim * bytesPerDim + maxCommmonPrefix, partitionBytes, 0, maxCommmonPrefix);
    if ( commonPrefix == -1) {
      return maxCommmonPrefix;
    }
    return commonPrefix;
  }

  private int getBucket(byte[] value, int dim, int commonPrefix) {
    return value[dim * bytesPerDim + commonPrefix] & 0xff;
  }

  /**
   * Reusable on-heap intro  selector.
   */
  private static class HeapSelector extends IntroSelector {
    //fixed from the beginning
    private final int bytesPerDim;
    //writer that must be set before running
    private HeapPointWriter writer;
    //byte start position for comparisons
    private int start;
    //byte end position for comparisons
    private int end;
    //common prefix
    private int commonPrefix;
    //scratch object
    private final BytesRef bytesRef1 = new BytesRef();
    //scratch object
    private final BytesRef bytesRef2 = new BytesRef();
    //holds the current pivot
    private final BytesRef pivot = new BytesRef();
    //holds the current pivot docID
    private int pivotDoc;

    public HeapSelector(int numDim, int bytesPerDim) {
      this.bytesPerDim = bytesPerDim;
      this.bytesRef1.length = numDim * bytesPerDim;
      this.bytesRef2.length = numDim * bytesPerDim;
      this.pivot.length = numDim * bytesPerDim;
    }

    /**
     * Set the data to be sorted. It must be done before calling select.
     * @param writer The data to be sorted
     * @param dim the dimension to sort from
     * @param commonPrefix byte we need to start from the comparisons
     */
    public void setHeapPointWriter(HeapPointWriter writer, int dim, int commonPrefix) {
      this.writer = writer;
      this.commonPrefix = commonPrefix;
      start = dim * bytesPerDim + commonPrefix;
      end = dim * bytesPerDim + bytesPerDim;
    }

    @Override
    protected void swap(int i, int j){
      writer.swap(i, j);
    }

    @Override
    protected void setPivot(int i) {
      writer.getPackedValueSlice(i, pivot);
      pivotDoc = writer.docIDs[i];
    }

    @Override
    protected int compare(int i, int j) {
      if (commonPrefix < bytesPerDim) {
        writer.getPackedValueSlice(i, bytesRef1);
        writer.getPackedValueSlice(j, bytesRef2);
        int cmp = FutureArrays.compareUnsigned(bytesRef1.bytes, bytesRef1.offset + start, bytesRef1.offset + end, bytesRef2.bytes, bytesRef2.offset + start, bytesRef2.offset + end);
        if (cmp != 0) {
          return cmp;
        }
      }
      return writer.docIDs[i] - writer.docIDs[j];
    }

    @Override
    protected int comparePivot(int j) {
      if (commonPrefix < bytesPerDim) {
        writer.getPackedValueSlice(j, bytesRef1);
        int cmp = FutureArrays.compareUnsigned(pivot.bytes, pivot.offset + start, pivot.offset + end, bytesRef1.bytes, bytesRef1.offset + start, bytesRef1.offset + end);
        if (cmp != 0) {
          return cmp;
        }
      }
      return pivotDoc - writer.docIDs[j];
    }
  }
}
