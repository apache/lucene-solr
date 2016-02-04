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
package org.apache.lucene.bkdtree;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.RamUsageEstimator;

final class GrowingHeapLatLonWriter implements LatLonWriter {
  int[] latEncs;
  int[] lonEncs;
  int[] docIDs;
  long[] ords;
  private int nextWrite;
  final int maxSize;

  public GrowingHeapLatLonWriter(int maxSize) {
    latEncs = new int[16];
    lonEncs = new int[16];
    docIDs = new int[16];
    ords = new long[16];
    this.maxSize = maxSize;
  }

  private int[] growExact(int[] arr, int size) {
    assert size > arr.length;
    int[] newArr = new int[size];
    System.arraycopy(arr, 0, newArr, 0, arr.length);
    return newArr;
  }

  private long[] growExact(long[] arr, int size) {
    assert size > arr.length;
    long[] newArr = new long[size];
    System.arraycopy(arr, 0, newArr, 0, arr.length);
    return newArr;
  }

  @Override
  public void append(int latEnc, int lonEnc, long ord, int docID) {
    assert ord == nextWrite;
    if (latEncs.length == nextWrite) {
      int nextSize = Math.min(maxSize, ArrayUtil.oversize(nextWrite+1, RamUsageEstimator.NUM_BYTES_INT));
      assert nextSize > nextWrite: "nextSize=" + nextSize + " vs nextWrite=" + nextWrite;
      latEncs = growExact(latEncs, nextSize);
      lonEncs = growExact(lonEncs, nextSize);
      ords = growExact(ords, nextSize);
      docIDs = growExact(docIDs, nextSize);
    }
    latEncs[nextWrite] = latEnc;
    lonEncs[nextWrite] = lonEnc;
    ords[nextWrite] = ord;
    docIDs[nextWrite] = docID;
    nextWrite++;
  }

  @Override
  public LatLonReader getReader(long start) {
    return new HeapLatLonReader(latEncs, lonEncs, ords, docIDs, (int) start, nextWrite);
  }

  @Override
  public void close() {
  }

  @Override
  public void destroy() {
  }

  @Override
  public String toString() {
    return "GrowingHeapLatLonWriter(count=" + nextWrite + " alloc=" + latEncs.length + ")";
  }
}
