package org.apache.lucene.facet.search;

import java.util.Arrays;
import java.util.concurrent.ArrayBlockingQueue;

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

/**
 * A thread-safe pool of {@code int[]} and {@code float[]} arrays. One specifies
 * the maximum number of arrays in the constructor. Calls to
 * {@link #allocateFloatArray()} or {@link #allocateIntArray()} take an array
 * from the pool, and if one is not available, allocate a new one. When you are
 * done using the array, you should {@link #free(int[]) free} it.
 * <p>
 * This class is used by {@link ReusingFacetArrays} for temporal facet
 * aggregation arrays, which can be reused across searches instead of being
 * allocated afresh on every search.
 * 
 * @lucene.experimental
 */
public final class ArraysPool {

  private final ArrayBlockingQueue<int[]> intsPool;
  private final ArrayBlockingQueue<float[]> floatsPool;
  
  public final int arrayLength;
  
  /**
   * Specifies the max number of arrays to pool, as well as the length of each
   * array to allocate.
   * 
   * @param arrayLength the size of the arrays to allocate
   * @param maxArrays the maximum number of arrays to pool, from each type
   * 
   * @throws IllegalArgumentException if maxArrays is set to 0.
   */
  public ArraysPool(int arrayLength, int maxArrays) {
    if (maxArrays == 0) {
      throw new IllegalArgumentException(
          "maxArrays cannot be 0 - don't use this class if you don't intend to pool arrays");
    }
    this.arrayLength = arrayLength;
    this.intsPool = new ArrayBlockingQueue<int[]>(maxArrays);
    this.floatsPool = new ArrayBlockingQueue<float[]>(maxArrays);
  }

  /**
   * Allocates a new {@code int[]}. If there's an available array in the pool,
   * it is used, otherwise a new array is allocated.
   */
  public final int[] allocateIntArray() {
    int[] arr = intsPool.poll();
    if (arr == null) {
      return new int[arrayLength];
    }
    Arrays.fill(arr, 0); // reset array
    return arr;
  }

  /**
   * Allocates a new {@code float[]}. If there's an available array in the pool,
   * it is used, otherwise a new array is allocated.
   */
  public final float[] allocateFloatArray() {
    float[] arr = floatsPool.poll();
    if (arr == null) {
      return new float[arrayLength];
    }
    Arrays.fill(arr, 0f); // reset array
    return arr;
  }

  /**
   * Frees a no-longer-needed array. If there's room in the pool, the array is
   * added to it, otherwise discarded.
   */
  public final void free(int[] arr) {
    if (arr != null) {
      // use offer - if there isn't room, we don't want to wait
      intsPool.offer(arr);
    }
  }

  /**
   * Frees a no-longer-needed array. If there's room in the pool, the array is
   * added to it, otherwise discarded.
   */
  public final void free(float[] arr) {
    if (arr != null) {
      // use offer - if there isn't room, we don't want to wait
      floatsPool.offer(arr);
    }
  }

}
