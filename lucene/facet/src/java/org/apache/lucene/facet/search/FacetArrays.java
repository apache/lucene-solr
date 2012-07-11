package org.apache.lucene.facet.search;

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
 * Provider of arrays used for facet operations such as counting.
 * 
 * @lucene.experimental
 */
public class FacetArrays {

  private int[] intArray;
  private float[] floatArray;
  private IntArrayAllocator intArrayAllocator;
  private FloatArrayAllocator floatArrayAllocator;
  private int arraysLength;

  /**
   * Create a FacetArrays with certain array allocators.
   * @param intArrayAllocator allocator for int arrays.
   * @param floatArrayAllocator allocator for float arrays.
   */
  public FacetArrays(IntArrayAllocator intArrayAllocator,
                      FloatArrayAllocator floatArrayAllocator) {
    this.intArrayAllocator = intArrayAllocator;
    this.floatArrayAllocator = floatArrayAllocator;
  }

  /**
   * Notify allocators that they can free arrays allocated 
   * on behalf of this FacetArrays object. 
   */
  public void free() {
    if (intArrayAllocator!=null) {
      intArrayAllocator.free(intArray);
      // Should give up handle to the array now
      // that it is freed.
      intArray = null;
    }
    if (floatArrayAllocator!=null) {
      floatArrayAllocator.free(floatArray);
      // Should give up handle to the array now
      // that it is freed.
      floatArray = null;
    }
    arraysLength = 0;
  }

  /**
   * Obtain an int array, e.g. for facet counting. 
   */
  public int[] getIntArray() {
    if (intArray == null) {
      intArray = intArrayAllocator.allocate();
      arraysLength = intArray.length;
    }
    return intArray;
  }

  /** Obtain a float array, e.g. for evaluating facet association values. */
  public float[] getFloatArray() {
    if (floatArray == null) {
      floatArray = floatArrayAllocator.allocate();
      arraysLength = floatArray.length;
    }
    return floatArray;
  }

  /**
   * Return the arrays length
   */
  public int getArraysLength() {
    return arraysLength;
  }

}