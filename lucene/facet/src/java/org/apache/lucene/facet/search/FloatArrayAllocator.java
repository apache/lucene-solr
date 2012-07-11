package org.apache.lucene.facet.search;

import java.util.Arrays;

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
 * An FloatArrayAllocator is an object which manages float array objects
 * of a certain size. These float arrays are needed temporarily during
 * faceted search (see {@link FacetsAccumulator} and can be reused across searches
 * instead of being allocated afresh on every search.
 * <P>
 * An FloatArrayAllocator is thread-safe.
 * 
 * @lucene.experimental
 */
public final class FloatArrayAllocator extends TemporaryObjectAllocator<float[]> {

  // An FloatArrayAllocater deals with integer arrays of a fixed size, size.
  private int size;

  /**
   * Construct an allocator for float arrays of size <CODE>size</CODE>,
   * keeping around a pool of up to <CODE>maxArrays</CODE> old arrays.
   * <P>
   * Note that the pool size only restricts the number of arrays that hang
   * around when not needed, but <I>not</I> the maximum number of arrays
   * that are allocated when actually is use: If a number of concurrent
   * threads ask for an allocation, all of them will get a counter array,
   * even if their number is greater than maxArrays. If an application wants
   * to limit the number of concurrent threads making allocations, it needs
   * to do so on its own - for example by blocking new threads until the
   * existing ones have finished.
   * <P>
   * In particular, when maxArrays=0, this object behaves as a trivial
   * allocator, always allocating a new array and never reusing an old one. 
   */
  public FloatArrayAllocator(int size, int maxArrays) {
    super(maxArrays);
    this.size = size;
  }

  @Override
  public float[] create() {
    return new float[size];
  }

  @Override
  public void clear(float[] array) {
    Arrays.fill(array, 0);
  }

}
