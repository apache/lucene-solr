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
 * A {@link FacetArrays} which uses the {@link ArraysPool} to allocate new
 * arrays and pool them on {@link #free()}.
 * 
 * @lucene.experimental
 */
public class ReusingFacetArrays extends FacetArrays {

  private final ArraysPool arraysPool;

  public ReusingFacetArrays(ArraysPool arraysPool) {
    super(arraysPool.arrayLength);
    this.arraysPool = arraysPool;
  }

  @Override
  protected int[] newIntArray() {
    return arraysPool.allocateIntArray();
  }
  
  @Override
  protected float[] newFloatArray() {
    return arraysPool.allocateFloatArray();
  }
  
  @Override
  protected void doFree(float[] floats, int[] ints) {
    arraysPool.free(floats);
    arraysPool.free(ints);
  }
  
}
