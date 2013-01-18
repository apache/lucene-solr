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
 * Provider of arrays used for facets aggregation. Returns either an
 * {@code int[]} or {@code float[]} of the specified array length. When the
 * arrays are no longer needed, you should call {@link #free()}, so that e.g.
 * they will be reclaimed.
 * 
 * <p>
 * <b>NOTE:</b> if you need to reuse the allocated arrays between search
 * requests, use {@link ReusingFacetArrays}.
 * 
 * <p>
 * <b>NOTE:</b> this class is not thread safe. You typically allocate it per
 * search.
 * 
 * @lucene.experimental
 */
public class FacetArrays {

  private int[] ints;
  private float[] floats;
  
  public final int arrayLength;

  /** Arrays will be allocated at the specified length. */
  public FacetArrays(int arrayLength) {
    this.arrayLength = arrayLength;
  }
  
  protected float[] newFloatArray() {
    return new float[arrayLength];
  }
  
  protected int[] newIntArray() {
    return new int[arrayLength];
  }
  
  protected void doFree(float[] floats, int[] ints) {
  }
  
  /**
   * Notifies that the arrays obtained from {@link #getIntArray()}
   * or {@link #getFloatArray()} are no longer needed and can be freed.
   */
  public final void free() {
    doFree(floats, ints);
    ints = null;
    floats = null;
  }

  public final int[] getIntArray() {
    if (ints == null) {
      ints = newIntArray();
    }
    return ints;
  }

  public final float[] getFloatArray() {
    if (floats == null) {
      floats = newFloatArray();
    }
    return floats;
  }

}
