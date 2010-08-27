/**
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

package org.apache.lucene.analysis;

import org.apache.lucene.util.ArrayUtil;

/**
 * Base utility class for implementing a {@link CharFilter}.
 * You subclass this, and then record mappings by calling
 * {@link #addOffCorrectMap}, and then invoke the correct
 * method to correct an offset.
 */
public abstract class BaseCharFilter extends CharFilter {

  private int offsets[];
  private int diffs[];
  private int size = 0;
  
  public BaseCharFilter(CharStream in) {
    super(in);
  }

  /** Retrieve the corrected offset. */
  @Override
  protected int correct(int currentOff) {
    if (offsets == null || currentOff < offsets[0]) {
      return currentOff;
    }
    
    int hi = size - 1;
    if(currentOff >= offsets[hi])
      return currentOff + diffs[hi];

    int lo = 0;
    int mid = -1;
    
    while (hi >= lo) {
      mid = (lo + hi) >>> 1;
      if (currentOff < offsets[mid])
        hi = mid - 1;
      else if (currentOff > offsets[mid])
        lo = mid + 1;
      else
        return currentOff + diffs[mid];
    }

    if (currentOff < offsets[mid])
      return mid == 0 ? currentOff : currentOff + diffs[mid-1];
    else
      return currentOff + diffs[mid];
  }
  
  protected int getLastCumulativeDiff() {
    return offsets == null ?
      0 : diffs[size-1];
  }

  protected void addOffCorrectMap(int off, int cumulativeDiff) {
    if (offsets == null) {
      offsets = new int[64];
      diffs = new int[64];
    } else if (size == offsets.length) {
      offsets = ArrayUtil.grow(offsets);
      diffs = ArrayUtil.grow(diffs);
    }
    
    offsets[size] = off;
    diffs[size++] = cumulativeDiff; 
  }
}
