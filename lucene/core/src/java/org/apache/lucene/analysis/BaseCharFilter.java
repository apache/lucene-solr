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

import java.util.Arrays;

/**
 * <p>
 *   Base utility class for implementing a {@link CharFilter}.
 *   You subclass this, and then record mappings by calling
 *   {@link #addOffCorrectMap}, and then invoke the correct
 *   method to correct an offset.
 * </p>
 + <p>
 +   CharFilters modify an input stream via a series of substring
 +   replacements (including deletions and insertions) to produce an output
 +   stream. There are three possible replacement cases: the replacement
 +   string has the same length as the original substring; the replacement
 +   is shorter; and the replacement is longer. In the latter two cases
 +   (when the replacement has a different length than the original),
 +   one or more offset correction mappings are required.
 + </p>
 + <p>
 +   When the replacement is shorter than the original (e.g. when the
 +   replacement is the empty string), a single offset correction mapping
 +   should be added at the replacement's end offset in the output stream.
 +   The <code>cumulativeDiff</code> parameter to the
 +   <code>addOffCorrectMapping()</code> method will be the sum of all
 +   previous replacement offset adjustments, with the addition of the
 +   difference between the lengths of the original substring and the
 +   replacement string (a positive value).
 + </p>
 + <p>
 +   When the replacement is longer than the original (e.g. when the
 +   original is the empty string), you should add as many offset
 +   correction mappings as the difference between the lengths of the
 +   replacement string and the original substring, starting at the
 +   end offset the original substring would have had in the output stream.
 +   The <code>cumulativeDiff</code> parameter to the
 +   <code>addOffCorrectMapping()</code> method will be the sum of all
 +   previous replacement offset adjustments, with the addition of the
 +   difference between the lengths of the original substring and the
 +   replacement string so far (a negative value).
 + </p>
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

  /**
   * <p>
   *   Adds an offset correction mapping at the given output stream offset.
   * </p>
   * <p>
   *   Assumption: the offset given with each successive call to this method
   *   will not be smaller than the offset given at the previous invocation.
   * </p>
   *
   * @param off The output stream offset at which to apply the correction
   * @param cumulativeDiff The input offset is given by adding this
   *                       to the output offset
   */
  protected void addOffCorrectMap(int off, int cumulativeDiff) {
    if (offsets == null) {
      offsets = new int[64];
      diffs = new int[64];
    } else if (size == offsets.length) {
      offsets = ArrayUtil.grow(offsets);
      diffs = ArrayUtil.grow(diffs);
    }
    
    assert (size == 0 || off >= offsets[size])
        : "Offset #" + size + "(" + off + ") is less than the last recorded offset "
          + offsets[size] + "\n" + Arrays.toString(offsets) + "\n" + Arrays.toString(diffs);
    
    if (size == 0 || off != offsets[size - 1]) {
      offsets[size] = off;
      diffs[size++] = cumulativeDiff;
    } else { // Overwrite the diff at the last recorded offset
      diffs[size - 1] = cumulativeDiff;
    }
  }
}
