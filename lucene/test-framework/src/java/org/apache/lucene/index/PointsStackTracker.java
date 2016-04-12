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

package org.apache.lucene.index;

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.index.PointValues.IntersectVisitor;
import org.apache.lucene.util.StringHelper;

/** Simple utility class to track the current BKD stack based solely on calls to {@link IntersectVisitor#compare}. */
public class PointsStackTracker {

  private final int numDims;
  private final int bytesPerDim;

  public final List<Cell> stack = new ArrayList<>();

  public class Cell {
    public final byte[] minPackedValue;
    public final byte[] maxPackedValue;

    public Cell(byte[] minPackedValue, byte[] maxPackedValue) {
      this.minPackedValue = minPackedValue.clone();
      this.maxPackedValue = maxPackedValue.clone();
    }

    public boolean contains(Cell other) {
      for(int dim=0;dim<numDims;dim++) {
        int offset = dim * bytesPerDim;
        // other.min < min?
        if (StringHelper.compare(bytesPerDim, other.minPackedValue, offset, minPackedValue, offset) < 0) {
          return false;
        }
        // other.max > max?
        if (StringHelper.compare(bytesPerDim, other.maxPackedValue, offset, maxPackedValue, offset) > 0) {
          return false;
        }
      }

      return true;
    }
  }

  public PointsStackTracker(int numDims, int bytesPerDim) {
    this.numDims = numDims;
    this.bytesPerDim = bytesPerDim;
  }
    
  public void onCompare(byte[] minPackedValue, byte[] maxPackedValue) {
    Cell cell = new Cell(minPackedValue, maxPackedValue);

    // Pop stack:
    while (stack.size() > 0 && stack.get(stack.size()-1).contains(cell) == false) {
      stack.remove(stack.size()-1);
      //System.out.println("  pop");
    }

    // Push stack:
    stack.add(cell);
  }

  // TODO: expose other details about the stack...
}
