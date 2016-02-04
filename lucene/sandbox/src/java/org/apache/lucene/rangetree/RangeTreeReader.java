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
package org.apache.lucene.rangetree;

import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.DocIdSetBuilder;
import org.apache.lucene.util.RamUsageEstimator;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

/** Handles intersection of a range with a numeric tree previously written with {@link RangeTreeWriter}.
 *
 * @lucene.experimental */

final class RangeTreeReader implements Accountable {
  final private long[] blockFPs;
  final private long[] blockMinValues;
  final IndexInput in;
  final long globalMaxValue;
  final int approxDocsPerBlock;

  public RangeTreeReader(IndexInput in) throws IOException {

    // Read index:
    int numLeaves = in.readVInt();
    approxDocsPerBlock = in.readVInt();

    blockMinValues = new long[numLeaves];
    for(int i=0;i<numLeaves;i++) {
      blockMinValues[i] = in.readLong();
    }
    blockFPs = new long[numLeaves];
    for(int i=0;i<numLeaves;i++) {
      blockFPs[i] = in.readVLong();
    }
    globalMaxValue = in.readLong();

    this.in = in;
  }

  public long getMinValue() {
    return blockMinValues[0];
  }

  public long getMaxValue() {
    return globalMaxValue;
  }

  private static final class QueryState {
    final IndexInput in;
    final DocIdSetBuilder docs;
    final long minValueIncl;
    final long maxValueIncl;
    final SortedNumericDocValues sndv;

    public QueryState(IndexInput in, int maxDoc,
                      long minValueIncl, long maxValueIncl,
                      SortedNumericDocValues sndv) {
      this.in = in;
      this.docs = new DocIdSetBuilder(maxDoc);
      this.minValueIncl = minValueIncl;
      this.maxValueIncl = maxValueIncl;
      this.sndv = sndv;
    }
  }

  public DocIdSet intersect(long minIncl, long maxIncl, SortedNumericDocValues sndv, int maxDoc) throws IOException {

    if (minIncl > maxIncl) {
      return DocIdSet.EMPTY;
    }

    if (minIncl > globalMaxValue || maxIncl < blockMinValues[0]) {
      return DocIdSet.EMPTY;
    }

    QueryState state = new QueryState(in.clone(), maxDoc,
                                      minIncl, maxIncl,
                                      sndv);

    int startBlockIncl = Arrays.binarySearch(blockMinValues, minIncl);
    if (startBlockIncl >= 0) {
      // There can be dups here, when the same value is added many
      // times.  Also, we need the first block whose min is < minIncl:
      while (startBlockIncl > 0 && blockMinValues[startBlockIncl] == minIncl) {
        startBlockIncl--;
      }
    } else {
      startBlockIncl = Math.max(-startBlockIncl-2, 0);
    }

    int endBlockIncl = Arrays.binarySearch(blockMinValues, maxIncl);
    if (endBlockIncl >= 0) {
      // There can be dups here, when the same value is added many
      // times.  Also, we need the first block whose max is > minIncl:
      while (endBlockIncl < blockMinValues.length-1 && blockMinValues[endBlockIncl] == maxIncl) {
        endBlockIncl++;
      }
    } else {
      endBlockIncl = Math.max(-endBlockIncl-2, 0);
    }

    assert startBlockIncl <= endBlockIncl;

    state.in.seek(blockFPs[startBlockIncl]);

    //System.out.println("startBlockIncl=" + startBlockIncl + " endBlockIncl=" + endBlockIncl);

    // Rough estimate of how many hits we'll see.  Note that in the degenerate case
    // (index same value many times) this could be a big over-estimate, but in the typical
    // case it's good:
    state.docs.grow(approxDocsPerBlock * (endBlockIncl - startBlockIncl + 1));

    int hitCount = 0;
    for (int block=startBlockIncl;block<=endBlockIncl;block++) {
      boolean doFilter = blockMinValues[block] <= minIncl || block == blockMinValues.length-1 || blockMinValues[block+1] >= maxIncl;
      //System.out.println("  block=" + block + " min=" + blockMinValues[block] + " doFilter=" + doFilter);

      int newCount;
      if (doFilter) {
        // We must filter each hit:
        newCount = addSome(state);
      } else {
        newCount = addAll(state);
      }

      hitCount += newCount;
    }

    // NOTE: hitCount is an over-estimate in the multi-valued case:
    return state.docs.build(hitCount);
  }

  /** Adds all docs from the current block. */
  private int addAll(QueryState state) throws IOException {
    // How many values are stored in this leaf cell:
    int count = state.in.readVInt();
    state.docs.grow(count);
    for(int i=0;i<count;i++) {
      int docID = state.in.readInt();
      state.docs.add(docID);
    }

    return count;
  }

  /** Adds docs from the current block, filtering each hit against the query min/max.  This
   *  is only needed on the boundary blocks. */
  private int addSome(QueryState state) throws IOException {
    int hitCount = 0;

    // How many points are stored in this leaf cell:
    int count = state.in.readVInt();
    state.docs.grow(count);
    for(int i=0;i<count;i++) {
      int docID = state.in.readInt();
      state.sndv.setDocument(docID);

      // How many values this doc has:
      int docValueCount = state.sndv.count();

      for(int j=0;j<docValueCount;j++) {
        long value = state.sndv.valueAt(j);

        if (value >= state.minValueIncl && value <= state.maxValueIncl) {
          state.docs.add(docID);
          hitCount++;

          // Stop processing values for this doc:
          break;
        }
      }
    }

    return hitCount;
  }

  @Override
  public long ramBytesUsed() {
    return blockMinValues.length * RamUsageEstimator.NUM_BYTES_LONG + 
      blockFPs.length * RamUsageEstimator.NUM_BYTES_LONG;
  }

  @Override
  public Collection<Accountable> getChildResources() {
    return Collections.emptyList();
  }
}
