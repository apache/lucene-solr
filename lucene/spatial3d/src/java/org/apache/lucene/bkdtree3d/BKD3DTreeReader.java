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
package org.apache.lucene.bkdtree3d;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.DocIdSetBuilder;
import org.apache.lucene.util.RamUsageEstimator;

/** Handles intersection of a shape with a BKD tree previously written with {@link BKD3DTreeWriter}.
 *
 * @lucene.experimental
 *
 * @deprecated Use dimensional values in Lucene 6.0 instead */
@Deprecated
final class BKD3DTreeReader implements Accountable {
  final private int[] splitValues; 
  final private int leafNodeOffset;
  final private long[] leafBlockFPs;
  final int maxDoc;
  final IndexInput in;

  enum Relation {CELL_INSIDE_SHAPE, SHAPE_CROSSES_CELL, SHAPE_OUTSIDE_CELL, SHAPE_INSIDE_CELL};

  interface ValueFilter {
    boolean accept(int docID);
    Relation compare(int cellXMin, int cellXMax, int cellYMin, int cellYMax, int cellZMin, int cellZMax);
  }

  public BKD3DTreeReader(IndexInput in, int maxDoc) throws IOException {

    // Read index:
    int numLeaves = in.readVInt();
    leafNodeOffset = numLeaves;

    // Tree is fully balanced binary tree, so number of nodes = numLeaves-1, except our nodeIDs are 1-based (splitValues[0] is unused):
    splitValues = new int[numLeaves];
    for(int i=0;i<numLeaves;i++) {
      splitValues[i] = in.readInt();
    }
    leafBlockFPs = new long[numLeaves];
    for(int i=0;i<numLeaves;i++) {
      leafBlockFPs[i] = in.readVLong();
    }

    this.maxDoc = maxDoc;
    this.in = in;
  }

  private static final class QueryState {
    final IndexInput in;
    byte[] scratch = new byte[16];
    final ByteArrayDataInput scratchReader = new ByteArrayDataInput(scratch);
    final DocIdSetBuilder docs;
    final int xMin;
    final int xMax;
    final int yMin;
    final int yMax;
    final int zMin;
    final int zMax;
    final ValueFilter valueFilter;

    public QueryState(IndexInput in, int maxDoc,
                      int xMin, int xMax,
                      int yMin, int yMax,
                      int zMin, int zMax,
                      ValueFilter valueFilter) {
      this.in = in;
      this.docs = new DocIdSetBuilder(maxDoc);
      this.xMin = xMin;
      this.xMax = xMax;
      this.yMin = yMin;
      this.yMax = yMax;
      this.zMin = zMin;
      this.zMax = zMax;
      this.valueFilter = valueFilter;
    }
  }

  public DocIdSet intersect(ValueFilter filter) throws IOException {
    return intersect(Integer.MIN_VALUE, Integer.MAX_VALUE,
                     Integer.MIN_VALUE, Integer.MAX_VALUE,
                     Integer.MIN_VALUE, Integer.MAX_VALUE,
                     filter);
  }

  /** Optimized intersect which takes the 3D bbox for the query and uses that to avoid filter.compare calls
  *   when cells are clearly outside the bbox. */
  public DocIdSet intersect(int xMin, int xMax, int yMin, int yMax, int zMin, int zMax, ValueFilter filter) throws IOException {

    QueryState state = new QueryState(in.clone(), maxDoc,
                                      xMin, xMax,
                                      yMin, yMax,
                                      zMin, zMax,
                                      filter);

    int hitCount = intersect(state, 1,
                             Integer.MIN_VALUE, Integer.MAX_VALUE,
                             Integer.MIN_VALUE, Integer.MAX_VALUE,
                             Integer.MIN_VALUE, Integer.MAX_VALUE);

    // NOTE: hitCount is an over-estimate in the multi-valued case:
    return state.docs.build(hitCount);
  }

  /** Fast path: this is called when the query rect fully encompasses all cells under this node. */
  private int addAll(QueryState state, int nodeID) throws IOException {
    //System.out.println("  addAll nodeID=" + nodeID + " leafNodeOffset=" + leafNodeOffset);

    if (nodeID >= leafNodeOffset) {

      /*
      System.out.println("A: " + BKDTreeWriter.decodeLat(cellLatMinEnc)
                         + " " + BKDTreeWriter.decodeLat(cellLatMaxEnc)
                         + " " + BKDTreeWriter.decodeLon(cellLonMinEnc)
                         + " " + BKDTreeWriter.decodeLon(cellLonMaxEnc));
      */

      // Leaf node
      long fp = leafBlockFPs[nodeID-leafNodeOffset];
      //System.out.println("    leaf fp=" + fp);
      state.in.seek(fp);
      
      //System.out.println("    seek to leafFP=" + fp);
      // How many points are stored in this leaf cell:
      int count = state.in.readVInt();
      //System.out.println("    count=" + count);
      state.docs.grow(count);
      for(int i=0;i<count;i++) {
        int docID = state.in.readInt();
        state.docs.add(docID);

        // Up above in the recursion we asked valueFilter to relate our cell, and it returned Relation.CELL_INSIDE_SHAPE
        // so all docs inside this cell better be accepted by the filter:

        // NOTE: this is too anal, because we lost precision in the pack/unpack (8 bytes to 4 bytes), a point that's a bit above/below the
        // earth's surface due to that quantization may incorrectly evaluate as not inside the shape:
        // assert state.valueFilter.accept(docID);
      }

      return count;
    } else {
      int count = addAll(state, 2*nodeID);
      count += addAll(state, 2*nodeID+1);
      return count;
    }
  }

  private int intersect(QueryState state,
                        int nodeID,
                        int cellXMin, int cellXMax,
                        int cellYMin, int cellYMax,
                        int cellZMin, int cellZMax)
    throws IOException {

    //System.out.println("BKD3D.intersect nodeID=" + nodeID + " cellX=" + cellXMin + " TO " + cellXMax + ", cellY=" + cellYMin + " TO " + cellYMax + ", cellZ=" + cellZMin + " TO " + cellZMax);

    if (cellXMin >= state.xMin ||
        cellXMax <= state.xMax ||
        cellYMin >= state.yMin ||
        cellYMax <= state.yMax ||
        cellZMin >= state.zMin ||
        cellZMax <= state.zMax) {

      // Only call the filter when the current cell does not fully contain the bbox:
      Relation r = state.valueFilter.compare(cellXMin, cellXMax,
                                             cellYMin, cellYMax,
                                             cellZMin, cellZMax);
      //System.out.println("  relation: " + r);

      if (r == Relation.SHAPE_OUTSIDE_CELL) {
        // This cell is fully outside of the query shape: stop recursing
        return 0;
      } else if (r == Relation.CELL_INSIDE_SHAPE) {
        // This cell is fully inside of the query shape: recursively add all points in this cell without filtering
        
        /*
        System.out.println(Thread.currentThread() + ": switch to addAll at cell" +
                           " x=" + Geo3DDocValuesFormat.decodeValue(cellXMin) + " to " + Geo3DDocValuesFormat.decodeValue(cellXMax) +
                           " y=" + Geo3DDocValuesFormat.decodeValue(cellYMin) + " to " + Geo3DDocValuesFormat.decodeValue(cellYMax) +
                           " z=" + Geo3DDocValuesFormat.decodeValue(cellZMin) + " to " + Geo3DDocValuesFormat.decodeValue(cellZMax));
        */
        return addAll(state, nodeID);
      } else {
        // The cell crosses the shape boundary, so we fall through and do full filtering
      }
    } else {
      // The whole point of the incoming bbox (state.xMin/xMax/etc.) is that it is
      // supposed to fully enclose the shape, so this cell we are visiting, which
      // fully contains the query's bbox, better in turn fully contain the shape!
      assert state.valueFilter.compare(cellXMin, cellXMax, cellYMin, cellYMax, cellZMin, cellZMax) == Relation.SHAPE_INSIDE_CELL: "got " + state.valueFilter.compare(cellXMin, cellXMax, cellYMin, cellYMax, cellZMin, cellZMax);
    }

    //System.out.println("\nintersect node=" + nodeID + " vs " + leafNodeOffset);

    if (nodeID >= leafNodeOffset) {
      //System.out.println("  leaf");
      // Leaf node; scan and filter all points in this block:
      //System.out.println("    intersect leaf nodeID=" + nodeID + " vs leafNodeOffset=" + leafNodeOffset + " fp=" + leafBlockFPs[nodeID-leafNodeOffset]);
      int hitCount = 0;

      long fp = leafBlockFPs[nodeID-leafNodeOffset];

      /*
      System.out.println("I: " + BKDTreeWriter.decodeLat(cellLatMinEnc)
                         + " " + BKDTreeWriter.decodeLat(cellLatMaxEnc)
                         + " " + BKDTreeWriter.decodeLon(cellLonMinEnc)
                         + " " + BKDTreeWriter.decodeLon(cellLonMaxEnc));
      */

      state.in.seek(fp);

      // How many points are stored in this leaf cell:
      int count = state.in.readVInt();

      state.docs.grow(count);
      //System.out.println("  count=" + count);
      for(int i=0;i<count;i++) {
        int docID = state.in.readInt();
        //System.out.println("  check docID=" + docID);
        if (state.valueFilter.accept(docID)) {
          state.docs.add(docID);
          hitCount++;
        }
      }

      return hitCount;

    } else {

      //System.out.println("  non-leaf");

      int splitDim = BKD3DTreeWriter.getSplitDim(cellXMin, cellXMax,
                                                 cellYMin, cellYMax,
                                                 cellZMin, cellZMax);

      int splitValue = splitValues[nodeID];

      int count = 0;

      if (splitDim == 0) {

        //System.out.println("  split on lat=" + splitValue);

        // Inner node split on x:

        // Left node:
        if (state.xMin <= splitValue) {
          //System.out.println("  recurse left");
          count += intersect(state,
                             2*nodeID,
                             cellXMin, splitValue,
                             cellYMin, cellYMax,
                             cellZMin, cellZMax);
        }

        // Right node:
        if (state.xMax >= splitValue) {
          //System.out.println("  recurse right");
          count += intersect(state,
                             2*nodeID+1,
                             splitValue, cellXMax,
                             cellYMin, cellYMax,
                             cellZMin, cellZMax);
        }

      } else if (splitDim == 1) {
        // Inner node split on y:

        // System.out.println("  split on lon=" + splitValue);

        // Left node:
        if (state.yMin <= splitValue) {
          // System.out.println("  recurse left");
          count += intersect(state,
                             2*nodeID,
                             cellXMin, cellXMax,
                             cellYMin, splitValue,
                             cellZMin, cellZMax);
        }

        // Right node:
        if (state.yMax >= splitValue) {
          // System.out.println("  recurse right");
          count += intersect(state,
                             2*nodeID+1,
                             cellXMin, cellXMax,
                             splitValue, cellYMax,
                             cellZMin, cellZMax);
        }
      } else {
        // Inner node split on z:

        // System.out.println("  split on lon=" + splitValue);

        // Left node:
        if (state.zMin <= splitValue) {
          // System.out.println("  recurse left");
          count += intersect(state,
                             2*nodeID,
                             cellXMin, cellXMax,
                             cellYMin, cellYMax,
                             cellZMin, splitValue);
        }

        // Right node:
        if (state.zMax >= splitValue) {
          // System.out.println("  recurse right");
          count += intersect(state,
                             2*nodeID+1,
                             cellXMin, cellXMax,
                             cellYMin, cellYMax,
                             splitValue, cellZMax);
        }
      }

      return count;
    }
  }

  @Override
  public long ramBytesUsed() {
    return splitValues.length * RamUsageEstimator.NUM_BYTES_INT + 
      leafBlockFPs.length * RamUsageEstimator.NUM_BYTES_LONG;
  }

  @Override
  public Collection<Accountable> getChildResources() {
    return Collections.emptyList();
  }
}
