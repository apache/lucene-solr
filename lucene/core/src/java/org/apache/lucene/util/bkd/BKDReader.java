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
package org.apache.lucene.util.bkd;

import java.io.IOException;
import java.util.Arrays;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.util.BytesRef;

/**
 * A {@link PointValues} wrapper for {@link BKDIndexInput} to handle intersections.
 *
 * @lucene.experimental
 */
public final class BKDReader extends PointValues {

  final BKDIndexInput in;

  /**
   * Sole constructor
   */
  public BKDReader(BKDIndexInput in) throws IOException {
    this.in = in;
  }

  /** Create a new {@link BKDIndexInput.IndexTree} */
  public BKDIndexInput.IndexTree getIndexTree() {
    return in.getIndexTree();
  }

  @Override
  public void intersect(IntersectVisitor visitor) throws IOException {
    intersect(visitor, in.getIndexTree(), in.getMinPackedValue(), in.getMaxPackedValue());
  }

  @Override
  public long estimatePointCount(IntersectVisitor visitor) {
    return estimatePointCount(
        visitor, in.getIndexTree(), in.getMinPackedValue(), in.getMaxPackedValue());
  }

  /** Fast path: this is called when the query box fully encompasses all cells under this node. */
  private void addAll(IntersectVisitor visitor, BKDIndexInput.IndexTree index, boolean grown)
      throws IOException {
    // System.out.println("R: addAll nodeID=" + nodeID);

    if (grown == false) {
      final long maxPointCount = (long) in.getConfig().maxPointsInLeafNode * index.getNumLeaves();
      if (maxPointCount
          <= Integer.MAX_VALUE) { // could be >MAX_VALUE if there are more than 2B points in total
        visitor.grow((int) maxPointCount);
        grown = true;
      }
    }

    if (index.isLeafNode()) {
      assert grown;
      // System.out.println("ADDALL");
      if (index.nodeExists()) {
        index.visitDocIDs(visitor);
      }
      // TODO: we can assert that the first value here in fact matches what the index claimed?
    } else {
      index.pushLeft();
      addAll(visitor, index, grown);
      index.pop();

      index.pushRight();
      addAll(visitor, index, grown);
      index.pop();
    }
  }

  private void intersect(
      IntersectVisitor visitor,
      BKDIndexInput.IndexTree index,
      byte[] cellMinPacked,
      byte[] cellMaxPacked)
      throws IOException {

    /*
    System.out.println("\nR: intersect nodeID=" + state.index.getNodeID());
    for(int dim=0;dim<numDims;dim++) {
      System.out.println("  dim=" + dim + "\n    cellMin=" + new BytesRef(cellMinPacked, dim*config.bytesPerDim, config.bytesPerDim) + "\n    cellMax=" + new BytesRef(cellMaxPacked, dim*config.bytesPerDim, config.bytesPerDim));
    }
    */

    Relation r = visitor.compare(cellMinPacked, cellMaxPacked);

    if (r == Relation.CELL_OUTSIDE_QUERY) {
      // This cell is fully outside of the query shape: stop recursing
    } else if (r == Relation.CELL_INSIDE_QUERY) {
      // This cell is fully inside of the query shape: recursively add all points in this cell
      // without filtering
      addAll(visitor, index, false);
      // The cell crosses the shape boundary, or the cell fully contains the query, so we fall
      // through and do full filtering:
    } else if (index.isLeafNode()) {

      // TODO: we can assert that the first value here in fact matches what the index claimed?

      // In the unbalanced case it's possible the left most node only has one child:
      if (index.nodeExists()) {
        // Leaf node; scan and filter all points in this block:
        index.visitDocValues(visitor);
      }

    } else {

      // Non-leaf node: recurse on the split left and right nodes
      int splitDim = index.getSplitDim();
      assert splitDim >= 0
          : "splitDim=" + splitDim + ", config.numIndexDims=" + in.getConfig().numIndexDims;
      assert splitDim < in.getConfig().numIndexDims
          : "splitDim=" + splitDim + ", config.numIndexDims=" + in.getConfig().numIndexDims;

      byte[] splitPackedValue = index.getSplitPackedValue();
      BytesRef splitDimValue = index.getSplitDimValue();
      assert splitDimValue.length == in.getConfig().bytesPerDim;
      // System.out.println("  splitDimValue=" + splitDimValue + " splitDim=" + splitDim);

      // make sure cellMin <= splitValue <= cellMax:
      assert Arrays.compareUnsigned(
                  cellMinPacked,
                  splitDim * in.getConfig().bytesPerDim,
                  splitDim * in.getConfig().bytesPerDim + in.getConfig().bytesPerDim,
                  splitDimValue.bytes,
                  splitDimValue.offset,
                  splitDimValue.offset + in.getConfig().bytesPerDim)
              <= 0
          : "config.bytesPerDim="
              + in.getConfig().bytesPerDim
              + " splitDim="
              + splitDim
              + " config.numIndexDims="
              + in.getConfig().numIndexDims
              + " config.numDims="
              + in.getConfig().numDims;
      assert Arrays.compareUnsigned(
                  cellMaxPacked,
                  splitDim * in.getConfig().bytesPerDim,
                  splitDim * in.getConfig().bytesPerDim + in.getConfig().bytesPerDim,
                  splitDimValue.bytes,
                  splitDimValue.offset,
                  splitDimValue.offset + in.getConfig().bytesPerDim)
              >= 0
          : "config.bytesPerDim="
              + in.getConfig().bytesPerDim
              + " splitDim="
              + splitDim
              + " config.numIndexDims="
              + in.getConfig().numIndexDims
              + " config.numDims="
              + in.getConfig().numDims;

      // Recurse on left sub-tree:
      System.arraycopy(
          cellMaxPacked, 0, splitPackedValue, 0, in.getConfig().packedIndexBytesLength);
      System.arraycopy(
          splitDimValue.bytes,
          splitDimValue.offset,
          splitPackedValue,
          splitDim * in.getConfig().bytesPerDim,
          in.getConfig().bytesPerDim);
      index.pushLeft();
      intersect(visitor, index, cellMinPacked, splitPackedValue);
      index.pop();

      // Restore the split dim value since it may have been overwritten while recursing:
      System.arraycopy(
          splitPackedValue,
          splitDim * in.getConfig().bytesPerDim,
          splitDimValue.bytes,
          splitDimValue.offset,
          in.getConfig().bytesPerDim);

      // Recurse on right sub-tree:
      System.arraycopy(
          cellMinPacked, 0, splitPackedValue, 0, in.getConfig().packedIndexBytesLength);
      System.arraycopy(
          splitDimValue.bytes,
          splitDimValue.offset,
          splitPackedValue,
          splitDim * in.getConfig().bytesPerDim,
          in.getConfig().bytesPerDim);
      index.pushRight();
      intersect(visitor, index, splitPackedValue, cellMaxPacked);
      index.pop();
    }
  }

  private long estimatePointCount(
      IntersectVisitor visitor,
      BKDIndexInput.IndexTree index,
      byte[] cellMinPacked,
      byte[] cellMaxPacked) {

    /*
    System.out.println("\nR: intersect nodeID=" + state.index.getNodeID());
    for(int dim=0;dim<numDims;dim++) {
      System.out.println("  dim=" + dim + "\n    cellMin=" + new BytesRef(cellMinPacked, dim*config.bytesPerDim, config.bytesPerDim) + "\n    cellMax=" + new BytesRef(cellMaxPacked, dim*config.bytesPerDim, config.bytesPerDim));
    }
    */

    Relation r = visitor.compare(cellMinPacked, cellMaxPacked);

    if (r == Relation.CELL_OUTSIDE_QUERY) {
      // This cell is fully outside of the query shape: stop recursing
      return 0L;
    } else if (r == Relation.CELL_INSIDE_QUERY) {
      return (long) in.getConfig().maxPointsInLeafNode * index.getNumLeaves();
    } else if (index.isLeafNode()) {
      // Assume half the points matched
      return (in.getConfig().maxPointsInLeafNode + 1) / 2;
    } else {

      // Non-leaf node: recurse on the split left and right nodes
      int splitDim = index.getSplitDim();
      assert splitDim >= 0
          : "splitDim=" + splitDim + ", config.numIndexDims=" + in.getConfig().numIndexDims;
      assert splitDim < in.getConfig().numIndexDims
          : "splitDim=" + splitDim + ", config.numIndexDims=" + in.getConfig().numIndexDims;

      byte[] splitPackedValue = index.getSplitPackedValue();
      BytesRef splitDimValue = index.getSplitDimValue();
      assert splitDimValue.length == in.getConfig().bytesPerDim;
      // System.out.println("  splitDimValue=" + splitDimValue + " splitDim=" + splitDim);

      // make sure cellMin <= splitValue <= cellMax:
      assert Arrays.compareUnsigned(
                  cellMinPacked,
                  splitDim * in.getConfig().bytesPerDim,
                  splitDim * in.getConfig().bytesPerDim + in.getConfig().bytesPerDim,
                  splitDimValue.bytes,
                  splitDimValue.offset,
                  splitDimValue.offset + in.getConfig().bytesPerDim)
              <= 0
          : "config.bytesPerDim="
              + in.getConfig().bytesPerDim
              + " splitDim="
              + splitDim
              + " config.numIndexDims="
              + in.getConfig().numIndexDims
              + " config.numDims="
              + in.getConfig().numDims;
      assert Arrays.compareUnsigned(
                  cellMaxPacked,
                  splitDim * in.getConfig().bytesPerDim,
                  splitDim * in.getConfig().bytesPerDim + in.getConfig().bytesPerDim,
                  splitDimValue.bytes,
                  splitDimValue.offset,
                  splitDimValue.offset + in.getConfig().bytesPerDim)
              >= 0
          : "config.bytesPerDim="
              + in.getConfig().bytesPerDim
              + " splitDim="
              + splitDim
              + " config.numIndexDims="
              + in.getConfig().numIndexDims
              + " config.numDims="
              + in.getConfig().numDims;

      // Recurse on left sub-tree:
      System.arraycopy(
          cellMaxPacked, 0, splitPackedValue, 0, in.getConfig().packedIndexBytesLength);
      System.arraycopy(
          splitDimValue.bytes,
          splitDimValue.offset,
          splitPackedValue,
          splitDim * in.getConfig().bytesPerDim,
          in.getConfig().bytesPerDim);
      index.pushLeft();
      final long leftCost = estimatePointCount(visitor, index, cellMinPacked, splitPackedValue);
      index.pop();

      // Restore the split dim value since it may have been overwritten while recursing:
      System.arraycopy(
          splitPackedValue,
          splitDim * in.getConfig().bytesPerDim,
          splitDimValue.bytes,
          splitDimValue.offset,
          in.getConfig().bytesPerDim);

      // Recurse on right sub-tree:
      System.arraycopy(
          cellMinPacked, 0, splitPackedValue, 0, in.getConfig().packedIndexBytesLength);
      System.arraycopy(
          splitDimValue.bytes,
          splitDimValue.offset,
          splitPackedValue,
          splitDim * in.getConfig().bytesPerDim,
          in.getConfig().bytesPerDim);
      index.pushRight();
      final long rightCost = estimatePointCount(visitor, index, splitPackedValue, cellMaxPacked);
      index.pop();
      return leftCost + rightCost;
    }
  }

  @Override
  public byte[] getMinPackedValue() {
    return in.getMinPackedValue().clone();
  }

  @Override
  public byte[] getMaxPackedValue() {
    return in.getMaxPackedValue().clone();
  }

  @Override
  public int getNumDimensions() {
    return in.getConfig().numDims;
  }

  @Override
  public int getNumIndexDimensions() {
    return in.getConfig().numIndexDims;
  }

  @Override
  public int getBytesPerDimension() {
    return in.getConfig().bytesPerDim;
  }

  @Override
  public long size() {
    return in.getPointCount();
  }

  @Override
  public int getDocCount() {
    return in.getDocCount();
  }
}
