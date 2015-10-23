package org.apache.lucene.util.bkd;

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

import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.DimensionalValues.IntersectVisitor;
import org.apache.lucene.index.DimensionalValues.Relation;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;

/** Handles intersection of an multi-dimensional shape in byte[] space with a block KD-tree previously written with {@link BKDWriter}.
 *
 * @lucene.experimental */

public class BKDReader implements Accountable {
  // Packed array of byte[] holding all split values in the full binary tree:
  final private byte[] splitPackedValues; 
  final private long[] leafBlockFPs;
  final private int leafNodeOffset;
  final int numDims;
  final int bytesPerDim;
  final IndexInput in;
  final int maxPointsInLeafNode;
  protected final int packedBytesLength;

  /** Caller must pre-seek the provided {@link IndexInput} to the index location that {@link BKDWriter#finish} returned */
  public BKDReader(IndexInput in) throws IOException {
    CodecUtil.checkHeader(in, BKDWriter.CODEC_NAME, BKDWriter.VERSION_START, BKDWriter.VERSION_START);
    numDims = in.readVInt();
    maxPointsInLeafNode = in.readVInt();
    bytesPerDim = in.readVInt();
    packedBytesLength = numDims * bytesPerDim;

    // Read index:
    int numLeaves = in.readVInt();
    leafNodeOffset = numLeaves;

    splitPackedValues = new byte[(1+bytesPerDim)*numLeaves];
    in.readBytes(splitPackedValues, 0, splitPackedValues.length);

    // Tree is fully balanced binary tree, so number of nodes = numLeaves-1, except our nodeIDs are 1-based (splitPackedValues[0] is unused):
    leafBlockFPs = new long[numLeaves];
    for(int i=0;i<numLeaves;i++) {
      leafBlockFPs[i] = in.readVLong();
    }

    this.in = in;
  }

  protected BKDReader(IndexInput in, int numDims, int maxPointsInLeafNode, int bytesPerDim, long[] leafBlockFPs, byte[] splitPackedValues) throws IOException {
    this.in = in;
    this.numDims = numDims;
    this.maxPointsInLeafNode = maxPointsInLeafNode;
    this.bytesPerDim = bytesPerDim;
    packedBytesLength = numDims * bytesPerDim;
    this.leafNodeOffset = leafBlockFPs.length;
    this.leafBlockFPs = leafBlockFPs;
    this.splitPackedValues = splitPackedValues;
  }

  private static final class IntersectState {
    final IndexInput in;
    final int[] scratchDocIDs;
    final byte[] scratchPackedValue;

    // Minimum point of the N-dim rect containing the query shape:
    final byte[] minPacked;
    // Maximum point of the N-dim rect containing the query shape:
    final byte[] maxPacked;
    final IntersectVisitor visitor;

    public IntersectState(IndexInput in, int packedBytesLength,
                          int maxPointsInLeafNode, byte[] minPacked, byte[] maxPacked,
                          IntersectVisitor visitor) {
      this.in = in;
      this.minPacked = minPacked;
      this.maxPacked = maxPacked;
      this.visitor = visitor;
      this.scratchDocIDs = new int[maxPointsInLeafNode];
      this.scratchPackedValue = new byte[packedBytesLength];
    }
  }

  public void intersect(IntersectVisitor visitor) throws IOException {
    byte[] minPacked = new byte[packedBytesLength];
    byte[] maxPacked = new byte[packedBytesLength];
    Arrays.fill(maxPacked, (byte) 0xff);
    intersect(minPacked, maxPacked, visitor);
  }

  // nocommit remove this one?  caller can "optimize" a containing bbox case itself in its compare method?
  public void intersect(byte[] minPacked, byte[] maxPacked, IntersectVisitor visitor) throws IOException {
    IntersectState state = new IntersectState(in.clone(), packedBytesLength,
                                              maxPointsInLeafNode, minPacked, maxPacked,
                                              visitor);
    byte[] rootMinPacked = new byte[packedBytesLength];
    byte[] rootMaxPacked = new byte[packedBytesLength];
    Arrays.fill(rootMaxPacked, (byte) 0xff);
    intersect(state, 1, rootMinPacked, rootMaxPacked);
  }

  /** Fast path: this is called when the query box fully encompasses all cells under this node. */
  private void addAll(IntersectState state, int nodeID) throws IOException {
    //System.out.println("R: addAll nodeID=" + nodeID);

    if (nodeID >= leafNodeOffset) {
      visitDocIDs(state.in, leafBlockFPs[nodeID-leafNodeOffset], state.visitor);
    } else {
      addAll(state, 2*nodeID);
      addAll(state, 2*nodeID+1);
    }
  }

  protected void visitDocIDs(IndexInput in, long blockFP, IntersectVisitor visitor) throws IOException {
    // Leaf node
    in.seek(blockFP);
      
    // How many points are stored in this leaf cell:
    int count = in.readVInt();

    // TODO: especially for the 1D case, this was a decent speedup, because caller could know it should budget for around XXX docs:
    //state.docs.grow(count);
    int docID = 0;
    for(int i=0;i<count;i++) {
      docID += in.readVInt();
      visitor.visit(docID);
    }
  }

  protected int readDocIDs(IndexInput in, long blockFP, int[] docIDs) throws IOException {
    in.seek(blockFP);

    // How many points are stored in this leaf cell:
    int count = in.readVInt();

    // TODO: we could maybe pollute the IntersectVisitor API with a "grow" method if this maybe helps perf
    // enough (it did before, esp. for the 1D case):
    //state.docs.grow(count);
    int docID = 0;
    for(int i=0;i<count;i++) {
      docID += in.readVInt();
      docIDs[i] = docID;
    }

    return count;
  }

  protected void visitDocValues(byte[] scratchPackedValue, IndexInput in, int[] docIDs, int count, IntersectVisitor visitor) throws IOException {
    for(int i=0;i<count;i++) {
      in.readBytes(scratchPackedValue, 0, scratchPackedValue.length);
      visitor.visit(docIDs[i], scratchPackedValue);
    }
  }

  private void intersect(IntersectState state,
                         int nodeID,
                         byte[] cellMinPacked, byte[] cellMaxPacked)
    throws IOException {

    /*
    System.out.println("\nR: intersect nodeID=" + nodeID);
    for(int dim=0;dim<numDims;dim++) {
      System.out.println("  dim=" + dim + "\n    cellMin=" + new BytesRef(cellMinPacked, dim*bytesPerDim, bytesPerDim) + "\n    cellMax=" + new BytesRef(cellMaxPacked, dim*bytesPerDim, bytesPerDim));
    }
    */

    // Optimization: only check the visitor when the current cell does not fully contain the bbox.  E.g. if the
    // query is a small area around London, UK, most of the high nodes in the BKD tree as we recurse will fully
    // contain the query, so we quickly recurse down until the nodes cross the query:
    boolean cellContainsQuery = BKDUtil.contains(bytesPerDim,
                                                 cellMinPacked, cellMaxPacked,
                                                 state.minPacked, state.maxPacked);

    if (cellContainsQuery == false) {

      Relation r = state.visitor.compare(cellMinPacked, cellMaxPacked);

      if (r == Relation.QUERY_OUTSIDE_CELL) {
        // This cell is fully outside of the query shape: stop recursing
        return;
      } else if (r == Relation.CELL_INSIDE_QUERY) {
        // This cell is fully inside of the query shape: recursively add all points in this cell without filtering
        addAll(state, nodeID);
        return;
      } else {
        // The cell crosses the shape boundary, so we fall through and do full filtering
      }
    }

    if (nodeID >= leafNodeOffset) {
      // Leaf node; scan and filter all points in this block:
      int count = readDocIDs(state.in, leafBlockFPs[nodeID-leafNodeOffset], state.scratchDocIDs);

      // Again, this time reading values and checking with the visitor
      visitDocValues(state.scratchPackedValue, state.in, state.scratchDocIDs, count, state.visitor);

    } else {
      
      // Non-leaf node: recurse on the split left and right nodes

      int address = nodeID * (bytesPerDim+1);
      int splitDim = splitPackedValues[address] & 0xff;
      assert splitDim < numDims;

      // TODO: can we alloc & reuse this up front?
      byte[] splitValue = new byte[bytesPerDim];
      System.arraycopy(splitPackedValues, address+1, splitValue, 0, bytesPerDim);

      // TODO: can we alloc & reuse this up front?
      byte[] splitPackedValue = new byte[packedBytesLength];

      if (BKDUtil.compare(bytesPerDim, state.minPacked, splitDim, splitValue, 0) <= 0) {
        // The query bbox overlaps our left cell, so we must recurse:
        System.arraycopy(state.maxPacked, 0, splitPackedValue, 0, packedBytesLength);
        System.arraycopy(splitValue, 0, splitPackedValue, splitDim*bytesPerDim, bytesPerDim);
        intersect(state,
                  2*nodeID,
                  cellMinPacked, splitPackedValue);
      }

      if (BKDUtil.compare(bytesPerDim, state.maxPacked, splitDim, splitValue, 0) >= 0) {
        // The query bbox overlaps our left cell, so we must recurse:
        System.arraycopy(state.minPacked, 0, splitPackedValue, 0, packedBytesLength);
        System.arraycopy(splitValue, 0, splitPackedValue, splitDim*bytesPerDim, bytesPerDim);
        intersect(state,
                  2*nodeID+1,
                  splitPackedValue, cellMaxPacked);
      }
    }
  }

  @Override
  public long ramBytesUsed() {
    return splitPackedValues.length +
      leafBlockFPs.length * RamUsageEstimator.NUM_BYTES_LONG;
  }
}
