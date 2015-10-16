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
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;

// nocommit rename generic stuff (Util, Reader, Writer) w/ BKD prefix even though they are package private

// nocommit the try/finally/delete-stuff is frustrating

/** Handles intersection of an multi-dimensional shape in byte[] space with a block KD-tree previously written with {@link BKDWriter}.
 *
 * @lucene.experimental */

public final class BKDReader implements Accountable {
  // Packed array of byte[] holding all split values in the full binary tree:
  final private byte[] splitPackedValues; 
  final private long[] leafBlockFPs;
  final private int leafNodeOffset;
  final int numDims;
  final int bytesPerDim;
  final IndexInput in;
  final int packedBytesLength;
  final int maxPointsInLeafNode;

  enum Relation {CELL_INSIDE_QUERY, QUERY_CROSSES_CELL, QUERY_OUTSIDE_CELL};

  /** We recurse the BKD tree, using a provided instance of this to guide the recursion.
   *
   * @lucene.experimental */
  public interface IntersectVisitor {
    /** Called for all docs in a leaf cell that's fully contained by the query.  The
     *  consumer should blindly accept the docID. */
    void visit(int docID);

    /** Called for all docs in a leaf cell that crosses the query.  The consumer
     *  should scrutinize the packedValue to decide whether to accept it. */
    void visit(int docID, byte[] packedValue);

    /** Called for non-leaf cells to test how the cell relates to the query, to
     *  determine how to further recurse down the treer. */
    Relation compare(byte[] minPacked, byte[] maxPacked);
  }

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
      //System.out.println("R:   leaf");

      // Leaf node
      state.in.seek(leafBlockFPs[nodeID-leafNodeOffset]);
      
      // How many points are stored in this leaf cell:
      int count = state.in.readVInt();

      // TODO: especially for the 1D case, this was a decent speedup, because caller could know it should budget for around XXX docs:
      //state.docs.grow(count);
      int docID = 0;
      for(int i=0;i<count;i++) {
        docID += state.in.readVInt();
        state.visitor.visit(docID);
      }
    } else {
      addAll(state, 2*nodeID);
      addAll(state, 2*nodeID+1);
    }
  }

  private void intersect(IntersectState state,
                        int nodeID,
                        byte[] cellMinPacked, byte[] cellMaxPacked)
    throws IOException {

    //System.out.println("\nR: intersect nodeID=" + nodeID + " cellMin=" + BKDUtil.bytesToInt(cellMinPacked, 0) + " cellMax=" + BKDUtil.bytesToInt(cellMaxPacked, 0));

    // Optimization: only check the visitor when the current cell does not fully contain the bbox.  E.g. if the
    // query is a small area around London, UK, most of the high nodes in the BKD tree as we recurse will fully
    // contain the query, so we quickly recurse down until the nodes cross the query:
    boolean cellContainsQuery = BKDUtil.contains(bytesPerDim,
                                                 cellMinPacked, cellMaxPacked,
                                                 state.minPacked, state.maxPacked);

    //System.out.println("R: cellContainsQuery=" + cellContainsQuery);

    if (cellContainsQuery == false) {

      Relation r = state.visitor.compare(cellMinPacked, cellMaxPacked);
      //System.out.println("R: relation=" + r);

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
      //System.out.println("    intersect leaf nodeID=" + nodeID + " vs leafNodeOffset=" + leafNodeOffset + " fp=" + leafBlockFPs[nodeID-leafNodeOffset]);

      state.in.seek(leafBlockFPs[nodeID-leafNodeOffset]);

      // How many points are stored in this leaf cell:
      int count = state.in.readVInt();

      // nocommit can we get this back?
      //state.docs.grow(count);
      int docID = 0;
      for(int i=0;i<count;i++) {
        docID += state.in.readVInt();
        state.scratchDocIDs[i] = docID;
      }

      // Again, this time reading values and checking with the visitor
      for(int i=0;i<count;i++) {
        state.in.readBytes(state.scratchPackedValue, 0, state.scratchPackedValue.length);
        state.visitor.visit(state.scratchDocIDs[i], state.scratchPackedValue);
      }

    } else {
      
      // Non-leaf node: recurse on the split left and right nodes

      int address = nodeID * (bytesPerDim+1);
      int splitDim = splitPackedValues[address] & 0xff;
      assert splitDim < numDims;

      // nocommit can we alloc & reuse this up front?
      byte[] splitValue = new byte[bytesPerDim];
      System.arraycopy(splitPackedValues, address+1, splitValue, 0, bytesPerDim);

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
