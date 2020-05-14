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

import org.apache.lucene.util.BytesRef;

/**
 * Serializes a KD tree in a index.
 *
 * @lucene.experimental */
public interface BKDIndexWriter {

  /** writes a leaf block in the index */
  void writeLeafBlock(BKDConfig config, BKDLeafBlock leafBlock,
                      int[] commonPrefixes, int sortedDim, int leafCardinality) throws IOException;


  /** writes inner nodes in the index */
  void writeIndex(BKDConfig config, BKDTreeLeafNodes leafNodes,
                  byte[] minPackedValue, byte[] maxPackedValue, long pointCount, int numberDocs) throws IOException;

  /** return the current position of the index */
  long getFilePointer();


  static int getNumLeftLeafNodes(int numLeaves) {
    assert numLeaves > 1: "getNumLeftLeaveNodes() called with " + numLeaves;
    // return the level that can be filled with this number of leaves
    int lastFullLevel = 31 - Integer.numberOfLeadingZeros(numLeaves);
    // how many leaf nodes are in the full level
    int leavesFullLevel = 1 << lastFullLevel;
    // half of the leaf nodes from the full level goes to the left
    int numLeftLeafNodes = leavesFullLevel / 2;
    // leaf nodes that do not fit in the full level
    int unbalancedLeafNodes = numLeaves - leavesFullLevel;
    // distribute unbalanced leaf nodes
    numLeftLeafNodes += Math.min(unbalancedLeafNodes, numLeftLeafNodes);
    // we should always place unbalanced leaf nodes on the left
    assert numLeftLeafNodes >= numLeaves - numLeftLeafNodes && numLeftLeafNodes <= 2L * (numLeaves - numLeftLeafNodes);
    return numLeftLeafNodes;
  }

  /** flat representation of a kd-tree */
  interface BKDTreeLeafNodes {
    /** number of leaf nodes */
    int numLeaves();
    /** pointer to the leaf node previously written. Leaves are order from
     * left to right, so leaf at {@code index} 0 is the leftmost leaf and
     * the the leaf at {@code numleaves()} -1 is the rightmost leaf */
    long getLeafLP(int index);
    /** split value between two leaves. The split value at position n corresponds to the
     *  leaves at (n -1) and n. */
    BytesRef getSplitValue(int index);
    /** split dimension between two leaves. The split dimension at position n corresponds to the
     *  leaves at (n -1) and n.*/
    int getSplitDimension(int index);
  }

  /** It represents a leaf block on thr BKD tree.*/
  interface BKDLeafBlock {

    /** number of points on the leaf */
    int count();

    /** the point values of this leaf at a given position packed
     * on a BytesRef */
    BytesRef packedValue(int position);

    /** the docId of this block at a given position */
    int docId(int position);
  }
}
