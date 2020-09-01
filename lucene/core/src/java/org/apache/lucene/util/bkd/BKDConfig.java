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

import org.apache.lucene.util.ArrayUtil;

/**
 * Basic parameters for indexing points on the BKD tree.
 */
public final class BKDConfig {

  /** Default maximum number of point in each leaf block */
  public static final int DEFAULT_MAX_POINTS_IN_LEAF_NODE = 512;

  /** Maximum number of index dimensions (2 * max index dimensions) */
  public static final int MAX_DIMS = 16;

  /** Maximum number of index dimensions */
  public static final int MAX_INDEX_DIMS = 8;

  /** How many dimensions we are storing at the leaf (data) nodes */
  public final int numDims;

  /** How many dimensions we are indexing in the internal nodes */
  public final int numIndexDims;

  /** How many bytes each value in each dimension takes. */
  public final int bytesPerDim;

  /** max points allowed on a Leaf block */
  public final int maxPointsInLeafNode;

  /** numDataDims * bytesPerDim */
  public final int packedBytesLength;

  /** numIndexDims * bytesPerDim */
  public final int packedIndexBytesLength;

  /** packedBytesLength plus docID size */
  public final int bytesPerDoc;

  public BKDConfig(final int numDims, final int numIndexDims, final int bytesPerDim, final int maxPointsInLeafNode) {
    verifyParams(numDims, numIndexDims, bytesPerDim, maxPointsInLeafNode);
    this.numDims = numDims;
    this.numIndexDims = numIndexDims;
    this.bytesPerDim = bytesPerDim;
    this.maxPointsInLeafNode = maxPointsInLeafNode;
    this.packedIndexBytesLength = numIndexDims * bytesPerDim;
    this.packedBytesLength = numDims * bytesPerDim;
    // dimensional values (numDims * bytesPerDim) + docID (int)
    this.bytesPerDoc = this.packedBytesLength + Integer.BYTES;
  }

  private static void verifyParams(final int numDims, final int numIndexDims, final int bytesPerDim, final int maxPointsInLeafNode) {
    // Check inputs are on bounds
    if (numDims < 1 || numDims > MAX_DIMS) {
      throw new IllegalArgumentException("numDims must be 1 .. " + MAX_DIMS + " (got: " + numDims + ")");
    }
    if (numIndexDims < 1 || numIndexDims > MAX_INDEX_DIMS) {
      throw new IllegalArgumentException("numIndexDims must be 1 .. " + MAX_INDEX_DIMS + " (got: " + numIndexDims + ")");
    }
    if (numIndexDims > numDims) {
      throw new IllegalArgumentException("numIndexDims cannot exceed numDims (" + numDims + ") (got: " + numIndexDims + ")");
    }
    if (bytesPerDim <= 0) {
      throw new IllegalArgumentException("bytesPerDim must be > 0; got " + bytesPerDim);
    }
    if (maxPointsInLeafNode <= 0) {
      throw new IllegalArgumentException("maxPointsInLeafNode must be > 0; got " + maxPointsInLeafNode);
    }
    if (maxPointsInLeafNode > ArrayUtil.MAX_ARRAY_LENGTH) {
      throw new IllegalArgumentException("maxPointsInLeafNode must be <= ArrayUtil.MAX_ARRAY_LENGTH (= " + ArrayUtil.MAX_ARRAY_LENGTH + "); got " + maxPointsInLeafNode);
    }
  }
}