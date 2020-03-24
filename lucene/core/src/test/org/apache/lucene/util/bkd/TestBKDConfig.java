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
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

public class TestBKDConfig extends LuceneTestCase {

  public void testCreate() {
    int numIndexDims = TestUtil.nextInt(random(), 1, BKDConfig.MAX_INDEX_DIMS);
    int numDims = TestUtil.nextInt(random(), numIndexDims, BKDConfig.MAX_DIMS);
    int bytesPerDim = TestUtil.nextInt(random(), 1, 32);
    int maxPointsInLeafNode = TestUtil.nextInt(random(), 1, Integer.MAX_VALUE);
    BKDConfig config = new BKDConfig(numDims, numIndexDims, bytesPerDim, maxPointsInLeafNode);
    assertEquals(numDims, config.numDims);
    assertEquals(numIndexDims, config.numIndexDims);
    assertEquals(bytesPerDim, config.bytesPerDim);
    assertEquals(maxPointsInLeafNode, config.maxPointsInLeafNode);
    assertEquals(numDims * bytesPerDim, config.packedBytesLength);
    assertEquals(numIndexDims * bytesPerDim, config.packedIndexBytesLength);
    assertEquals(numDims * bytesPerDim + Integer.BYTES, config.bytesPerDoc);
  }

  public void testTooManyDimensions() {
    int numIndexDims = TestUtil.nextInt(random(), 1, BKDConfig.MAX_INDEX_DIMS);
    int numDims = TestUtil.nextInt(random(), BKDConfig.MAX_DIMS + 1, Integer.MAX_VALUE);
    int bytesPerDim = TestUtil.nextInt(random(), 1, 32);
    int maxPointsInLeafNode = TestUtil.nextInt(random(), 1, Integer.MAX_VALUE);
    IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> { new BKDConfig(numDims, numIndexDims, bytesPerDim, maxPointsInLeafNode);});
    assertEquals("numDims must be 1 .. " + BKDConfig.MAX_DIMS + " (got: " + numDims + ")", ex.getMessage());
  }

  public void testTooManyIndexDimensions() {
    int numIndexDims = TestUtil.nextInt(random(), BKDConfig.MAX_INDEX_DIMS + 1, BKDConfig.MAX_DIMS);
    int numDims = TestUtil.nextInt(random(), numIndexDims, BKDConfig.MAX_DIMS);
    int bytesPerDim = TestUtil.nextInt(random(), 1, 32);
    int maxPointsInLeafNode = TestUtil.nextInt(random(), 1, Integer.MAX_VALUE);
    IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> { new BKDConfig(numDims, numIndexDims, bytesPerDim, maxPointsInLeafNode);});
    assertEquals("numIndexDims must be 1 .. " + BKDConfig.MAX_INDEX_DIMS + " (got: " + numIndexDims + ")", ex.getMessage());
  }

  public void testBadDimensions() {
    int numIndexDims = TestUtil.nextInt(random(), 2, BKDConfig.MAX_INDEX_DIMS);
    int numDims = TestUtil.nextInt(random(), 1, numIndexDims - 1);
    int bytesPerDim = TestUtil.nextInt(random(), 1, 32);
    int maxPointsInLeafNode = TestUtil.nextInt(random(), 1, Integer.MAX_VALUE);
    IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> { new BKDConfig(numDims, numIndexDims, bytesPerDim, maxPointsInLeafNode);});
    assertEquals("numIndexDims cannot exceed numDims (" + numDims + ") (got: " + numIndexDims + ")", ex.getMessage());
  }

  public void testNegativeOrZeroMaxPointsOnLeaf() {
    int numIndexDims = TestUtil.nextInt(random(), 1, BKDConfig.MAX_INDEX_DIMS);
    int numDims = TestUtil.nextInt(random(), numIndexDims, BKDConfig.MAX_DIMS);
    int bytesPerDim = TestUtil.nextInt(random(), 1, 32);
    int maxPointsInLeafNode = TestUtil.nextInt(random(), Integer.MIN_VALUE, 0);
    IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> { new BKDConfig(numDims, numIndexDims, bytesPerDim, maxPointsInLeafNode);});
    assertEquals("maxPointsInLeafNode must be > 0; got " + maxPointsInLeafNode, ex.getMessage());
  }

  public void testTooManyMaxPointsOnLeaf() {
    int numIndexDims = TestUtil.nextInt(random(), 1, BKDConfig.MAX_INDEX_DIMS);
    int numDims = TestUtil.nextInt(random(), numIndexDims, BKDConfig.MAX_DIMS);
    int bytesPerDim = TestUtil.nextInt(random(), 1, 32);
    int maxPointsInLeafNode = TestUtil.nextInt(random(), ArrayUtil.MAX_ARRAY_LENGTH + 1, Integer.MAX_VALUE);
    IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> { new BKDConfig(numDims, numIndexDims, bytesPerDim, maxPointsInLeafNode);});
    assertEquals("maxPointsInLeafNode must be <= ArrayUtil.MAX_ARRAY_LENGTH (= " + ArrayUtil.MAX_ARRAY_LENGTH + "); got " + maxPointsInLeafNode, ex.getMessage());
  }
}
