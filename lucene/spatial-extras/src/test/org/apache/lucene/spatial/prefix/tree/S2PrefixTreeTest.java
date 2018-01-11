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

package org.apache.lucene.spatial.prefix.tree;

import com.carrotsearch.randomizedtesting.annotations.Repeat;
import com.google.common.geometry.S2CellId;
import org.apache.lucene.spatial.spatial4j.Geo3dSpatialContextFactory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.Test;

/**
 * Test for S2 Spatial prefix tree.
 */
public class S2PrefixTreeTest extends LuceneTestCase{

  @Test
  @Repeat(iterations = 100)
  public void testCells() {
    int face = random().nextInt(6);
    S2CellId id = S2CellId.fromFacePosLevel(face, 0, 0);
    int level = random().nextInt(30);
    for (int i = 0; i < level; i++) {
      int pos = random().nextInt(4);
      id = id.childBegin();
      if (pos == 0) continue;
      id = id.next();
      if (pos == 1) continue;
      id = id.next();
      if (pos == 2) continue;
      id = id.next();
    }
    S2PrefixTree tree = new S2PrefixTree(new Geo3dSpatialContextFactory().newSpatialContext(), S2PrefixTree.MAX_LEVELS);
    S2PrefixTreeCell cell = new S2PrefixTreeCell(tree, id);
    BytesRef ref = cell.getTokenBytesWithLeaf(null);
    S2PrefixTreeCell cell2 = new S2PrefixTreeCell(tree, S2CellId.none());
    cell2.readCell(tree, ref);
    assertEquals(cell, cell2);
  }

  @Test
  @Repeat(iterations = 10)
  public void testDistanceAndLevels() {
    S2PrefixTree tree = new S2PrefixTree(new Geo3dSpatialContextFactory().newSpatialContext(), S2PrefixTree.MAX_LEVELS);
    int level = random().nextInt(31) + 1;
    double distance = tree.getDistanceForLevel(level);
    assertEquals(level, tree.getLevelForDistance(distance));
  }

}