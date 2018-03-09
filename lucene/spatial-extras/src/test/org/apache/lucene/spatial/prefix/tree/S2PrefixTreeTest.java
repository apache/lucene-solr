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
import com.google.common.geometry.S2Projections;
import org.apache.lucene.spatial.spatial4j.Geo3dSpatialContextFactory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.Test;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.shape.Point;

/**
 * Test for S2 Spatial prefix tree.
 */
public class S2PrefixTreeTest extends LuceneTestCase{

  @Test
  @Repeat(iterations = 10)
  public void testCells() {
    int face = random().nextInt(6);
    S2CellId id = S2CellId.fromFacePosLevel(face, 0, 0);
    int arity = random().nextInt(3) + 1;
    int level = random().nextInt(S2PrefixTree.getMaxLevels(arity));
    level = level * arity;
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
    S2PrefixTree tree = new S2PrefixTree(new Geo3dSpatialContextFactory().newSpatialContext(), S2PrefixTree.getMaxLevels(arity), arity);
    S2PrefixTreeCell cell = new S2PrefixTreeCell(tree, id);
    BytesRef ref = cell.getTokenBytesWithLeaf(null);
    if (random().nextBoolean()) {
      int newOffset = random().nextInt(10) + 1;
      byte[] newBytes = new byte[ref.bytes.length +  newOffset];
      for (int i = 0; i < ref.bytes.length; i++) {
        newBytes[i + newOffset] = ref.bytes[i];
      }
      ref.bytes = newBytes;
      ref.offset = ref.offset + newOffset;
    }
    S2PrefixTreeCell cell2 = new S2PrefixTreeCell(tree, null);
    cell2.readCell(tree, ref);
    assertEquals(cell, cell2);
  }

  @Test
  @Repeat(iterations = 10)
  public void testDistanceAndLevels() {
    S2PrefixTree tree = new S2PrefixTree(new Geo3dSpatialContextFactory().newSpatialContext(), S2PrefixTree.getMaxLevels(1), 1);

    double randomDist = random().nextDouble() * 5;
    int levelDistance = tree.getLevelForDistance(randomDist);
    double distanceLevel = tree.getDistanceForLevel(levelDistance);
    assertTrue(randomDist > distanceLevel);


    tree = new S2PrefixTree(new Geo3dSpatialContextFactory().newSpatialContext(), S2PrefixTree.getMaxLevels(2), 2);

    levelDistance = tree.getLevelForDistance(randomDist);
    distanceLevel = tree.getDistanceForLevel(levelDistance);
    assertTrue(randomDist > distanceLevel);

    tree = new S2PrefixTree(new Geo3dSpatialContextFactory().newSpatialContext(), S2PrefixTree.getMaxLevels(3), 3);

    levelDistance = tree.getLevelForDistance(randomDist);
    distanceLevel = tree.getDistanceForLevel(levelDistance);
    assertTrue(randomDist > distanceLevel);

  }

  @Test
  @Repeat(iterations = 10)
  public void testPrecision() {
    int arity = random().nextInt(3) +1;
    SpatialContext context = new Geo3dSpatialContextFactory().newSpatialContext();
    S2PrefixTree tree = new S2PrefixTree(context, S2PrefixTree.getMaxLevels(arity), arity);
    double precision = random().nextDouble();
    int level = tree.getLevelForDistance(precision);
    Point point = context.getShapeFactory().pointXY(0, 0);
    CellIterator iterator = tree.getTreeCellIterator(point, level);
    S2PrefixTreeCell cell = null;
    while (iterator.hasNext()) {
      cell = (S2PrefixTreeCell)iterator.next();
    }
    assertTrue(cell.getLevel() == level);
    double precisionCell = S2Projections.MAX_WIDTH.getValue(cell.cellId.level());
    assertTrue(precision > precisionCell);
  }
}