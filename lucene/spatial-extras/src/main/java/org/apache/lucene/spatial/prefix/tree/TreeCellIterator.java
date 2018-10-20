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

import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.Shape;

/**
 * Navigates a {@link org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree} from a given cell (typically the world
 * cell) down to a maximum number of configured levels, filtered by a given shape. Intermediate non-leaf cells are
 * returned. It supports {@link #remove()} for skipping traversal of subcells of the current cell.
 *
 * @lucene.internal
 */
class TreeCellIterator extends CellIterator {
  //This class uses a stack approach, which is more efficient than creating linked nodes. And it might more easily
  // pave the way for re-using Cell & CellIterator at a given level in the future.

  private final Shape shapeFilter;//possibly null
  private final CellIterator[] iterStack;//starts at level 1
  private int stackIdx;//-1 when done
  private boolean descend;

  public TreeCellIterator(Shape shapeFilter, int detailLevel, Cell parentCell) {
    this.shapeFilter = shapeFilter;
    assert parentCell.getLevel() == 0;
    iterStack = new CellIterator[detailLevel];
    iterStack[0] = parentCell.getNextLevelCells(shapeFilter);
    stackIdx = 0;//always points to an iter (non-null)
    //note: not obvious but needed to visit the first cell before trying to descend
    descend = false;
  }

  @Override
  public boolean hasNext() {
    if (nextCell != null)
      return true;
    while (true) {
      if (stackIdx == -1)//the only condition in which we return false
        return false;
      //If we can descend...
      if (descend && !(stackIdx == iterStack.length - 1 || iterStack[stackIdx].thisCell().isLeaf())) {
        CellIterator nextIter = iterStack[stackIdx].thisCell().getNextLevelCells(shapeFilter);
        //push stack
        iterStack[++stackIdx] = nextIter;
      }
      //Get sibling...
      if (iterStack[stackIdx].hasNext()) {
        nextCell = iterStack[stackIdx].next();
        //at detailLevel
        if (stackIdx == iterStack.length - 1 && !(shapeFilter instanceof Point)) //point check is a kludge
          nextCell.setLeaf();//because at bottom
        break;
      }
      //Couldn't get next; go up...
      //pop stack
      iterStack[stackIdx--] = null;
      descend = false;//so that we don't re-descend where we just were
    }
    assert nextCell != null;
    descend = true;//reset
    return true;
  }

  @Override
  public void remove() {
    assert thisCell() != null && nextCell == null;
    descend = false;
  }

  //TODO implement a smart nextFrom() that looks at the parent's bytes first

}
