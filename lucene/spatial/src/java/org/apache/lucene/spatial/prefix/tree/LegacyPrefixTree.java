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

import java.util.Arrays;

import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.shape.Point;
import com.spatial4j.core.shape.Rectangle;
import com.spatial4j.core.shape.Shape;
import org.apache.lucene.util.BytesRef;

/** The base for the original two SPT's: Geohash and Quad. Don't subclass this for new SPTs.
 * @lucene.internal */
abstract class LegacyPrefixTree extends SpatialPrefixTree {
  public LegacyPrefixTree(SpatialContext ctx, int maxLevels) {
    super(ctx, maxLevels);
  }

  public double getDistanceForLevel(int level) {
    if (level < 1 || level > getMaxLevels())
      throw new IllegalArgumentException("Level must be in 1 to maxLevels range");
    //TODO cache for each level
    Cell cell = getCell(ctx.getWorldBounds().getCenter(), level);
    Rectangle bbox = cell.getShape().getBoundingBox();
    double width = bbox.getWidth();
    double height = bbox.getHeight();
    //Use standard cartesian hypotenuse. For geospatial, this answer is larger
    // than the correct one but it's okay to over-estimate.
    return Math.sqrt(width * width + height * height);
  }

  /**
   * Returns the cell containing point {@code p} at the specified {@code level}.
   */
  protected abstract Cell getCell(Point p, int level);

  @Override
  public Cell readCell(BytesRef term, Cell scratch) {
    LegacyCell cell = (LegacyCell) scratch;
    if (cell == null)
      cell = (LegacyCell) getWorldCell();
    cell.readCell(term);
    return cell;
  }

  @Override
  public CellIterator getTreeCellIterator(Shape shape, int detailLevel) {
    if (!(shape instanceof Point))
      return super.getTreeCellIterator(shape, detailLevel);

    //This specialization is here because the legacy implementations don't have a fast implementation of
    // cell.getSubCells(point). It's fastest here to encode the full bytes for detailLevel, and create
    // subcells from the bytesRef in a loop. This avoids an O(N^2) encode, and we have O(N) instead.

    Cell cell = getCell((Point) shape, detailLevel);
    assert cell instanceof LegacyCell;
    BytesRef fullBytes = cell.getTokenBytesNoLeaf(null);
    //fill in reverse order to be sorted
    Cell[] cells = new Cell[detailLevel];
    for (int i = 1; i < detailLevel; i++) {
      fullBytes.length = i;
      Cell parentCell = readCell(fullBytes, null);
      cells[i-1] = parentCell;
    }
    cells[detailLevel-1] = cell;
    return new FilterCellIterator(Arrays.asList(cells).iterator(), null);//null filter
  }

}
