package org.apache.lucene.spatial.prefix;

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

import com.spatial4j.core.shape.Point;
import org.apache.lucene.spatial.prefix.tree.Cell;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;
import org.apache.lucene.spatial.util.ShapeFieldCacheProvider;
import org.apache.lucene.util.BytesRef;

/**
 * Implementation of {@link ShapeFieldCacheProvider} designed for {@link PrefixTreeStrategy}s.
 *
 * Note, due to the fragmented representation of Shapes in these Strategies, this implementation
 * can only retrieve the central {@link Point} of the original Shapes.
 *
 * @lucene.internal
 */
public class PointPrefixTreeFieldCacheProvider extends ShapeFieldCacheProvider<Point> {

  final SpatialPrefixTree grid; //

  public PointPrefixTreeFieldCacheProvider(SpatialPrefixTree grid, String shapeField, int defaultSize) {
    super( shapeField, defaultSize );
    this.grid = grid;
  }

  private Cell scanCell = null;//re-used in readShape to save GC

  @Override
  protected Point readShape(BytesRef term) {
    scanCell = grid.getCell(term.bytes, term.offset, term.length, scanCell);
    if (scanCell.getLevel() == grid.getMaxLevels() && !scanCell.isLeaf())
      return scanCell.getCenter();
    return null;
  }
}
