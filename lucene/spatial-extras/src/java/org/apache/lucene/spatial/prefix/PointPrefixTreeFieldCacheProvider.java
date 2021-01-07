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
package org.apache.lucene.spatial.prefix;

import org.apache.lucene.spatial.prefix.tree.Cell;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;
import org.apache.lucene.spatial.util.ShapeFieldCacheProvider;
import org.apache.lucene.util.BytesRef;
import org.locationtech.spatial4j.shape.Point;

/**
 * Implementation of {@link ShapeFieldCacheProvider} designed for {@link PrefixTreeStrategy}s that
 * index points (AND ONLY POINTS!).
 *
 * @lucene.internal
 */
public class PointPrefixTreeFieldCacheProvider extends ShapeFieldCacheProvider<Point> {

  private final SpatialPrefixTree grid;
  private Cell scanCell; // re-used in readShape to save GC

  public PointPrefixTreeFieldCacheProvider(
      SpatialPrefixTree grid, String shapeField, int defaultSize) {
    super(shapeField, defaultSize);
    this.grid = grid;
  }

  @Override
  protected Point readShape(BytesRef term) {
    scanCell = grid.readCell(term, scanCell);
    if (scanCell.getLevel() == grid.getMaxLevels()) return scanCell.getShape().getCenter();
    return null;
  }
}
