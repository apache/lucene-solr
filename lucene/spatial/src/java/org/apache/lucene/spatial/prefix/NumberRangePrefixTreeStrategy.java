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

import java.util.Iterator;

import com.spatial4j.core.shape.Point;
import com.spatial4j.core.shape.Shape;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.spatial.prefix.tree.Cell;
import org.apache.lucene.spatial.prefix.tree.NumberRangePrefixTree;

/** A PrefixTree based on Number/Date ranges. This isn't very "spatial" on the surface (to the user) but
 * it's implemented using spatial so that's why it's here extending a SpatialStrategy. When using this class, you will
 * use various utility methods on the prefix tree implementation to convert objects/strings to/from shapes.
 *
 * To use with dates, pass in {@link org.apache.lucene.spatial.prefix.tree.DateRangePrefixTree}.
 *
 * @lucene.experimental
 */
public class NumberRangePrefixTreeStrategy extends RecursivePrefixTreeStrategy {

  public NumberRangePrefixTreeStrategy(NumberRangePrefixTree prefixTree, String fieldName) {
    super(prefixTree, fieldName);
    setPruneLeafyBranches(false);
    setPrefixGridScanLevel(prefixTree.getMaxLevels()-2);//user might want to change, however
    setPointsOnly(false);
    setDistErrPct(0);
  }

  @Override
  public NumberRangePrefixTree getGrid() {
    return (NumberRangePrefixTree) super.getGrid();
  }

  @Override
  protected Iterator<Cell> createCellIteratorToIndex(Shape shape, int detailLevel, Iterator<Cell> reuse) {
    //levels doesn't actually matter; NumberRange based Shapes have their own "level".
    return super.createCellIteratorToIndex(shape, grid.getMaxLevels(), reuse);
  }

  /** Unsupported. */
  @Override
  public ValueSource makeDistanceValueSource(Point queryPoint, double multiplier) {
    throw new UnsupportedOperationException();
  }
}
