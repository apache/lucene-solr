package org.apache.lucene.spatial;

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

import java.text.ParseException;

import org.apache.lucene.document.Document;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.spatial.prefix.RecursivePrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.NumberRangePrefixTree;
import com.spatial4j.core.shape.Point;
import com.spatial4j.core.shape.Shape;

/** A PrefixTree based on Number/Date ranges. This isn't very "spatial" on the surface (to the user) but
 * it's implemented using spatial so that's why it's here extending a SpatialStrategy.
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
  public void addFields(Document doc, Shape shape) {
    //levels doesn't actually matter; NumberRange based Shapes have their own "level".
    doc.addLargeText(getFieldName(), createTokenStream(shape, grid.getMaxLevels()));
  }

  /** For a Date based tree, pass in a Calendar, with unspecified fields marked as cleared.
   * See {@link NumberRangePrefixTree#toShape(Object)}. */
  public Shape toShape(Object value) {
    return getGrid().toShape(value);
  }

  /** See {@link NumberRangePrefixTree#toRangeShape(Shape, Shape)}. */
  public Shape toRangeShape(Shape min, Shape max) {
    return getGrid().toRangeShape(min, max);
  }

  /** See {@link NumberRangePrefixTree#parseShape(String)}. */
  public Shape parseShape(String str) throws ParseException {
    return getGrid().parseShape(str);
  }

  /** Unsupported. */
  @Override
  public ValueSource makeDistanceValueSource(Point queryPoint, double multiplier) {
    throw new UnsupportedOperationException();
  }
}
