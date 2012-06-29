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

import com.spatial4j.core.shape.Shape;
import org.apache.lucene.search.Filter;
import org.apache.lucene.spatial.SimpleSpatialFieldInfo;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.apache.lucene.spatial.query.UnsupportedSpatialOperation;

/**
 * Based on {@link RecursivePrefixTreeFilter}.
 *
 * @lucene.experimental
 */
public class RecursivePrefixTreeStrategy extends PrefixTreeStrategy {

  private int prefixGridScanLevel;//TODO how is this customized?

  public RecursivePrefixTreeStrategy(SpatialPrefixTree grid) {
    super(grid);
    prefixGridScanLevel = grid.getMaxLevels() - 4;//TODO this default constant is dependent on the prefix grid size
  }

  public void setPrefixGridScanLevel(int prefixGridScanLevel) {
    this.prefixGridScanLevel = prefixGridScanLevel;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName()+"(prefixGridScanLevel:"+prefixGridScanLevel+",SPG:("+ grid +"))";
  }

  @Override
  public Filter makeFilter(SpatialArgs args, SimpleSpatialFieldInfo fieldInfo) {
    final SpatialOperation op = args.getOperation();
    if (! SpatialOperation.is(op, SpatialOperation.IsWithin, SpatialOperation.Intersects, SpatialOperation.BBoxWithin, SpatialOperation.BBoxIntersects))
      throw new UnsupportedSpatialOperation(op);

    Shape shape = args.getShape();

    int detailLevel = grid.getMaxLevelForPrecision(shape,args.getDistPrecision());

    return new RecursivePrefixTreeFilter(
        fieldInfo.getFieldName(), grid,shape, prefixGridScanLevel, detailLevel);
  }
}




