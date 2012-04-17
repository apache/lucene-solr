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

import com.spatial4j.core.exception.UnsupportedSpatialOperation;
import com.spatial4j.core.query.SpatialArgs;
import com.spatial4j.core.query.SpatialOperation;
import com.spatial4j.core.shape.Shape;
import org.apache.lucene.queries.function.FunctionQuery;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.FilteredQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.spatial.SimpleSpatialFieldInfo;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;


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
  public Query makeQuery(SpatialArgs args, SimpleSpatialFieldInfo fieldInfo) {
    Filter f = makeFilter(args, fieldInfo);

    ValueSource vs = makeValueSource(args, fieldInfo);
    return new FilteredQuery( new FunctionQuery(vs), f );
  }

  @Override
  public Filter makeFilter(SpatialArgs args, SimpleSpatialFieldInfo fieldInfo) {
    final SpatialOperation op = args.getOperation();
    if (! SpatialOperation.is(op, SpatialOperation.IsWithin, SpatialOperation.Intersects, SpatialOperation.BBoxWithin))
      throw new UnsupportedSpatialOperation(op);

    Shape qshape = args.getShape();

    int detailLevel = grid.getMaxLevelForPrecision(qshape,args.getDistPrecision());

    return new RecursivePrefixTreeFilter(
        fieldInfo.getFieldName(), grid,qshape, prefixGridScanLevel, detailLevel);
  }
}




