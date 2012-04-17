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
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.spatial.SimpleSpatialFieldInfo;
import org.apache.lucene.spatial.prefix.tree.Node;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;

import java.util.List;

public class TermQueryPrefixTreeStrategy extends PrefixTreeStrategy {

  public TermQueryPrefixTreeStrategy(SpatialPrefixTree grid) {
    super(grid);
  }

  @Override
  public Filter makeFilter(SpatialArgs args, SimpleSpatialFieldInfo fieldInfo) {
    return new QueryWrapperFilter( makeQuery(args, fieldInfo) );
  }

  @Override
  public Query makeQuery(SpatialArgs args, SimpleSpatialFieldInfo fieldInfo) {
    if (args.getOperation() != SpatialOperation.Intersects &&
        args.getOperation() != SpatialOperation.IsWithin &&
        args.getOperation() != SpatialOperation.Overlaps ){
      // TODO -- can translate these other query types
      throw new UnsupportedSpatialOperation(args.getOperation());
    }
    Shape qshape = args.getShape();
    int detailLevel = grid.getMaxLevelForPrecision(qshape, args.getDistPrecision());
    List<Node> cells = grid.getNodes(qshape, detailLevel, false);

    BooleanQuery booleanQuery = new BooleanQuery();
    for (Node cell : cells) {
      booleanQuery.add(new TermQuery(new Term(fieldInfo.getFieldName(), cell.getTokenString())), BooleanClause.Occur.SHOULD);
    }
    return booleanQuery;
  }

}
