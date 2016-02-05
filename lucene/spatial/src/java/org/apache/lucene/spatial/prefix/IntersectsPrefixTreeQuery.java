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

import java.io.IOException;

import com.spatial4j.core.shape.Shape;
import com.spatial4j.core.shape.SpatialRelation;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.spatial.prefix.tree.Cell;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;
import org.apache.lucene.util.BitDocIdSet;
import org.apache.lucene.util.FixedBitSet;

/**
 * A Query matching documents that have an {@link SpatialRelation#INTERSECTS}
 * (i.e. not DISTINCT) relationship with a provided query shape.
 *
 * @lucene.internal
 */
public class IntersectsPrefixTreeQuery extends AbstractVisitingPrefixTreeQuery {

  public IntersectsPrefixTreeQuery(Shape queryShape, String fieldName,
                                   SpatialPrefixTree grid, int detailLevel,
                                   int prefixGridScanLevel) {
    super(queryShape, fieldName, grid, detailLevel, prefixGridScanLevel);
  }

  @Override
  protected DocIdSet getDocIdSet(LeafReaderContext context) throws IOException {
    /* Possible optimizations (in IN ADDITION TO THOSE LISTED IN VISITORTEMPLATE):

    * If docFreq is 1 (or < than some small threshold), then check to see if we've already
      collected it; if so short-circuit. Don't do this just for point data, as there is
      no benefit, or only marginal benefit when multi-valued.

    * Point query shape optimization when the only indexed data is a point (no leaves).  Result is a term query.

     */
    return new VisitorTemplate(context) {
      private FixedBitSet results;

      @Override
      protected void start() {
        results = new FixedBitSet(maxDoc);
      }

      @Override
      protected DocIdSet finish() {
        return new BitDocIdSet(results);
      }

      @Override
      protected boolean visitPrefix(Cell cell) throws IOException {
        if (cell.getShapeRel() == SpatialRelation.WITHIN || cell.getLevel() == detailLevel) {
          collectDocs(results);
          return false;
        }
        return true;
      }

      @Override
      protected void visitLeaf(Cell cell) throws IOException {
        collectDocs(results);
      }

    }.getDocIdSet();
  }

  @Override
  public String toString(String field) {
    return getClass().getSimpleName() + "(" +
        "fieldName=" + fieldName + "," +
        "queryShape=" + queryShape + "," +
        "detailLevel=" + detailLevel + "," +
        "prefixGridScanLevel=" + prefixGridScanLevel +
        ")";
  }

}
