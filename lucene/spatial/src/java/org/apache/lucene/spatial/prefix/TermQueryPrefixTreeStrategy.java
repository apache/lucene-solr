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

import java.util.ArrayList;
import java.util.List;

import com.spatial4j.core.shape.Point;
import com.spatial4j.core.shape.Shape;
import org.apache.lucene.queries.TermsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.spatial.prefix.tree.Cell;
import org.apache.lucene.spatial.prefix.tree.CellIterator;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.apache.lucene.spatial.query.UnsupportedSpatialOperation;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;

/**
 * A basic implementation of {@link PrefixTreeStrategy} using a large
 * {@link TermsQuery} of all the cells from
 * {@link SpatialPrefixTree#getTreeCellIterator(com.spatial4j.core.shape.Shape, int)}.
 * It only supports the search of indexed Point shapes.
 * <p>
 * The precision of query shapes (distErrPct) is an important factor in using
 * this Strategy. If the precision is too precise then it will result in many
 * terms which will amount to a slower query.
 *
 * @lucene.experimental
 */
public class TermQueryPrefixTreeStrategy extends PrefixTreeStrategy {

  protected boolean simplifyIndexedCells = false;

  public TermQueryPrefixTreeStrategy(SpatialPrefixTree grid, String fieldName) {
    super(grid, fieldName);
  }

  @Override
  protected CellToBytesRefIterator newCellToBytesRefIterator() {
    //Ensure we don't have leaves, as this strategy doesn't handle them.
    return new CellToBytesRefIterator() {
      @Override
      public BytesRef next() {
        if (!cellIter.hasNext()) {
          return null;
        }
        return cellIter.next().getTokenBytesNoLeaf(bytesRef);
      }
    };
  }

  @Override
  public Query makeQuery(SpatialArgs args) {
    final SpatialOperation op = args.getOperation();
    if (op != SpatialOperation.Intersects)
      throw new UnsupportedSpatialOperation(op);

    Shape shape = args.getShape();
    int detailLevel = grid.getLevelForDistance(args.resolveDistErr(ctx, distErrPct));

    //--get a List of BytesRef for each term we want (no parents, no leaf bytes))
    final int GUESS_NUM_TERMS;
    if (shape instanceof Point)
      GUESS_NUM_TERMS = detailLevel;//perfect guess
    else
      GUESS_NUM_TERMS = 4096;//should this be a method on SpatialPrefixTree?

    BytesRefBuilder masterBytes = new BytesRefBuilder();//shared byte array for all terms
    List<BytesRef> terms = new ArrayList<>(GUESS_NUM_TERMS);

    CellIterator cells = grid.getTreeCellIterator(shape, detailLevel);
    while (cells.hasNext()) {
      Cell cell = cells.next();
      if (!cell.isLeaf())
        continue;
      BytesRef term = cell.getTokenBytesNoLeaf(null);//null because we want a new BytesRef
      //We copy out the bytes because it may be re-used across the iteration. This also gives us the opportunity
      // to use one contiguous block of memory for the bytes of all terms we need.
      masterBytes.grow(masterBytes.length() + term.length);
      masterBytes.append(term);
      term.bytes = null;//don't need; will reset later
      term.offset = masterBytes.length() - term.length;
      terms.add(term);
    }
    //doing this now because if we did earlier, it's possible the bytes needed to grow()
    for (BytesRef byteRef : terms) {
      byteRef.bytes = masterBytes.bytes();
    }
    //unfortunately TermsQuery will needlessly sort & dedupe
    //TODO an automatonQuery might be faster?
    return new TermsQuery(getFieldName(), terms);
  }

}
