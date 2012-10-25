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

import com.spatial4j.core.shape.Shape;
import com.spatial4j.core.shape.SpatialRelation;
import org.apache.lucene.index.*;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.spatial.prefix.tree.Node;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.OpenBitSet;
import org.apache.lucene.util.StringHelper;

import java.io.IOException;
import java.util.LinkedList;

/**
 * Performs a spatial intersection filter between a query shape and a field
 * indexed with {@link SpatialPrefixTree}, a Trie. SPT yields terms (grids) at
 * length 1 (aka "Level 1") and at greater lengths corresponding to greater
 * precisions. This filter recursively traverses each grid length and uses
 * methods on {@link Shape} to efficiently know that all points at a prefix fit
 * in the shape or not to either short-circuit unnecessary traversals or to
 * efficiently load all enclosed points.  If no indexed data lies in a portion
 * of the shape then that portion of the query shape is quickly passed over
 * without decomposing the shape unnecessarily.
 *
 * @lucene.internal
 */
public class RecursivePrefixTreeFilter extends Filter {

  /* TODOs for future:

Can a polygon query shape be optimized / made-simpler at recursive depths (e.g. intersection of shape + cell box)

RE "scan" threshold:
  // IF configured to do so, we could use term.freq() as an estimate on the number of places at this depth.  OR, perhaps
  //  make estimates based on the total known term count at this level?
  if (!scan) {
    //Make some estimations on how many points there are at this level and how few there would need to be to set
    // !scan to false.
    long termsThreshold = (long) estimateNumberIndexedTerms(cell.length(),queryShape.getDocFreqExpenseThreshold(cell));
    long thisOrd = termsEnum.ord();
    scan = (termsEnum.seek(thisOrd+termsThreshold+1) == TermsEnum.SeekStatus.END
            || !cell.contains(termsEnum.term()));
    termsEnum.seek(thisOrd);//return to last position
  }

  */

  private final String fieldName;
  private final SpatialPrefixTree grid;
  private final Shape queryShape;
  private final int prefixGridScanLevel;//at least one less than grid.getMaxLevels()
  private final int detailLevel;

  public RecursivePrefixTreeFilter(String fieldName, SpatialPrefixTree grid, Shape queryShape, int prefixGridScanLevel,
                             int detailLevel) {
    this.fieldName = fieldName;
    this.grid = grid;
    this.queryShape = queryShape;
    this.prefixGridScanLevel = Math.max(1,Math.min(prefixGridScanLevel,grid.getMaxLevels()-1));
    this.detailLevel = detailLevel;
    assert detailLevel <= grid.getMaxLevels();
  }

  @Override
  public DocIdSet getDocIdSet(AtomicReaderContext ctx, Bits acceptDocs) throws IOException {
    AtomicReader reader = ctx.reader();
    OpenBitSet bits = new OpenBitSet(reader.maxDoc());
    Terms terms = reader.terms(fieldName);
    if (terms == null)
      return null;
    TermsEnum termsEnum = terms.iterator(null);
    DocsEnum docsEnum = null;//cached for termsEnum.docs() calls
    Node scanCell = null;

    //cells is treated like a stack. LinkedList conveniently has bulk add to beginning. It's in sorted order so that we
    //  always advance forward through the termsEnum index.
    LinkedList<Node> cells = new LinkedList<Node>(
        grid.getWorldNode().getSubCells(queryShape) );

    //This is a recursive algorithm that starts with one or more "big" cells, and then recursively dives down into the
    // first such cell that intersects with the query shape.  It's a depth first traversal because we don't move onto
    // the next big cell (breadth) until we're completely done considering all smaller cells beneath it. For a given
    // cell, if it's *within* the query shape then we can conveniently short-circuit the depth traversal and
    // grab all documents assigned to this cell/term.  For an intersection of the cell and query shape, we either
    // recursively step down another grid level or we decide heuristically (via prefixGridScanLevel) that there aren't
    // that many points, and so we scan through all terms within this cell (i.e. the term starts with the cell's term),
    // seeing which ones are within the query shape.
    while(!cells.isEmpty()) {
      final Node cell = cells.removeFirst();
      final BytesRef cellTerm = new BytesRef(cell.getTokenBytes());
      TermsEnum.SeekStatus seekStat = termsEnum.seekCeil(cellTerm);
      if (seekStat == TermsEnum.SeekStatus.END)
        break;
      if (seekStat == TermsEnum.SeekStatus.NOT_FOUND)
        continue;
      if (cell.getLevel() == detailLevel || cell.isLeaf()) {
        docsEnum = termsEnum.docs(acceptDocs, docsEnum, 0);
        addDocs(docsEnum,bits);
      } else {//any other intersection
        //If the next indexed term is the leaf marker, then add all of them
        BytesRef nextCellTerm = termsEnum.next();
        assert StringHelper.startsWith(nextCellTerm, cellTerm);
        scanCell = grid.getNode(nextCellTerm.bytes, nextCellTerm.offset, nextCellTerm.length, scanCell);
        if (scanCell.isLeaf()) {
          docsEnum = termsEnum.docs(acceptDocs, docsEnum, 0);
          addDocs(docsEnum,bits);
          termsEnum.next();//move pointer to avoid potential redundant addDocs() below
        }

        //Decide whether to continue to divide & conquer, or whether it's time to scan through terms beneath this cell.
        // Scanning is a performance optimization trade-off.
        boolean scan = cell.getLevel() >= prefixGridScanLevel;//simple heuristic

        if (!scan) {
          //Divide & conquer
          cells.addAll(0, cell.getSubCells(queryShape));//add to beginning
        } else {
          //Scan through all terms within this cell to see if they are within the queryShape. No seek()s.
          for(BytesRef term = termsEnum.term(); term != null && StringHelper.startsWith(term,cellTerm); term = termsEnum.next()) {
            scanCell = grid.getNode(term.bytes, term.offset, term.length, scanCell);
            int termLevel = scanCell.getLevel();
            if (termLevel > detailLevel)
              continue;
            if (termLevel == detailLevel || scanCell.isLeaf()) {
              //TODO should put more thought into implications of box vs point
              Shape cShape = termLevel == grid.getMaxLevels() ? scanCell.getCenter() : scanCell.getShape();
              if(queryShape.relate(cShape) == SpatialRelation.DISJOINT)
                continue;

              docsEnum = termsEnum.docs(acceptDocs, docsEnum, 0);
              addDocs(docsEnum,bits);
            }
          }//term loop
        }
      }
    }//cell loop

    return bits;
  }

  private void addDocs(DocsEnum docsEnum, OpenBitSet bits) throws IOException {
    int docid;
    while ((docid = docsEnum.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
      bits.fastSet(docid);
    }
  }

  @Override
  public String toString() {
    return getClass().getSimpleName()+"{fieldName='" + fieldName + '\'' + ", shape=" + queryShape + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    RecursivePrefixTreeFilter that = (RecursivePrefixTreeFilter) o;

    if (!fieldName.equals(that.fieldName)) return false;
    //note that we don't need to look at grid since for the same field it should be the same
    if (prefixGridScanLevel != that.prefixGridScanLevel) return false;
    if (detailLevel != that.detailLevel) return false;
    if (!queryShape.equals(that.queryShape)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = fieldName.hashCode();
    result = 31 * result + queryShape.hashCode();
    result = 31 * result + detailLevel;
    return result;
  }
}
