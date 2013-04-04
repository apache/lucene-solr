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
import com.spatial4j.core.shape.SpatialRelation;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.spatial.prefix.tree.Cell;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.SentinelIntSet;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

/**
 * Finds docs where its indexed shape {@link org.apache.lucene.spatial.query.SpatialOperation#Contains
 * CONTAINS} the query shape. For use on {@link RecursivePrefixTreeStrategy}.
 *
 * @lucene.experimental
 */
public class ContainsPrefixTreeFilter extends AbstractPrefixTreeFilter {

  public ContainsPrefixTreeFilter(Shape queryShape, String fieldName, SpatialPrefixTree grid, int detailLevel) {
    super(queryShape, fieldName, grid, detailLevel);
  }

  @Override
  public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {
    return new ContainsVisitor(context, acceptDocs).visit(grid.getWorldCell(), acceptDocs);
  }

  private class ContainsVisitor extends BaseTermsEnumTraverser {

    public ContainsVisitor(AtomicReaderContext context, Bits acceptDocs) throws IOException {
      super(context, acceptDocs);
    }

    BytesRef termBytes = new BytesRef();
    Cell nextCell;//see getLeafDocs

    /** This is the primary algorithm; recursive.  Returns null if finds none. */
    private SmallDocSet visit(Cell cell, Bits acceptContains) throws IOException {

      if (termsEnum == null)//signals all done
        return null;

      //Leaf docs match all query shape
      SmallDocSet leafDocs = getLeafDocs(cell, acceptContains);

      // Get the AND of all child results
      SmallDocSet combinedSubResults = null;
      Collection<Cell> subCells = cell.getSubCells(queryShape);
      for (Cell subCell : subCells) {
        if (!seekExact(subCell))
          combinedSubResults = null;
        else if (subCell.getLevel() == detailLevel)
          combinedSubResults = getDocs(subCell, acceptContains);
        else if (subCell.getShapeRel() == SpatialRelation.WITHIN)
          combinedSubResults = getLeafDocs(subCell, acceptContains);
        else
          combinedSubResults = visit(subCell, acceptContains); //recursion

        if (combinedSubResults == null)
          break;
        acceptContains = combinedSubResults;//has the 'AND' effect on next iteration
      }

      // Result: OR the leaf docs with AND of all child results
      if (combinedSubResults != null) {
        if (leafDocs == null)
          return combinedSubResults;
        return leafDocs.union(combinedSubResults);
      }
      return leafDocs;
    }

    private boolean seekExact(Cell cell) throws IOException {
      assert new BytesRef(cell.getTokenBytes()).compareTo(termBytes) > 0;

      termBytes.bytes = cell.getTokenBytes();
      termBytes.length = termBytes.bytes.length;
      return termsEnum.seekExact(termBytes, cell.getLevel() <= 2);
    }

    private SmallDocSet getDocs(Cell cell, Bits acceptContains) throws IOException {
      assert new BytesRef(cell.getTokenBytes()).equals(termBytes);

      return collectDocs(acceptContains);
    }

    private SmallDocSet getLeafDocs(Cell leafCell, Bits acceptContains) throws IOException {
      assert new BytesRef(leafCell.getTokenBytes()).equals(termBytes);

      BytesRef nextTerm = termsEnum.next();
      if (nextTerm == null) {
        termsEnum = null;//signals all done
        return null;
      }
      nextCell = grid.getCell(nextTerm.bytes, nextTerm.offset, nextTerm.length, nextCell);
      if (nextCell.getLevel() == leafCell.getLevel() && nextCell.isLeaf()) {
        return collectDocs(acceptContains);
      } else {
        return null;
      }
    }

    private SmallDocSet collectDocs(Bits acceptContains) throws IOException {
      SmallDocSet set = null;

      docsEnum = termsEnum.docs(acceptContains, docsEnum, DocsEnum.FLAG_NONE);
      int docid;
      while ((docid = docsEnum.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
        if (set == null) {
          int size = termsEnum.docFreq();
          if (size <= 0)
            size = 16;
          set = new SmallDocSet(size);
        }
        set.set(docid);
      }
      return set;
    }

  }//class ContainsVisitor

  /** A hash based mutable set of docIds. If this were Solr code then we might
   * use a combination of HashDocSet and SortedIntDocSet instead. */
  private static class SmallDocSet extends DocIdSet implements Bits {

    private final SentinelIntSet intSet;
    private int maxInt = 0;

    public SmallDocSet(int size) {
      intSet = new SentinelIntSet(size, -1);
    }

    @Override
    public boolean get(int index) {
      return intSet.exists(index);
    }

    public void set(int index) {
      intSet.put(index);
      if (index > maxInt)
        maxInt = index;
    }

    /** Largest docid. */
    @Override
    public int length() {
      return maxInt;
    }

    /** Number of docids. */
    public int size() {
      return intSet.size();
    }

    /** NOTE: modifies and returns either "this" or "other" */
    public SmallDocSet union(SmallDocSet other) {
      SmallDocSet bigger;
      SmallDocSet smaller;
      if (other.intSet.size() > this.intSet.size()) {
        bigger = other;
        smaller = this;
      } else {
        bigger = this;
        smaller = other;
      }
      //modify bigger
      for (int v : smaller.intSet.keys) {
        if (v == smaller.intSet.emptyVal)
          continue;
        bigger.set(v);
      }
      return bigger;
    }

    @Override
    public Bits bits() throws IOException {
      //if the # of docids is super small, return null since iteration is going
      // to be faster
      return size() > 4 ? this : null;
    }

    @Override
    public DocIdSetIterator iterator() throws IOException {
      if (size() == 0)
        return null;
      //copy the unsorted values to a new array then sort them
      int d = 0;
      final int[] docs = new int[intSet.size()];
      for (int v : intSet.keys) {
        if (v == intSet.emptyVal)
          continue;
        docs[d++] = v;
      }
      assert d == intSet.size();
      final int size = d;

      //sort them
      Arrays.sort(docs, 0, size);

      return new DocIdSetIterator() {
        int idx = -1;
        @Override
        public int docID() {
          if (idx >= 0 && idx < size)
            return docs[idx];
          else
            return -1;
        }

        @Override
        public int nextDoc() throws IOException {
          if (++idx < size)
            return docs[idx];
          return NO_MORE_DOCS;
        }

        @Override
        public int advance(int target) throws IOException {
          //for this small set this is likely faster vs. a binary search
          // into the sorted array
          return slowAdvance(target);
        }

        @Override
        public long cost() {
          return size;
        }
      };
    }

  }//class SmallDocSet

}
