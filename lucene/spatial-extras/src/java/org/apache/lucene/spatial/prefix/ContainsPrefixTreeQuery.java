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
import java.util.Arrays;

import com.spatial4j.core.shape.Shape;
import com.spatial4j.core.shape.SpatialRelation;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.spatial.prefix.tree.Cell;
import org.apache.lucene.spatial.prefix.tree.CellIterator;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.SentinelIntSet;

/**
 * Finds docs where its indexed shape {@link org.apache.lucene.spatial.query.SpatialOperation#Contains
 * CONTAINS} the query shape. For use on {@link RecursivePrefixTreeStrategy}.
 *
 * @lucene.experimental
 */
public class ContainsPrefixTreeQuery extends AbstractPrefixTreeQuery {

  /**
   * If the spatial data for a document is comprised of multiple overlapping or adjacent parts,
   * it might fail to match a query shape when doing the CONTAINS predicate when the sum of
   * those shapes contain the query shape but none do individually.  Set this to false to
   * increase performance if you don't care about that circumstance (such as if your indexed
   * data doesn't even have such conditions).  See LUCENE-5062.
   */
  protected final boolean multiOverlappingIndexedShapes;

  public ContainsPrefixTreeQuery(Shape queryShape, String fieldName, SpatialPrefixTree grid, int detailLevel, boolean multiOverlappingIndexedShapes) {
    super(queryShape, fieldName, grid, detailLevel);
    this.multiOverlappingIndexedShapes = multiOverlappingIndexedShapes;
  }

  @Override
  public boolean equals(Object o) {
    if (!super.equals(o))
      return false;
    return multiOverlappingIndexedShapes == ((ContainsPrefixTreeQuery)o).multiOverlappingIndexedShapes;
  }

  @Override
  public int hashCode() {
    return super.hashCode() + (multiOverlappingIndexedShapes ? 1 : 0);
  }

  @Override
  public String toString(String field) {
    return getClass().getSimpleName() + "(" +
        "fieldName=" + fieldName + "," +
        "queryShape=" + queryShape + "," +
        "detailLevel=" + detailLevel + "," +
        "multiOverlappingIndexedShapes=" + multiOverlappingIndexedShapes +
        ")";
  }

  @Override
  protected DocIdSet getDocIdSet(LeafReaderContext context) throws IOException {
    return new ContainsVisitor(context).visit(grid.getWorldCell(), null);
  }

  private class ContainsVisitor extends BaseTermsEnumTraverser {

    public ContainsVisitor(LeafReaderContext context) throws IOException {
      super(context);
      if (termsEnum != null) {
        nextTerm();//advance to first
      }
    }

    BytesRef seekTerm = new BytesRef();//temp; see seek()
    BytesRef thisTerm;//current term in termsEnum
    Cell indexedCell;//the cell wrapper around thisTerm

    /** This is the primary algorithm; recursive.  Returns null if finds none. */
    private SmallDocSet visit(Cell cell, Bits acceptContains) throws IOException {

      if (thisTerm == null)//signals all done
        return null;

      // Get the AND of all child results (into combinedSubResults)
      SmallDocSet combinedSubResults = null;
      //   Optimization: use null subCellsFilter when we know cell is within the query shape.
      Shape subCellsFilter = queryShape;
      if (cell.getLevel() != 0 && ((cell.getShapeRel() == null || cell.getShapeRel() == SpatialRelation.WITHIN))) {
        subCellsFilter = null;
        assert cell.getShape().relate(queryShape) == SpatialRelation.WITHIN;
      }
      CellIterator subCells = cell.getNextLevelCells(subCellsFilter);
      while (subCells.hasNext()) {
        Cell subCell = subCells.next();
        if (!seek(subCell)) {
          combinedSubResults = null;
        } else if (subCell.getLevel() == detailLevel) {
          combinedSubResults = getDocs(subCell, acceptContains);
        } else if (!multiOverlappingIndexedShapes &&
            subCell.getShapeRel() == SpatialRelation.WITHIN) {
          combinedSubResults = getLeafDocs(subCell, acceptContains);
        } else {
          //OR the leaf docs with all child results
          SmallDocSet leafDocs = getLeafDocs(subCell, acceptContains);
          SmallDocSet subDocs = visit(subCell, acceptContains); //recursion
          combinedSubResults = union(leafDocs, subDocs);
        }

        if (combinedSubResults == null)
          break;
        acceptContains = combinedSubResults;//has the 'AND' effect on next iteration
      }

      return combinedSubResults;
    }

    private boolean seek(Cell cell) throws IOException {
      if (thisTerm == null)
        return false;
      final int compare = indexedCell.compareToNoLeaf(cell);
      if (compare > 0) {
        return false;//leap-frog effect
      } else if (compare == 0) {
        return true; // already there!
      } else {//compare > 0
        //seek!
        seekTerm = cell.getTokenBytesNoLeaf(seekTerm);
        final TermsEnum.SeekStatus seekStatus = termsEnum.seekCeil(seekTerm);
        if (seekStatus == TermsEnum.SeekStatus.END) {
          thisTerm = null;//all done
          return false;
        }
        thisTerm = termsEnum.term();
        indexedCell = grid.readCell(thisTerm, indexedCell);
        if (seekStatus == TermsEnum.SeekStatus.FOUND) {
          return true;
        }
        return indexedCell.isLeaf() && indexedCell.compareToNoLeaf(cell) == 0;
      }
    }

    /** Get prefix & leaf docs at this cell. */
    private SmallDocSet getDocs(Cell cell, Bits acceptContains) throws IOException {
      assert indexedCell.compareToNoLeaf(cell) == 0;
      //called when we've reached detailLevel.
      if (indexedCell.isLeaf()) {//only a leaf
        SmallDocSet result = collectDocs(acceptContains);
        nextTerm();
        return result;
      } else {
        SmallDocSet docsAtPrefix = collectDocs(acceptContains);
        if (!nextTerm()) {
          return docsAtPrefix;
        }
        //collect leaf too
        if (indexedCell.isLeaf() && indexedCell.compareToNoLeaf(cell) == 0) {
          SmallDocSet docsAtLeaf = collectDocs(acceptContains);
          nextTerm();
          return union(docsAtPrefix, docsAtLeaf);
        } else {
          return docsAtPrefix;
        }
      }
    }

    /** Gets docs on the leaf of the given cell, _if_ there is a leaf cell, otherwise null. */
    private SmallDocSet getLeafDocs(Cell cell, Bits acceptContains) throws IOException {
      assert indexedCell.compareToNoLeaf(cell) == 0;
      //Advance past prefix if we're at a prefix; return null if no leaf
      if (!indexedCell.isLeaf()) {
        if (!nextTerm() || !indexedCell.isLeaf() || indexedCell.getLevel() != cell.getLevel()) {
          return null;
        }
      }
      SmallDocSet result = collectDocs(acceptContains);
      nextTerm();
      return result;
    }

    private boolean nextTerm() throws IOException {
      if ((thisTerm = termsEnum.next()) == null)
        return false;
      indexedCell = grid.readCell(thisTerm, indexedCell);
      return true;
    }

    private SmallDocSet union(SmallDocSet aSet, SmallDocSet bSet) {
      if (bSet != null) {
        if (aSet == null)
          return bSet;
        return aSet.union(bSet);//union is 'or'
      }
      return aSet;
    }

    private SmallDocSet collectDocs(Bits acceptContains) throws IOException {
      SmallDocSet set = null;

      postingsEnum = termsEnum.postings(postingsEnum, PostingsEnum.NONE);
      int docid;
      while ((docid = postingsEnum.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
        if (acceptContains != null && acceptContains.get(docid) == false) {
          continue;
        }
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
  // TODO use DocIdSetBuilder?
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
          if (idx < 0) {
            return -1;
          } else if (idx < size) {
            return docs[idx];
          } else {
            return NO_MORE_DOCS;
          }
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

    @Override
    public long ramBytesUsed() {
      return RamUsageEstimator.alignObjectSize(
            RamUsageEstimator.NUM_BYTES_OBJECT_REF
          + RamUsageEstimator.NUM_BYTES_INT)
          + intSet.ramBytesUsed();
    }

  }//class SmallDocSet

}
