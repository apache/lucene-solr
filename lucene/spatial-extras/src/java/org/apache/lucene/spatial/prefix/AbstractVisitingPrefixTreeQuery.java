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
import java.util.Iterator;

import com.spatial4j.core.shape.Shape;
import com.spatial4j.core.shape.SpatialRelation;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.spatial.prefix.tree.Cell;
import org.apache.lucene.spatial.prefix.tree.CellIterator;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;
import org.apache.lucene.util.BytesRef;

/**
 * Traverses a {@link SpatialPrefixTree} indexed field, using the template and
 * visitor design patterns for subclasses to guide the traversal and collect
 * matching documents.
 * <p>
 * Subclasses implement {@link #getDocIdSet(org.apache.lucene.index.LeafReaderContext)}
 * by instantiating a custom {@link VisitorTemplate} subclass (i.e. an anonymous inner class)
 * and implement the required methods.
 *
 * @lucene.internal
 */
public abstract class AbstractVisitingPrefixTreeQuery extends AbstractPrefixTreeQuery {

  //Historical note: this code resulted from a refactoring of RecursivePrefixTreeQuery,
  // which in turn came out of SOLR-2155

  //This class perhaps could have been implemented in terms of FilteredTermsEnum & MultiTermQuery.
  //  Maybe so for simple Intersects predicate but not for when we want to collect terms
  //  differently depending on cell state like IsWithin and for fuzzy/accurate collection planned improvements.  At
  //  least it would just make things more complicated.

  protected final int prefixGridScanLevel;//at least one less than grid.getMaxLevels()

  public AbstractVisitingPrefixTreeQuery(Shape queryShape, String fieldName, SpatialPrefixTree grid,
                                         int detailLevel, int prefixGridScanLevel) {
    super(queryShape, fieldName, grid, detailLevel);
    this.prefixGridScanLevel = Math.max(0, Math.min(prefixGridScanLevel, grid.getMaxLevels() - 1));
    assert detailLevel <= grid.getMaxLevels();
  }

  /**
   * An abstract class designed to make it easy to implement predicates or
   * other operations on a {@link SpatialPrefixTree} indexed field. An instance
   * of this class is not designed to be re-used across LeafReaderContext
   * instances so simply create a new one per-leaf.
   * The {@link #getDocIdSet()} method here starts the work. It first checks
   * that there are indexed terms; if not it quickly returns null. Then it calls
   * {@link #start()} so a subclass can set up a return value, like an
   * {@link org.apache.lucene.util.FixedBitSet}. Then it starts the traversal
   * process, calling {@link #findSubCellsToVisit(org.apache.lucene.spatial.prefix.tree.Cell)}
   * which by default finds the top cells that intersect {@code queryShape}. If
   * there isn't an indexed cell for a corresponding cell returned for this
   * method then it's short-circuited until it finds one, at which point
   * {@link #visitPrefix(org.apache.lucene.spatial.prefix.tree.Cell)} is called. At
   * some depths, of the tree, the algorithm switches to a scanning mode that
   * calls {@link #visitScanned(org.apache.lucene.spatial.prefix.tree.Cell)}
   * for each leaf cell found.
   *
   * @lucene.internal
   */
  public abstract class VisitorTemplate extends BaseTermsEnumTraverser {

  /* Future potential optimizations:

  * Can a polygon query shape be optimized / made-simpler at recursive depths
    (e.g. intersection of shape + cell box)

  * RE "scan" vs divide & conquer performance decision:
    We should use termsEnum.docFreq() as an estimate on the number of places at
    this depth.  It would be nice if termsEnum knew how many terms
    start with the current term without having to repeatedly next() & test to find out.

  * Perhaps don't do intermediate seek()'s to cells above detailLevel that have Intersects
    relation because we won't be collecting those docs any way.  However seeking
    does act as a short-circuit.  So maybe do some percent of the time or when the level
    is above some threshold.

  */

    //
    //  TODO MAJOR REFACTOR SIMPLIFICATION BASED ON TreeCellIterator  TODO
    //

    private VNode curVNode;//current pointer, derived from query shape
    private BytesRef curVNodeTerm = new BytesRef();//curVNode.cell's term, without leaf. in main loop only

    private BytesRef thisTerm;//the result of termsEnum.term()
    private Cell indexedCell;//Cell wrapper of thisTerm. Always updated when thisTerm is.

    public VisitorTemplate(LeafReaderContext context) throws IOException {
      super(context);
    }

    public DocIdSet getDocIdSet() throws IOException {
      assert curVNode == null : "Called more than once?";
      if (termsEnum == null)
        return null;
      if (!nextTerm()) {//advances
        return null;
      }

      curVNode = new VNode(null);
      curVNode.reset(grid.getWorldCell());

      start();

      addIntersectingChildren();

      main: while (thisTerm != null) {//terminates for other reasons too!

        //Advance curVNode pointer
        if (curVNode.children != null) {
          //-- HAVE CHILDREN: DESCEND
          assert curVNode.children.hasNext();//if we put it there then it has something
          preSiblings(curVNode);
          curVNode = curVNode.children.next();
        } else {
          //-- NO CHILDREN: ADVANCE TO NEXT SIBLING
          VNode parentVNode = curVNode.parent;
          while (true) {
            if (parentVNode == null)
              break main; // all done
            if (parentVNode.children.hasNext()) {
              //advance next sibling
              curVNode = parentVNode.children.next();
              break;
            } else {
              //reached end of siblings; pop up
              postSiblings(parentVNode);
              parentVNode.children = null;//GC
              parentVNode = parentVNode.parent;
            }
          }
        }

        //Seek to curVNode's cell (or skip if termsEnum has moved beyond)
        final int compare = indexedCell.compareToNoLeaf(curVNode.cell);
        if (compare > 0) {
          // The indexed cell is after; continue loop to next query cell
          continue;
        }
        if (compare < 0) {
          // The indexed cell is before; seek ahead to query cell:
          //      Seek !
          curVNode.cell.getTokenBytesNoLeaf(curVNodeTerm);
          TermsEnum.SeekStatus seekStatus = termsEnum.seekCeil(curVNodeTerm);
          if (seekStatus == TermsEnum.SeekStatus.END)
            break; // all done
          thisTerm = termsEnum.term();
          indexedCell = grid.readCell(thisTerm, indexedCell);
          if (seekStatus == TermsEnum.SeekStatus.NOT_FOUND) {
            // Did we find a leaf of the cell we were looking for or something after?
            if (!indexedCell.isLeaf() || indexedCell.compareToNoLeaf(curVNode.cell) != 0)
              continue; // The indexed cell is after; continue loop to next query cell
          }
        }
        // indexedCell == queryCell (disregarding leaf).

        // If indexedCell is a leaf then there's no prefix (prefix sorts before) -- just visit and continue
        if (indexedCell.isLeaf()) {
          visitLeaf(indexedCell);//TODO or query cell? Though shouldn't matter.
          if (!nextTerm()) break;
          continue;
        }
        // If a prefix (non-leaf) then visit; see if we descend.
        final boolean descend = visitPrefix(curVNode.cell);//need to use curVNode.cell not indexedCell
        if (!nextTerm()) break;
        // Check for adjacent leaf with the same prefix
        if (indexedCell.isLeaf() && indexedCell.getLevel() == curVNode.cell.getLevel()) {
          visitLeaf(indexedCell);//TODO or query cell? Though shouldn't matter.
          if (!nextTerm()) break;
        }


        if (descend) {
          addIntersectingChildren();
        }

      }//main loop

      return finish();
    }

    /** Called initially, and whenever {@link #visitPrefix(org.apache.lucene.spatial.prefix.tree.Cell)}
     * returns true. */
    private void addIntersectingChildren() throws IOException {
      assert thisTerm != null;
      Cell cell = curVNode.cell;
      if (cell.getLevel() >= detailLevel)
        throw new IllegalStateException("Spatial logic error");

      //Decide whether to continue to divide & conquer, or whether it's time to
      // scan through terms beneath this cell.
      // Scanning is a performance optimization trade-off.

      //TODO use termsEnum.docFreq() as heuristic
      boolean scan = cell.getLevel() >= prefixGridScanLevel;//simple heuristic

      if (!scan) {
        //Divide & conquer (ultimately termsEnum.seek())

        Iterator<Cell> subCellsIter = findSubCellsToVisit(cell);
        if (!subCellsIter.hasNext())//not expected
          return;
        curVNode.children = new VNodeCellIterator(subCellsIter, new VNode(curVNode));

      } else {
        //Scan (loop of termsEnum.next())

        scan(detailLevel);
      }
    }

    /**
     * Called when doing a divide and conquer to find the next intersecting cells
     * of the query shape that are beneath {@code cell}. {@code cell} is
     * guaranteed to have an intersection and thus this must return some number
     * of nodes.
     */
    protected CellIterator findSubCellsToVisit(Cell cell) {
      return cell.getNextLevelCells(queryShape);
    }

    /**
     * Scans ({@code termsEnum.next()}) terms until a term is found that does
     * not start with curVNode's cell. If it finds a leaf cell or a cell at
     * level {@code scanDetailLevel} then it calls {@link
     * #visitScanned(org.apache.lucene.spatial.prefix.tree.Cell)}.
     */
    protected void scan(int scanDetailLevel) throws IOException {
      //note: this can be a do-while instead in 6x; 5x has a back-compat with redundant leaves -- LUCENE-4942
      while (curVNode.cell.isPrefixOf(indexedCell)) {
        if (indexedCell.getLevel() == scanDetailLevel
            || (indexedCell.getLevel() < scanDetailLevel && indexedCell.isLeaf())) {
          visitScanned(indexedCell);
        }
        //advance
        if (!nextTerm()) break;
      }
    }

    private boolean nextTerm() throws IOException {
      if ((thisTerm = termsEnum.next()) == null)
        return false;
      indexedCell = grid.readCell(thisTerm, indexedCell);
      return true;
    }

    /** Used for {@link VNode#children}. */
    private class VNodeCellIterator implements Iterator<VNode> {

      final Iterator<Cell> cellIter;
      private final VNode vNode;

      VNodeCellIterator(Iterator<Cell> cellIter, VNode vNode) {
        this.cellIter = cellIter;
        this.vNode = vNode;
      }

      @Override
      public boolean hasNext() {
        return cellIter.hasNext();
      }

      @Override
      public VNode next() {
        assert hasNext();
        vNode.reset(cellIter.next());
        return vNode;
      }

      @Override
      public void remove() {//it always removes
      }
    }

    /** Called first to setup things. */
    protected abstract void start() throws IOException;

    /** Called last to return the result. */
    protected abstract DocIdSet finish() throws IOException;

    /**
     * Visit an indexed non-leaf cell. The presence of a prefix cell implies
     * there are leaf cells at further levels. The cell passed should have it's
     * {@link org.apache.lucene.spatial.prefix.tree.Cell#getShapeRel()} set
     * relative to the filtered shape.
     *
     * @param cell An intersecting cell; not a leaf.
     * @return true to descend to more levels.
     */
    protected abstract boolean visitPrefix(Cell cell) throws IOException;

    /**
     * Called when an indexed leaf cell is found. An
     * indexed leaf cell usually means associated documents won't be found at
     * further detail levels.  However, if a document has
     * multiple overlapping shapes at different resolutions, then this isn't true.
     */
    protected abstract void visitLeaf(Cell cell) throws IOException;

    /**
     * The cell is either indexed as a leaf or is the last level of detail. It
     * might not even intersect the query shape, so be sure to check for that.
     * The default implementation will check that and if passes then call
     * {@link #visitLeaf(org.apache.lucene.spatial.prefix.tree.Cell)} or
     * {@link #visitPrefix(org.apache.lucene.spatial.prefix.tree.Cell)}.
     */
    protected void visitScanned(Cell cell) throws IOException {
      final SpatialRelation relate = cell.getShape().relate(queryShape);
      if (relate.intersects()) {
        cell.setShapeRel(relate);//just being pedantic
        if (cell.isLeaf()) {
          visitLeaf(cell);
        } else {
          visitPrefix(cell);
        }
      }
    }

    protected void preSiblings(VNode vNode) throws IOException {
    }

    protected void postSiblings(VNode vNode) throws IOException {
    }
  }//class VisitorTemplate

  /**
   * A visitor node/cell found via the query shape for {@link VisitorTemplate}.
   * Sometimes these are reset(cell). It's like a LinkedList node but forms a
   * tree.
   *
   * @lucene.internal
   */
  protected static class VNode {
    //Note: The VNode tree adds more code to debug/maintain v.s. a flattened
    // LinkedList that we used to have. There is more opportunity here for
    // custom behavior (see preSiblings & postSiblings) but that's not
    // leveraged yet. Maybe this is slightly more GC friendly.

    final VNode parent;//only null at the root
    Iterator<VNode> children;//null, then sometimes set, then null
    Cell cell;//not null (except initially before reset())

    /**
     * call reset(cell) after to set the cell.
     */
    VNode(VNode parent) { // remember to call reset(cell) after
      this.parent = parent;
    }

    void reset(Cell cell) {
      assert cell != null;
      this.cell = cell;
      assert children == null;
    }

  }
}
