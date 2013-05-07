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
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.spatial.prefix.tree.Cell;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.StringHelper;

import java.io.IOException;
import java.util.Iterator;

/**
 * Traverses a {@link SpatialPrefixTree} indexed field, using the template &
 * visitor design patterns for subclasses to guide the traversal and collect
 * matching documents.
 * <p/>
 * Subclasses implement {@link #getDocIdSet(org.apache.lucene.index.AtomicReaderContext,
 * org.apache.lucene.util.Bits)} by instantiating a custom {@link
 * VisitorTemplate} subclass (i.e. an anonymous inner class) and implement the
 * required methods.
 *
 * @lucene.internal
 */
public abstract class AbstractVisitingPrefixTreeFilter extends AbstractPrefixTreeFilter {

  //Historical note: this code resulted from a refactoring of RecursivePrefixTreeFilter,
  // which in turn came out of SOLR-2155

  protected final int prefixGridScanLevel;//at least one less than grid.getMaxLevels()

  public AbstractVisitingPrefixTreeFilter(Shape queryShape, String fieldName, SpatialPrefixTree grid,
                                          int detailLevel, int prefixGridScanLevel) {
    super(queryShape, fieldName, grid, detailLevel);
    this.prefixGridScanLevel = Math.max(0, Math.min(prefixGridScanLevel, grid.getMaxLevels() - 1));
    assert detailLevel <= grid.getMaxLevels();
  }

  @Override
  public boolean equals(Object o) {
    if (!super.equals(o)) return false;//checks getClass == o.getClass & instanceof

    AbstractVisitingPrefixTreeFilter that = (AbstractVisitingPrefixTreeFilter) o;

    if (prefixGridScanLevel != that.prefixGridScanLevel) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + prefixGridScanLevel;
    return result;
  }

  /**
   * An abstract class designed to make it easy to implement predicates or
   * other operations on a {@link SpatialPrefixTree} indexed field. An instance
   * of this class is not designed to be re-used across AtomicReaderContext
   * instances so simply create a new one for each call to, say a {@link
   * org.apache.lucene.search.Filter#getDocIdSet(org.apache.lucene.index.AtomicReaderContext, org.apache.lucene.util.Bits)}.
   * The {@link #getDocIdSet()} method here starts the work. It first checks
   * that there are indexed terms; if not it quickly returns null. Then it calls
   * {@link #start()} so a subclass can set up a return value, like an
   * {@link org.apache.lucene.util.OpenBitSet}. Then it starts the traversal
   * process, calling {@link #findSubCellsToVisit(org.apache.lucene.spatial.prefix.tree.Cell)}
   * which by default finds the top cells that intersect {@code queryShape}. If
   * there isn't an indexed cell for a corresponding cell returned for this
   * method then it's short-circuited until it finds one, at which point
   * {@link #visit(org.apache.lucene.spatial.prefix.tree.Cell)} is called. At
   * some depths, of the tree, the algorithm switches to a scanning mode that
   * finds calls {@link #visitScanned(org.apache.lucene.spatial.prefix.tree.Cell)}
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

  */

    protected final boolean hasIndexedLeaves;//if false then we can skip looking for them

    private VNode curVNode;//current pointer, derived from query shape
    private BytesRef curVNodeTerm = new BytesRef();//curVNode.cell's term.
    private Cell scanCell;

    private BytesRef thisTerm;//the result of termsEnum.term()

    public VisitorTemplate(AtomicReaderContext context, Bits acceptDocs,
                           boolean hasIndexedLeaves) throws IOException {
      super(context, acceptDocs);
      this.hasIndexedLeaves = hasIndexedLeaves;
    }

    public DocIdSet getDocIdSet() throws IOException {
      assert curVNode == null : "Called more than once?";
      if (termsEnum == null)
        return null;
      //advance
      if ((thisTerm = termsEnum.next()) == null)
        return null; // all done

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
        curVNodeTerm.bytes = curVNode.cell.getTokenBytes();
        curVNodeTerm.length = curVNodeTerm.bytes.length;
        int compare = termsEnum.getComparator().compare(thisTerm, curVNodeTerm);
        if (compare > 0) {
          // leap frog (termsEnum is beyond where we would otherwise seek)
          assert ! context.reader().terms(fieldName).iterator(null).seekExact(curVNodeTerm, false) : "should be absent";
        } else {
          if (compare < 0) {
            // Seek !
            TermsEnum.SeekStatus seekStatus = termsEnum.seekCeil(curVNodeTerm, true);
            if (seekStatus == TermsEnum.SeekStatus.END)
              break; // all done
            thisTerm = termsEnum.term();
            if (seekStatus == TermsEnum.SeekStatus.NOT_FOUND) {
              continue; // leap frog
            }
          }
          // Visit!
          boolean descend = visit(curVNode.cell);
          //advance
          if ((thisTerm = termsEnum.next()) == null)
            break; // all done
          if (descend)
            addIntersectingChildren();

        }

      }//main loop

      return finish();
    }

    /** Called initially, and whenever {@link #visit(org.apache.lucene.spatial.prefix.tree.Cell)}
     * returns true. */
    private void addIntersectingChildren() throws IOException {
      assert thisTerm != null;
      Cell cell = curVNode.cell;
      if (cell.getLevel() >= detailLevel)
        throw new IllegalStateException("Spatial logic error");

      //Check for adjacent leaf (happens for indexed non-point shapes)
      if (hasIndexedLeaves && cell.getLevel() != 0) {
        //If the next indexed term just adds a leaf marker ('+') to cell,
        // then add all of those docs
        assert StringHelper.startsWith(thisTerm, curVNodeTerm);
        scanCell = grid.getCell(thisTerm.bytes, thisTerm.offset, thisTerm.length, scanCell);
        if (scanCell.getLevel() == cell.getLevel() && scanCell.isLeaf()) {
          visitLeaf(scanCell);
          //advance
          if ((thisTerm = termsEnum.next()) == null)
            return; // all done
        }
      }

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
     * Called when doing a divide & conquer to find the next intersecting cells
     * of the query shape that are beneath {@code cell}. {@code cell} is
     * guaranteed to have an intersection and thus this must return some number
     * of nodes.
     */
    protected Iterator<Cell> findSubCellsToVisit(Cell cell) {
      return cell.getSubCells(queryShape).iterator();
    }

    /**
     * Scans ({@code termsEnum.next()}) terms until a term is found that does
     * not start with curVNode's cell. If it finds a leaf cell or a cell at
     * level {@code scanDetailLevel} then it calls {@link
     * #visitScanned(org.apache.lucene.spatial.prefix.tree.Cell)}.
     */
    protected void scan(int scanDetailLevel) throws IOException {
      for (;
           thisTerm != null && StringHelper.startsWith(thisTerm, curVNodeTerm);
           thisTerm = termsEnum.next()) {
        scanCell = grid.getCell(thisTerm.bytes, thisTerm.offset, thisTerm.length, scanCell);

        int termLevel = scanCell.getLevel();
        if (termLevel > scanDetailLevel)
          continue;
        if (termLevel == scanDetailLevel || scanCell.isLeaf()) {
          visitScanned(scanCell);
        }
      }//term loop
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
     * Visit an indexed cell returned from
     * {@link #findSubCellsToVisit(org.apache.lucene.spatial.prefix.tree.Cell)}.
     *
     * @param cell An intersecting cell.
     * @return true to descend to more levels. It is an error to return true
     * if cell.level == detailLevel
     */
    protected abstract boolean visit(Cell cell) throws IOException;

    /**
     * Called after visit() returns true and an indexed leaf cell is found. An
     * indexed leaf cell means associated documents generally won't be found at
     * further detail levels.
     */
    protected abstract void visitLeaf(Cell cell) throws IOException;

    /**
     * The cell is either indexed as a leaf or is the last level of detail. It
     * might not even intersect the query shape, so be sure to check for that.
     */
    protected abstract void visitScanned(Cell cell) throws IOException;


    protected void preSiblings(VNode vNode) throws IOException {
    }

    protected void postSiblings(VNode vNode) throws IOException {
    }
  }//class VisitorTemplate

  /**
   * A Visitor Cell/Cell found via the query shape for {@link VisitorTemplate}.
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
