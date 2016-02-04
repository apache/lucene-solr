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
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.spatial.prefix.tree.Cell;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;
import org.apache.lucene.util.Bits;

/**
 * Computes facets on cells for {@link org.apache.lucene.spatial.prefix.PrefixTreeStrategy}.
 * <p>
 * <em>NOTE:</em> If for a given document and a given field using
 * {@link org.apache.lucene.spatial.prefix.RecursivePrefixTreeStrategy}
 * multiple values are indexed (i.e. multi-valued) and at least one of them is a non-point, then there is a possibility
 * of double-counting the document in the facet results.  Since each shape is independently turned into grid cells at
 * a resolution chosen by the shape's size, it's possible they will be indexed at different resolutions.  This means
 * the document could be present in BOTH the postings for a cell in both its prefix and leaf variants.  To avoid this,
 * use a single valued field with a {@link com.spatial4j.core.shape.ShapeCollection} (or WKT equivalent).  Or
 * calculate a suitable level/distErr to index both and call
 * {@link org.apache.lucene.spatial.prefix.PrefixTreeStrategy#createIndexableFields(com.spatial4j.core.shape.Shape, int)}
 * with the same value for all shapes for a given document/field.
 *
 * @lucene.experimental
 */
public class PrefixTreeFacetCounter {

  /** A callback/visitor of facet counts. */
  public static abstract class FacetVisitor {
    /** Called at the start of the segment, if there is indexed data. */
    public void startOfSegment() {}

    /** Called for cells with a leaf, or cells at the target facet level.  {@code count} is greater than zero.
     * When an ancestor cell is given with non-zero count, the count can be considered to be added to all cells
     * below. You won't necessarily get a cell at level {@code facetLevel} if the indexed data is courser (bigger).
     */
    public abstract void visit(Cell cell, int count);
  }

  private PrefixTreeFacetCounter() {
  }

  /**
   * Computes facets using a callback/visitor style design, allowing flexibility for the caller to determine what to do
   * with each underlying count.
   * @param strategy the prefix tree strategy (contains the field reference, grid, max levels)
   * @param context the IndexReader's context
   * @param topAcceptDocs a Bits to limit counted docs. If null, live docs are counted.
   * @param queryShape the shape to limit the range of facet counts to
   * @param facetLevel the maximum depth (detail) of faceted cells
   * @param facetVisitor the visitor/callback to receive the counts
   */
  public static void compute(PrefixTreeStrategy strategy, IndexReaderContext context, final Bits topAcceptDocs,
                             Shape queryShape, int facetLevel, FacetVisitor facetVisitor)
      throws IOException {
    //We collect per-leaf
    for (final LeafReaderContext leafCtx : context.leaves()) {
      //determine leaf acceptDocs Bits
      Bits leafAcceptDocs;
      if (topAcceptDocs == null) {
        leafAcceptDocs = leafCtx.reader().getLiveDocs();//filter deleted
      } else {
        leafAcceptDocs = new Bits() {
          @Override
          public boolean get(int index) {
            return topAcceptDocs.get(leafCtx.docBase + index);
          }

          @Override
          public int length() {
            return leafCtx.reader().maxDoc();
          }
        };
      }

      compute(strategy, leafCtx, leafAcceptDocs, queryShape, facetLevel, facetVisitor);
    }
  }

  /** Lower-level per-leaf segment method. */
  public static void compute(final PrefixTreeStrategy strategy, final LeafReaderContext context, final Bits acceptDocs,
                             final Shape queryShape, final int facetLevel, final FacetVisitor facetVisitor)
      throws IOException {
    if (acceptDocs != null && acceptDocs.length() != context.reader().maxDoc()) {
      throw new IllegalArgumentException(
          "acceptDocs bits length " + acceptDocs.length() +" != leaf maxdoc " + context.reader().maxDoc());
    }
    final SpatialPrefixTree tree = strategy.getGrid();

    //scanLevel is an optimization knob of AbstractVisitingPrefixTreeFilter. It's unlikely
    // another scanLevel would be much faster and it tends to be a risky knob (can help a little, can hurt a ton).
    // TODO use RPT's configured scan level?  Do we know better here?  Hard to say.
    final int scanLevel = tree.getMaxLevels();
    //AbstractVisitingPrefixTreeFilter is a Lucene Filter.  We don't need a filter; we use it for its great prefix-tree
    // traversal code.  TODO consider refactoring if/when it makes sense (more use cases than this)
    new AbstractVisitingPrefixTreeQuery(queryShape, strategy.getFieldName(), tree, facetLevel, scanLevel) {
      
      @Override
      public String toString(String field) {
        return "anonPrefixTreeQuery";//un-used
      }

      @Override
      public DocIdSet getDocIdSet(LeafReaderContext contexts) throws IOException {
        assert facetLevel == super.detailLevel;//same thing, FYI. (constant)

        return new VisitorTemplate(context) {

          @Override
          protected void start() throws IOException {
            facetVisitor.startOfSegment();
          }

          @Override
          protected DocIdSet finish() throws IOException {
            return null;//unused;
          }

          @Override
          protected boolean visitPrefix(Cell cell) throws IOException {
            // At facetLevel...
            if (cell.getLevel() == facetLevel) {
              // Count docs
              visitLeaf(cell);//we're not a leaf but we treat it as such at facet level
              return false;//don't descend further; this is enough detail
            }

            // We optimize for discriminating filters (reflected in acceptDocs) and short-circuit if no
            // matching docs. We could do this at all levels or never but the closer we get to the facet level, the
            // higher the probability this is worthwhile. We do when docFreq == 1 because it's a cheap check, especially
            // due to "pulsing" in the codec.
            //TODO this opt should move to VisitorTemplate (which contains an optimization TODO to this effect)
            if (cell.getLevel() == facetLevel - 1 || termsEnum.docFreq() == 1) {
              if (!hasDocsAtThisTerm()) {
                return false;
              }
            }
            return true;
          }

          @Override
          protected void visitLeaf(Cell cell) throws IOException {
            final int count = countDocsAtThisTerm();
            if (count > 0) {
              facetVisitor.visit(cell, count);
            }
          }

          private int countDocsAtThisTerm() throws IOException {
            if (acceptDocs == null) {
              return termsEnum.docFreq();
            }
            int count = 0;
            postingsEnum = termsEnum.postings(postingsEnum, PostingsEnum.NONE);
            while (postingsEnum.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
              if (acceptDocs.get(postingsEnum.docID()) == false) {
                continue;
              }
              count++;
            }
            return count;
          }

          private boolean hasDocsAtThisTerm() throws IOException {
            if (acceptDocs == null) {
              return true;
            }
            postingsEnum = termsEnum.postings(postingsEnum, PostingsEnum.NONE);
            int nextDoc = postingsEnum.nextDoc();
            while (nextDoc != DocIdSetIterator.NO_MORE_DOCS && acceptDocs.get(nextDoc) == false) {
              nextDoc = postingsEnum.nextDoc();
            }
            return nextDoc != DocIdSetIterator.NO_MORE_DOCS;
          }

        }.getDocIdSet();
      }
    }.getDocIdSet(context);
  }
}
