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

import java.io.IOException;
import java.util.List;

import com.spatial4j.core.shape.Shape;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.spatial.NumberRangePrefixTreeStrategy;
import org.apache.lucene.spatial.NumberRangePrefixTreeStrategy.Facets;
import org.apache.lucene.spatial.prefix.tree.Cell;
import org.apache.lucene.spatial.prefix.tree.NumberRangePrefixTree;
import org.apache.lucene.spatial.prefix.tree.NumberRangePrefixTree.UnitNRShape;
import org.apache.lucene.util.Bits;

/**
 * Computes range facets for {@link NumberRangePrefixTreeStrategy}.
 *
 * @see NumberRangePrefixTreeStrategy#calcFacets(IndexReaderContext, Bits, Shape, int)
 *
 * @lucene.internal
 */
public class NumberRangePrefixTreeFacets {

  public static Facets compute(NumberRangePrefixTreeStrategy strategy,
                               IndexReaderContext context, final Bits acceptDocs, Shape queryShape, int facetLevel)
      throws IOException {

    Facets facets = new Facets(facetLevel);

    // TODO should we pre-create all parent buckets? It's not necessary, but the client/user may find it convenient to
    //   have so it needn't do a bunch of calendar work itself to ascertain which buckets are missing. It would
    //   also then easily allow us to have a too-many-facets exception (e.g. you ask for a millisecond bucket for
    //   the entire year). We could do that now but we would only be able to throw if the actual counts get to the
    //   threshold vs. being able to know the possible values consistently a-priori which is much preferred. Now on the
    //   other hand, we can facet over extremely sparse data sets without needless parent buckets.

    //We collect per-leaf
    final List<LeafReaderContext> leaves = context.leaves();
    for (final LeafReaderContext leafCtx : leaves) {
      //determine leaf acceptDocs
      Bits leafAcceptDocs;
      if (acceptDocs == null) {
        leafAcceptDocs = leafCtx.reader().getLiveDocs();
      } else if (leaves.size() == 1) {
        leafAcceptDocs = acceptDocs;
      } else {
        leafAcceptDocs = new Bits() {//note: it'd be nice if Lucene's BitsSlice was public.

          final int docBase = leafCtx.docBase;

          @Override
          public boolean get(int index) {
            return acceptDocs.get(docBase + index);
          }

          @Override
          public int length() {
            return leafCtx.reader().maxDoc();
          }
        };
      }

      facets = compute(strategy, leafCtx, leafAcceptDocs, queryShape, facets);
    }
    return facets;

  }

  public static Facets compute(final NumberRangePrefixTreeStrategy strategy,
                               LeafReaderContext context, Bits acceptDocs, Shape queryShape, final Facets facets)
      throws IOException {
    final NumberRangePrefixTree tree = strategy.getGrid();
    final int scanLevel = tree.getMaxLevels();

    //TODO extract AbstractVisitingPrefixTreeFilter / VisitorTemplate to be generic, not necessarily a Filter/DocIdSet.
    new AbstractVisitingPrefixTreeFilter(queryShape, strategy.getFieldName(), tree, facets.detailLevel, scanLevel) {

      @Override
      public DocIdSet getDocIdSet(LeafReaderContext context, Bits acceptDocs) throws IOException {
        return new VisitorTemplate(context, acceptDocs, !strategy.pointsOnly) {

          Facets.FacetParentVal parentFacet;

          @Override
          protected void start() throws IOException {
          }

          @Override
          protected DocIdSet finish() throws IOException {
            return null;//unused
          }

          @Override
          protected boolean visit(Cell cell) throws IOException {
            // At facetLevel...
            if (cell.getLevel() == facets.detailLevel) {
              //note: parentFacet shouldn't be null if we get here

              // Count docs
              int count = countDocsAtThisTermInSet(acceptDocs);
              if (count > 0) {
                //lazy init childCounts
                if (parentFacet.childCounts == null) {
                  parentFacet.childCounts = new int[parentFacet.childCountsLen];
                }
                UnitNRShape unitShape = (UnitNRShape) cell.getShape();
                parentFacet.childCounts[unitShape.getValAtLevel(cell.getLevel())] += count;
              }
              return false;//don't descend further; this is enough detail
            }

            parentFacet = null;//reset

            // At parent
            if (cell.getLevel() == facets.detailLevel - 1) {
              if (!hasDocsAtThisTermInSet(acceptDocs)) {
                return false;
              }
              //Look for existing parentFacet (from previous segment)
              UnitNRShape unitShape = (UnitNRShape) cell.getShape();
              UnitNRShape key = unitShape.clone();
              parentFacet = facets.parents.get(key);
              if (parentFacet == null) {//didn't find one; make a new one
                parentFacet = new Facets.FacetParentVal();
                parentFacet.childCountsLen = tree.getNumSubCells(unitShape);
                facets.parents.put(key, parentFacet);
              }
            }
            return true;
          }

          @Override
          protected void visitLeaf(Cell cell) throws IOException {
            final int levelsToGo = facets.detailLevel - cell.getLevel();
            if (levelsToGo <= 0) {
              return;//do nothing; we already collected in visit()
              //note: once we index ranges without direct prefix's of leaves,
              //  we'll need to collect here at levelsToGo==0 too.
            }
            int count = countDocsAtThisTermInSet(acceptDocs);
            if (count == 0) {
              return;
            }
            if (levelsToGo == 1) {
              // Because all leaves also have an indexed non-leaf, we can be sure we have parentCell set via visit().
              parentFacet.parentLeaves += count;
            } else {
              facets.topLeaves += count;
            }

          }

          @Override
          protected void visitScanned(Cell cell) throws IOException {
            //TODO does this belong in superclass?  It ignores boolean result from visit(), but that's ok.
            if (queryShape.relate(cell.getShape()).intersects()) {
              if (cell.isLeaf()) {
                visitLeaf(cell);
              } else {
                visit(cell);
              }
            }
          }

          //TODO These utility methods could move to superclass

          private int countDocsAtThisTermInSet(Bits actualBaseDocs) throws IOException {
            if (actualBaseDocs == null) {
              return termsEnum.docFreq();
            }
            int count = 0;
            docsEnum = termsEnum.docs(actualBaseDocs, docsEnum, DocsEnum.FLAG_NONE);
            while (docsEnum.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
              count++;
            }
            return count;
          }

          private boolean hasDocsAtThisTermInSet(Bits actualBaseDocs) throws IOException {
            if (actualBaseDocs == null) {
              return true;
            }
            docsEnum = termsEnum.docs(actualBaseDocs, docsEnum, DocsEnum.FLAG_NONE);
            return (docsEnum.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
          }

        }.getDocIdSet();
      }
    }.getDocIdSet(context, acceptDocs);

    return facets;
  }

}
