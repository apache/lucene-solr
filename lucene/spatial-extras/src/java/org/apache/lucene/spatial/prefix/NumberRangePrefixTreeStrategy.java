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

import static org.apache.lucene.spatial.prefix.tree.NumberRangePrefixTree.UnitNRShape;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.spatial.prefix.tree.Cell;
import org.apache.lucene.spatial.prefix.tree.NumberRangePrefixTree;
import org.apache.lucene.util.Bits;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.Shape;

/**
 * A PrefixTree based on Number/Date ranges. This isn't very "spatial" on the surface (to the user)
 * but it's implemented using spatial so that's why it's here extending a SpatialStrategy. When
 * using this class, you will use various utility methods on the prefix tree implementation to
 * convert objects/strings to/from shapes.
 *
 * <p>To use with dates, pass in {@link org.apache.lucene.spatial.prefix.tree.DateRangePrefixTree}.
 *
 * @lucene.experimental
 */
public class NumberRangePrefixTreeStrategy extends RecursivePrefixTreeStrategy {

  public NumberRangePrefixTreeStrategy(NumberRangePrefixTree prefixTree, String fieldName) {
    super(prefixTree, fieldName);
    setPruneLeafyBranches(false);
    setPrefixGridScanLevel(prefixTree.getMaxLevels() - 2); // user might want to change, however
    setPointsOnly(false);
    setDistErrPct(0);
  }

  @Override
  public NumberRangePrefixTree getGrid() {
    return (NumberRangePrefixTree) super.getGrid();
  }

  @Override
  protected boolean isPointShape(Shape shape) {
    if (shape instanceof NumberRangePrefixTree.UnitNRShape) {
      return ((NumberRangePrefixTree.UnitNRShape) shape).getLevel() == grid.getMaxLevels();
    } else {
      return false;
    }
  }

  @Override
  protected boolean isGridAlignedShape(Shape shape) {
    // any UnitNRShape other than the world is a single cell/term
    if (shape instanceof NumberRangePrefixTree.UnitNRShape) {
      return ((NumberRangePrefixTree.UnitNRShape) shape).getLevel() > 0;
    } else {
      return false;
    }
  }

  /** Unsupported. */
  @Override
  public DoubleValuesSource makeDistanceValueSource(Point queryPoint, double multiplier) {
    throw new UnsupportedOperationException();
  }

  /**
   * Calculates facets between {@code start} and {@code end} to a detail level one greater than that
   * provided by the arguments. For example providing March to October of 2014 would return facets
   * to the day level of those months. This is just a convenience method.
   *
   * @see #calcFacets(IndexReaderContext, Bits, Shape, int)
   */
  public Facets calcFacets(
      IndexReaderContext context, Bits topAcceptDocs, UnitNRShape start, UnitNRShape end)
      throws IOException {
    Shape facetRange = getGrid().toRangeShape(start, end);
    int detailLevel = Math.max(start.getLevel(), end.getLevel()) + 1;
    return calcFacets(context, topAcceptDocs, facetRange, detailLevel);
  }

  /**
   * Calculates facets (aggregated counts) given a range shape (start-end span) and a level, which
   * specifies the detail. To get the level of an existing shape, say a Calendar, call {@link
   * org.apache.lucene.spatial.prefix.tree.NumberRangePrefixTree#toUnitShape(Object)} then call
   * {@link org.apache.lucene.spatial.prefix.tree.NumberRangePrefixTree.UnitNRShape#getLevel()}.
   * Facet computation is implemented by navigating the underlying indexed terms efficiently.
   */
  public Facets calcFacets(
      IndexReaderContext context, Bits topAcceptDocs, Shape facetRange, final int level)
      throws IOException {
    final Facets facets = new Facets(level);
    PrefixTreeFacetCounter.compute(
        this,
        context,
        topAcceptDocs,
        facetRange,
        level,
        new PrefixTreeFacetCounter.FacetVisitor() {
          Facets.FacetParentVal parentFacet;
          UnitNRShape parentShape;

          @Override
          public void visit(Cell cell, int count) {
            if (cell.getLevel()
                < level - 1) { // some ancestor of parent facet level, direct or distant
              parentFacet = null; // reset
              parentShape = null; // reset
              facets.topLeaves += count;
            } else if (cell.getLevel() == level - 1) { // parent
              // set up FacetParentVal
              setupParent((UnitNRShape) cell.getShape());
              parentFacet.parentLeaves += count;
            } else { // at facet level
              UnitNRShape unitShape = (UnitNRShape) cell.getShape();
              UnitNRShape unitShapeParent = unitShape.getShapeAtLevel(unitShape.getLevel() - 1);
              if (parentFacet == null || !parentShape.equals(unitShapeParent)) {
                setupParent(unitShapeParent);
              }
              // lazy init childCounts
              if (parentFacet.childCounts == null) {
                parentFacet.childCounts = new int[parentFacet.childCountsLen];
              }
              parentFacet.childCounts[unitShape.getValAtLevel(cell.getLevel())] += count;
            }
          }

          private void setupParent(UnitNRShape unitShape) {
            parentShape = unitShape.clone();
            // Look for existing parentFacet (from previous segment), or create anew if needed
            parentFacet = facets.parents.get(parentShape);
            if (parentFacet == null) { // didn't find one; make a new one
              parentFacet = new Facets.FacetParentVal();
              parentFacet.childCountsLen = getGrid().getNumSubCells(parentShape);
              facets.parents.put(parentShape, parentFacet);
            }
          }
        });
    return facets;
  }

  /** Facet response information */
  public static class Facets {
    // TODO consider a variable-level structure -- more general purpose.

    public Facets(int detailLevel) {
      this.detailLevel = detailLevel;
    }

    /** The bottom-most detail-level counted, as requested. */
    public final int detailLevel;

    /**
     * The count of documents with ranges that completely spanned the parents of the detail level.
     * In more technical terms, this is the count of leaf cells 2 up and higher from the bottom.
     * Usually you only care about counts at detailLevel, and so you will add this number to all
     * other counts below, including to omitted/implied children counts of 0. If there are no
     * indexed ranges (just instances, i.e. fully specified dates) then this value will always be 0.
     */
    public int topLeaves;

    /**
     * Holds all the {@link FacetParentVal} instances in order of the key. This is sparse; there
     * won't be an instance if it's count and children are all 0. The keys are {@link
     * org.apache.lucene.spatial.prefix.tree.NumberRangePrefixTree.UnitNRShape} shapes, which can be
     * converted back to the original Object (i.e. a Calendar) via {@link
     * NumberRangePrefixTree#toObject(org.apache.lucene.spatial.prefix.tree.NumberRangePrefixTree.UnitNRShape)}.
     */
    public final SortedMap<UnitNRShape, FacetParentVal> parents = new TreeMap<>();

    /** Holds a block of detailLevel counts aggregated to their parent level. */
    public static class FacetParentVal {

      /**
       * The count of ranges that span all of the childCounts. In more technical terms, this is the
       * number of leaf cells found at this parent. Treat this like {@link Facets#topLeaves}.
       */
      public int parentLeaves;

      /**
       * The length of {@link #childCounts}. If childCounts is not null then this is
       * childCounts.length, otherwise it says how long it would have been if it weren't null.
       */
      public int childCountsLen;

      /**
       * The detail level counts. It will be null if there are none, and thus they are assumed 0.
       * Most apps, when presenting the information, will add {@link #topLeaves} and {@link
       * #parentLeaves} to each count.
       */
      public int[] childCounts;
      // assert childCountsLen == childCounts.length
    }

    @Override
    public String toString() {
      StringBuilder buf = new StringBuilder(2048);
      buf.append("Facets: level=")
          .append(detailLevel)
          .append(" topLeaves=")
          .append(topLeaves)
          .append(" parentCount=")
          .append(parents.size());
      for (Map.Entry<UnitNRShape, FacetParentVal> entry : parents.entrySet()) {
        buf.append('\n');
        if (buf.length() > 1000) {
          buf.append("...");
          break;
        }
        final FacetParentVal pVal = entry.getValue();
        buf.append(' ').append(entry.getKey()).append(" leafCount=").append(pVal.parentLeaves);
        if (pVal.childCounts != null) {
          buf.append(' ').append(Arrays.toString(pVal.childCounts));
        }
      }
      return buf.toString();
    }
  }
}
