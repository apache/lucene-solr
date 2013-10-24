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

import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.distance.DistanceUtils;
import com.spatial4j.core.shape.Circle;
import com.spatial4j.core.shape.Point;
import com.spatial4j.core.shape.Rectangle;
import com.spatial4j.core.shape.Shape;
import com.spatial4j.core.shape.SpatialRelation;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.spatial.prefix.tree.Cell;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

/**
 * Finds docs where its indexed shape is {@link org.apache.lucene.spatial.query.SpatialOperation#IsWithin
 * WITHIN} the query shape.  It works by looking at cells outside of the query
 * shape to ensure documents there are excluded. By default, it will
 * examine all cells, and it's fairly slow.  If you know that the indexed shapes
 * are never comprised of multiple disjoint parts (which also means it is not multi-valued),
 * then you can pass {@code SpatialPrefixTree.getDistanceForLevel(maxLevels)} as
 * the {@code queryBuffer} constructor parameter to minimally look this distance
 * beyond the query shape's edge.  Even if the indexed shapes are sometimes
 * comprised of multiple disjoint parts, you might want to use this option with
 * a large buffer as a faster approximation with minimal false-positives.
 *
 * @lucene.experimental
 */
//TODO LUCENE-4869: implement faster algorithm based on filtering out false-positives of a
//  minimal query buffer by looking in a DocValues cache holding a representative
//  point of each disjoint component of a document's shape(s).
public class WithinPrefixTreeFilter extends AbstractVisitingPrefixTreeFilter {

  private final Shape bufferedQueryShape;//if null then the whole world

  /**
   * See {@link AbstractVisitingPrefixTreeFilter#AbstractVisitingPrefixTreeFilter(com.spatial4j.core.shape.Shape, String, org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree, int, int)}.
   * {@code queryBuffer} is the (minimum) distance beyond the query shape edge
   * where non-matching documents are looked for so they can be excluded. If
   * -1 is used then the whole world is examined (a good default for correctness).
   */
  public WithinPrefixTreeFilter(Shape queryShape, String fieldName, SpatialPrefixTree grid,
                                int detailLevel, int prefixGridScanLevel, double queryBuffer) {
    super(queryShape, fieldName, grid, detailLevel, prefixGridScanLevel);
    if (queryBuffer == -1)
      this.bufferedQueryShape = null;
    else
      this.bufferedQueryShape = bufferShape(queryShape, queryBuffer);
  }

  /** Returns a new shape that is larger than shape by at distErr.
   */
  //TODO move this generic code elsewhere?  Spatial4j?
  protected Shape bufferShape(Shape shape, double distErr) {
    if (distErr <= 0)
      throw new IllegalArgumentException("distErr must be > 0");
    SpatialContext ctx = grid.getSpatialContext();
    if (shape instanceof Point) {
      return ctx.makeCircle((Point)shape, distErr);
    } else if (shape instanceof Circle) {
      Circle circle = (Circle) shape;
      double newDist = circle.getRadius() + distErr;
      if (ctx.isGeo() && newDist > 180)
        newDist = 180;
      return ctx.makeCircle(circle.getCenter(), newDist);
    } else {
      Rectangle bbox = shape.getBoundingBox();
      double newMinX = bbox.getMinX() - distErr;
      double newMaxX = bbox.getMaxX() + distErr;
      double newMinY = bbox.getMinY() - distErr;
      double newMaxY = bbox.getMaxY() + distErr;
      if (ctx.isGeo()) {
        if (newMinY < -90)
          newMinY = -90;
        if (newMaxY > 90)
          newMaxY = 90;
        if (newMinY == -90 || newMaxY == 90 || bbox.getWidth() + 2*distErr > 360) {
          newMinX = -180;
          newMaxX = 180;
        } else {
          newMinX = DistanceUtils.normLonDEG(newMinX);
          newMaxX = DistanceUtils.normLonDEG(newMaxX);
        }
      } else {
        //restrict to world bounds
        newMinX = Math.max(newMinX, ctx.getWorldBounds().getMinX());
        newMaxX = Math.min(newMaxX, ctx.getWorldBounds().getMaxX());
        newMinY = Math.max(newMinY, ctx.getWorldBounds().getMinY());
        newMaxY = Math.min(newMaxY, ctx.getWorldBounds().getMaxY());
      }
      return ctx.makeRectangle(newMinX, newMaxX, newMinY, newMaxY);
    }
  }


  @Override
  public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {
    return new VisitorTemplate(context, acceptDocs, true) {
      private FixedBitSet inside;
      private FixedBitSet outside;
      private SpatialRelation visitRelation;

      @Override
      protected void start() {
        inside = new FixedBitSet(maxDoc);
        outside = new FixedBitSet(maxDoc);
      }

      @Override
      protected DocIdSet finish() {
        inside.andNot(outside);
        return inside;
      }

      @Override
      protected Iterator<Cell> findSubCellsToVisit(Cell cell) {
        //use buffered query shape instead of orig.  Works with null too.
        return cell.getSubCells(bufferedQueryShape).iterator();
      }

      @Override
      protected boolean visit(Cell cell) throws IOException {
        //cell.relate is based on the bufferedQueryShape; we need to examine what
        // the relation is against the queryShape
        visitRelation = cell.getShape().relate(queryShape);
        if (visitRelation == SpatialRelation.WITHIN) {
          collectDocs(inside);
          return false;
        } else if (visitRelation == SpatialRelation.DISJOINT) {
          collectDocs(outside);
          return false;
        } else if (cell.getLevel() == detailLevel) {
          collectDocs(inside);
          return false;
        }
        return true;
      }

      @Override
      protected void visitLeaf(Cell cell) throws IOException {
        //visitRelation is declared as a field, populated by visit() so we don't recompute it
        assert detailLevel != cell.getLevel();
        assert visitRelation == cell.getShape().relate(queryShape);
        if (allCellsIntersectQuery(cell, visitRelation))
          collectDocs(inside);
        else
          collectDocs(outside);
      }

      /** Returns true if the provided cell, and all its sub-cells down to
       * detailLevel all intersect the queryShape.
       */
      private boolean allCellsIntersectQuery(Cell cell, SpatialRelation relate/*cell to query*/) {
        if (relate == null)
          relate = cell.getShape().relate(queryShape);
        if (cell.getLevel() == detailLevel)
          return relate.intersects();
        if (relate == SpatialRelation.WITHIN)
          return true;
        if (relate == SpatialRelation.DISJOINT)
          return false;
        // Note: Generating all these cells just to determine intersection is not ideal.
        // It was easy to implement but could be optimized. For example if the docs
        // in question are already marked in the 'outside' bitset then it can be avoided.
        Collection<Cell> subCells = cell.getSubCells(null);
        for (Cell subCell : subCells) {
          if (!allCellsIntersectQuery(subCell, null))//recursion
            return false;
        }
        return true;
      }

      @Override
      protected void visitScanned(Cell cell) throws IOException {
        if (allCellsIntersectQuery(cell, null)) {
          collectDocs(inside);
        } else {
          collectDocs(outside);
        }
      }

    }.getDocIdSet();
  }

}
