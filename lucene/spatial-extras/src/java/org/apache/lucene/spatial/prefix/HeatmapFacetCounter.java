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
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.spatial.prefix.tree.Cell;
import org.apache.lucene.spatial.prefix.tree.CellIterator;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.Rectangle;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.SpatialRelation;

/**
 * Computes spatial facets in two dimensions as a grid of numbers.  The data is often visualized as a so-called
 * "heatmap", hence the name.
 *
 * @lucene.experimental
 */
public class HeatmapFacetCounter {
  //TODO where should this code live? It could go to PrefixTreeFacetCounter, or maybe here in its own class is fine.

  /** Maximum number of supported rows (or columns). */
  public static final int MAX_ROWS_OR_COLUMNS = (int) Math.sqrt(ArrayUtil.MAX_ARRAY_LENGTH);
  static {
    Math.multiplyExact(MAX_ROWS_OR_COLUMNS, MAX_ROWS_OR_COLUMNS);//will throw if doesn't stay within integer
  }

  /** Response structure */
  public static class Heatmap {
    public final int columns;
    public final int rows;
    public final int[] counts;//in order of 1st column (all rows) then 2nd column (all rows) etc.
    public final Rectangle region;

    public Heatmap(int columns, int rows, Rectangle region) {
      this.columns = columns;
      this.rows = rows;
      this.counts = new int[columns * rows];
      this.region = region;
    }

    public int getCount(int x, int y) {
      return counts[x * rows + y];
    }

    @Override
    public String toString() {
      return "Heatmap{" + columns + "x" + rows + " " + region + '}';
    }
  }

  /**
   * Calculates spatial 2D facets (aggregated counts) in a grid, sometimes called a heatmap.
   * Facet computation is implemented by navigating the underlying indexed terms efficiently. If you don't know exactly
   * what facetLevel to go to for a given input box but you have some sense of how many cells there should be relative
   * to the size of the shape, then consider using the logic that {@link org.apache.lucene.spatial.prefix.PrefixTreeStrategy}
   * uses when approximating what level to go to when indexing a shape given a distErrPct.
   *
   * @param context the IndexReader's context
   * @param topAcceptDocs a Bits to limit counted docs.  If null, live docs are counted.
   * @param inputShape the shape to gather grid squares for; typically a {@link Rectangle}.
   *                   The <em>actual</em> heatmap area will usually be larger since the cells on the edge that overlap
   *                   are returned. We always return a rectangle of integers even if the inputShape isn't a rectangle
   *                   -- the non-intersecting cells will all be 0.
   *                   If null is given, the entire world is assumed.
   * @param facetLevel the target depth (detail) of cells.
   * @param maxCells the maximum number of cells to return. If the cells exceed this count, an
   */
  public static Heatmap calcFacets(PrefixTreeStrategy strategy, IndexReaderContext context, Bits topAcceptDocs,
                                   Shape inputShape, final int facetLevel, int maxCells) throws IOException {
    if (maxCells > (MAX_ROWS_OR_COLUMNS * MAX_ROWS_OR_COLUMNS)) {
      throw new IllegalArgumentException("maxCells (" + maxCells + ") should be <= " + MAX_ROWS_OR_COLUMNS);
    }
    if (inputShape == null) {
      inputShape = strategy.getSpatialContext().getWorldBounds();
    }
    final Rectangle inputRect = inputShape.getBoundingBox();
    //First get the rect of the cell at the bottom-left at depth facetLevel
    final SpatialPrefixTree grid = strategy.getGrid();
    final SpatialContext ctx = grid.getSpatialContext();
    final Point cornerPt = ctx.makePoint(inputRect.getMinX(), inputRect.getMinY());
    final CellIterator cellIterator = grid.getTreeCellIterator(cornerPt, facetLevel);
    Cell cornerCell = null;
    while (cellIterator.hasNext()) {
      cornerCell = cellIterator.next();
    }
    assert cornerCell != null && cornerCell.getLevel() == facetLevel : "Cell not at target level: " + cornerCell;
    final Rectangle cornerRect = (Rectangle) cornerCell.getShape();
    assert cornerRect.hasArea();
    //Now calculate the number of columns and rows necessary to cover the inputRect
    double heatMinX = cornerRect.getMinX();//note: we might change this below...
    final double cellWidth = cornerRect.getWidth();
    final Rectangle worldRect = ctx.getWorldBounds();
    final int columns = calcRowsOrCols(cellWidth, heatMinX, inputRect.getWidth(), inputRect.getMinX(), worldRect.getWidth());
    final double heatMinY = cornerRect.getMinY();
    final double cellHeight = cornerRect.getHeight();
    final int rows = calcRowsOrCols(cellHeight, heatMinY, inputRect.getHeight(), inputRect.getMinY(), worldRect.getHeight());
    assert rows > 0 && columns > 0;
    if (columns > MAX_ROWS_OR_COLUMNS || rows > MAX_ROWS_OR_COLUMNS || columns * rows > maxCells) {
      throw new IllegalArgumentException(
          "Too many cells (" + columns + " x " + rows + ") for level " + facetLevel + " shape " + inputRect);
    }

    //Create resulting heatmap bounding rectangle & Heatmap object.
    final double halfCellWidth = cellWidth / 2.0;
    // if X world-wraps, use world bounds' range
    if (columns * cellWidth + halfCellWidth > worldRect.getWidth()) {
      heatMinX = worldRect.getMinX();
    }
    double heatMaxX = heatMinX + columns * cellWidth;
    if (Math.abs(heatMaxX - worldRect.getMaxX()) < halfCellWidth) {//numeric conditioning issue
      heatMaxX = worldRect.getMaxX();
    } else if (heatMaxX > worldRect.getMaxX()) {//wraps dateline (won't happen if !geo)
      heatMaxX = heatMaxX - worldRect.getMaxX() +  worldRect.getMinX();
    }
    final double halfCellHeight = cellHeight / 2.0;
    double heatMaxY = heatMinY + rows * cellHeight;
    if (Math.abs(heatMaxY - worldRect.getMaxY()) < halfCellHeight) {//numeric conditioning issue
      heatMaxY = worldRect.getMaxY();
    }

    final Heatmap heatmap = new Heatmap(columns, rows, ctx.makeRectangle(heatMinX, heatMaxX, heatMinY, heatMaxY));

    //All ancestor cell counts (of facetLevel) will be captured during facet visiting and applied later. If the data is
    // just points then there won't be any ancestors.
    //Facet count of ancestors covering all of the heatmap:
    int[] allCellsAncestorCount = new int[1]; // single-element array so it can be accumulated in the inner class
    //All other ancestors:
    Map<Rectangle,Integer> ancestors = new HashMap<>();

    //Now lets count some facets!
    PrefixTreeFacetCounter.compute(strategy, context, topAcceptDocs, inputShape, facetLevel,
        new PrefixTreeFacetCounter.FacetVisitor() {
      @Override
      public void visit(Cell cell, int count) {
        final double heatMinX = heatmap.region.getMinX();
        final Rectangle rect = (Rectangle) cell.getShape();
        if (cell.getLevel() == facetLevel) {//heatmap level; count it directly
          //convert to col & row
          int column;
          if (rect.getMinX() >= heatMinX) {
            column = (int) Math.round((rect.getMinX() - heatMinX) / cellWidth);
          } else { // due to dateline wrap
            column = (int) Math.round((rect.getMinX() + 360 - heatMinX) / cellWidth);
          }
          int row = (int) Math.round((rect.getMinY() - heatMinY) / cellHeight);
          //note: unfortunately, it's possible for us to visit adjacent cells to the heatmap (if the SpatialPrefixTree
          // allows adjacent cells to overlap on the seam), so we need to skip them
          if (column < 0 || column >= heatmap.columns || row < 0 || row >= heatmap.rows) {
            return;
          }
          // increment
          heatmap.counts[column * heatmap.rows + row] += count;

        } else if (rect.relate(heatmap.region) == SpatialRelation.CONTAINS) {//containing ancestor
          allCellsAncestorCount[0] += count;

        } else { // ancestor
          // note: not particularly efficient (possible put twice, and Integer wrapper); oh well
          Integer existingCount = ancestors.put(rect, count);
          if (existingCount != null) {
            ancestors.put(rect, count + existingCount);
          }
        }
      }
    });

    //Update the heatmap counts with ancestor counts

    // Apply allCellsAncestorCount
    if (allCellsAncestorCount[0] > 0) {
      for (int i = 0; i < heatmap.counts.length; i++) {
        heatmap.counts[i] += allCellsAncestorCount[0];
      }
    }

    // Apply ancestors
    //  note: This approach isn't optimized for a ton of ancestor cells. We'll potentially increment the same cells
    //    multiple times in separate passes if any ancestors overlap. IF this poses a problem, we could optimize it
    //    with additional complication by keeping track of intervals in a sorted tree structure (possible TreeMap/Set)
    //    and iterate them cleverly such that we just make one pass at this stage.

    int[] pair = new int[2];//output of intersectInterval
    for (Map.Entry<Rectangle, Integer> entry : ancestors.entrySet()) {
      Rectangle rect = entry.getKey(); // from a cell (thus doesn't cross DL)
      final int count = entry.getValue();

      //note: we approach this in a way that eliminates int overflow/underflow (think huge cell, tiny heatmap)
      intersectInterval(heatMinY, heatMaxY, cellHeight, rows, rect.getMinY(), rect.getMaxY(), pair);
      final int startRow = pair[0];
      final int endRow = pair[1];

      if (!heatmap.region.getCrossesDateLine()) {
        intersectInterval(heatMinX, heatMaxX, cellWidth, columns, rect.getMinX(), rect.getMaxX(), pair);
        final int startCol = pair[0];
        final int endCol = pair[1];
        incrementRange(heatmap, startCol, endCol, startRow, endRow, count);

      } else {
        // note: the cell rect might intersect 2 disjoint parts of the heatmap, so we do the left & right separately
        final int leftColumns = (int) Math.round((180 - heatMinX) / cellWidth);
        final int rightColumns = heatmap.columns - leftColumns;
        //left half of dateline:
        if (rect.getMaxX() > heatMinX) {
          intersectInterval(heatMinX, 180, cellWidth, leftColumns, rect.getMinX(), rect.getMaxX(), pair);
          final int startCol = pair[0];
          final int endCol = pair[1];
          incrementRange(heatmap, startCol, endCol, startRow, endRow, count);
        }
        //right half of dateline
        if (rect.getMinX() < heatMaxX) {
          intersectInterval(-180, heatMaxX, cellWidth, rightColumns, rect.getMinX(), rect.getMaxX(), pair);
          final int startCol = pair[0] + leftColumns;
          final int endCol = pair[1] + leftColumns;
          incrementRange(heatmap, startCol, endCol, startRow, endRow, count);
        }
      }
    }

    return heatmap;
  }

  private static void intersectInterval(double heatMin, double heatMax, double heatCellLen, int numCells,
                                        double cellMin, double cellMax,
                                        int[] out) {
    assert heatMin < heatMax && cellMin < cellMax;
    //precondition: we know there's an intersection
    if (heatMin >= cellMin) {
      out[0] = 0;
    } else {
      out[0] = (int) Math.round((cellMin - heatMin) / heatCellLen);
    }
    if (heatMax <= cellMax) {
      out[1] = numCells - 1;
    } else {
      out[1] = (int) Math.round((cellMax - heatMin) / heatCellLen) - 1;
    }
  }

  private static void incrementRange(Heatmap heatmap, int startColumn, int endColumn, int startRow, int endRow,
                                     int count) {
    //startColumn & startRow are not necessarily within the heatmap range; likewise numRows/columns may overlap.
    if (startColumn < 0) {
      endColumn += startColumn;
      startColumn = 0;
    }
    endColumn = Math.min(heatmap.columns-1, endColumn);

    if (startRow < 0) {
      endRow += startRow;
      startRow = 0;
    }
    endRow = Math.min(heatmap.rows-1, endRow);

    if (startRow > endRow) {
      return;//short-circuit
    }
    for (int c = startColumn; c <= endColumn; c++) {
      int cBase = c * heatmap.rows;
      for (int r = startRow; r <= endRow; r++) {
        heatmap.counts[cBase + r] += count;
      }
    }
  }

  /** Computes the number of intervals (rows or columns) to cover a range given the sizes. */
  private static int calcRowsOrCols(double cellRange, double cellMin, double requestRange, double requestMin,
                                    double worldRange) {
    assert requestMin >= cellMin;
    //Idealistically this wouldn't be so complicated but we concern ourselves with overflow and edge cases
    double range = (requestRange + (requestMin - cellMin));
    if (range == 0) {
      return 1;
    }
    final double intervals = Math.ceil(range / cellRange);
    if (intervals > Integer.MAX_VALUE) {
      return Integer.MAX_VALUE;//should result in an error soon (exceed thresholds)
    }
    // ensures we don't have more intervals than world bounds (possibly due to rounding/edge issue)
    final long intervalsMax = Math.round(worldRange / cellRange);
    if (intervalsMax > Integer.MAX_VALUE) {
      //just return intervals
      return (int) intervals;
    }
    return Math.min((int)intervalsMax, (int)intervals);
  }

  private HeatmapFacetCounter() {
  }
}
