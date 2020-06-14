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

package org.apache.solr.client.solrj.response.json;

import java.util.List;
import java.util.Map;

import org.apache.solr.common.util.NamedList;

/**
 * Represents the result of a "heatmap" JSON facet.
 *
 * Allows access to all top-level facet properties (e.g. {@code minX}, {@code maxY}, etc.) as well as the heatmap data
 * itself in one of two forms.
 */
public class HeatmapJsonFacet {
  private int gridLevel;
  private int columns;
  private int rows;
  private double minX;
  private double maxX;
  private double minY;
  private double maxY;
  private List<List<Integer>> countGrid;
  private String countEncodedAsBase64PNG;

  @SuppressWarnings({"unchecked"})
  public HeatmapJsonFacet(NamedList<Object> heatmapNL) {
    gridLevel = (int) heatmapNL.get("gridLevel");
    columns = (int) heatmapNL.get("columns");
    rows = (int) heatmapNL.get("rows");
    minX = (double) heatmapNL.get("minX");
    maxX = (double) heatmapNL.get("maxX");
    minY = (double) heatmapNL.get("minY");
    maxY = (double) heatmapNL.get("maxY");
    System.out.println("Rows is: " + rows);
    System.out.println("Cols is " + columns);
    System.out.println("Whole deal is: " + heatmapNL);

    if (heatmapNL.get("counts_ints2D") == null) {
      countEncodedAsBase64PNG = (String) heatmapNL.get("counts_png");
    } else {
      countGrid = (List<List<Integer>>) heatmapNL.get("counts_ints2D");
    }
  }

  private int getNumCols(List<List<Integer>> grid) {
    for (List<Integer> row : grid) {
      if (row !=null ) return row.size();
    }
    throw new IllegalStateException("All rows in heatmap grid were null!");
  }

  public List<List<Integer>> getCountGrid() {
    return countGrid;
  }

  public String getCountPng() {
    return countEncodedAsBase64PNG;
  }

  public int getGridLevel() { return gridLevel; }
  public int getNumColumns() { return columns; }
  public int getNumRows() { return rows; }
  public double getMinX() { return minX; }
  public double getMaxX() { return maxX; }
  public double getMinY() { return minY; }
  public double getMaxY() { return maxY; }

  /**
   * A NamedList is a proper "heatmap" response if it contains <i>all</i> expected properties
   *
   * We try to be rather strict in determining whether {@code potentialHeatmapValues} is a "heatmap".  Users can name
   * subfacets arbitrarily, so having some names match those expected in a "heatmap" response could just be coincidence.
   * <p>
   * Heatmap facets do not support subfacets.
   */
  public static boolean isHeatmapFacet(NamedList<Object> potentialHeatmapValues) {
    boolean hasGridLevel = false;
    boolean hasColumns = false;
    boolean hasRows = false;
    boolean hasMinX = false;
    boolean hasMaxX = false;
    boolean hasMinY = false;
    boolean hasMaxY = false;
    boolean hasCountGrid = false;
    for (Map.Entry<String, Object> entry : potentialHeatmapValues) {
      String key = entry.getKey();
      if ("gridLevel".equals(key)) {
        hasGridLevel = true;
      } else if ("columns".equals(key)) {
        hasColumns = true;
      } else if ("rows".equals(key)) {
        hasRows = true;
      } else if ("minX".equals(key)) {
        hasMinX = true;
      } else if ("maxX".equals(key)) {
        hasMaxX = true;
      } else if ("minY".equals(key)) {
        hasMinY = true;
      } else if ("maxY".equals(key)){
        hasMaxY = true;
      } else if (key != null && key.startsWith("counts_")) {
        hasCountGrid = true;
      }
    }

    return potentialHeatmapValues.size() == 8 && hasGridLevel && hasColumns && hasRows && hasMinX && hasMaxX && hasMinY
        && hasMaxY && hasCountGrid;
  }
}
