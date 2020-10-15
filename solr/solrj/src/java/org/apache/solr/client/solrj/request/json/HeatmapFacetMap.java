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

package org.apache.solr.client.solrj.request.json;

import java.util.Map;

/**
 * Represents a "heatmap" facet in a JSON request query.
 *
 * Ready for use with {@link JsonQueryRequest#withFacet(String, Map)}
 */
public class HeatmapFacetMap extends JsonFacetMap<HeatmapFacetMap> {
  public HeatmapFacetMap(String fieldName) {
    super("heatmap");

    if (fieldName == null) {
      throw new IllegalArgumentException("Parameter 'fieldName' must be non-null");
    }

    put("field", fieldName);
  }

  @Override
  public HeatmapFacetMap getThis() { return this; }

  @Override
  public HeatmapFacetMap withSubFacet(String facetName,
                                      @SuppressWarnings({"rawtypes"})JsonFacetMap map) {
    throw new UnsupportedOperationException(getClass().getName() + " doesn't currently support subfacets");
  }

  /**
   * Indicate the region to compute the heatmap facet on.
   *
   * Defaults to the "world" ("[-180,-90 TO 180,90]")
   */
  public HeatmapFacetMap setRegionQuery(String queryString) {
    if (queryString == null) {
      throw new IllegalArgumentException("Parameter 'queryString' must be non-null");
    }

    put("geom", queryString);
    return this;
  }

  /**
   * Indicates the size of each cell in the computed heatmap grid
   *
   * If not set, defaults to being computed by {@code distErrPct} or {@code distErr}
   *
   * @param individualCellSize the forced size of each cell in the heatmap grid
   *
   * @see #setDistErr(double)
   * @see #setDistErrPct(double)
   */
  public HeatmapFacetMap setGridLevel(int individualCellSize) {
    if (individualCellSize <= 0) {
      throw new IllegalArgumentException("Parameter 'individualCellSize' must be a positive integer");
    }
    put("gridLevel", individualCellSize);
    return this;
  }

  /**
   * A fraction of the heatmap region that is used to compute the cell size.
   *
   * Defaults to 0.15 if not specified.
   *
   * @see #setGridLevel(int)
   * @see #setDistErr(double)
   */
  public HeatmapFacetMap setDistErrPct(double distErrPct) {
    if (distErrPct < 0 || distErrPct > 1) {
      throw new IllegalArgumentException("Parameter 'distErrPct' must be between 0.0 and 1.0");
    }
    put("distErrPct", distErrPct);
    return this;
  }

  /**
   * Indicates the maximum acceptable cell error distance.
   *
   * Used to compute the size of each cell in the heatmap grid rather than specifying {@link #setGridLevel(int)}
   *
   * @param distErr a positive value representing the maximum acceptable cell error.
   *
   * @see #setGridLevel(int)
   * @see #setDistErrPct(double)
   */
  public HeatmapFacetMap setDistErr(double distErr) {
    if (distErr < 0) {
      throw new IllegalArgumentException("Parameter 'distErr' must be non-negative");
    }
    put("distErr", distErr);
    return this;
  }

  public enum HeatmapFormat {
    INTS2D("ints2D"), PNG("png");

    private final String value;

    HeatmapFormat(String value) {
      this.value = value;
    }

    @Override
    public String toString() { return value; }
  }

  /**
   * Sets the format that the computed heatmap should be returned in.
   *
   * Defaults to 'ints2D' if not specified.
   */
  public HeatmapFacetMap setHeatmapFormat(HeatmapFormat format) {
    if (format == null) {
      throw new IllegalArgumentException("Parameter 'format' must be non-null");
    }
    put("format", format.toString());
    return this;
  }
}
