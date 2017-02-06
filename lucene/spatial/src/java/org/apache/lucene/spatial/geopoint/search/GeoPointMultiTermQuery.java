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

package org.apache.lucene.spatial.geopoint.search;

import java.io.IOException;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.spatial.geopoint.document.GeoPointField;
import org.apache.lucene.spatial.geopoint.document.GeoPointField.TermEncoding;
import org.apache.lucene.spatial.util.GeoRelationUtils;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.SloppyMath;

/**
 * TermQuery for GeoPointField for overriding {@link org.apache.lucene.search.MultiTermQuery} methods specific to
 * Geospatial operations
 *
 * @lucene.experimental
 * @deprecated Use the higher performance {@code LatLonPoint} queries instead.
 */
@Deprecated
abstract class GeoPointMultiTermQuery extends MultiTermQuery {
  // simple bounding box optimization - no objects used to avoid dependencies
  protected final double minLon;
  protected final long minEncoded;
  protected final int minX;
  protected final double minLat;
  protected final int minY;
  protected final double maxLon;
  protected final int maxX;
  protected final double maxLat;
  protected final int maxY;

  protected final short maxShift;
  protected final TermEncoding termEncoding;
  protected final CellComparator cellComparator;

  /**
   * Constructs a query matching terms that cannot be represented with a single
   * Term.
   */
  public GeoPointMultiTermQuery(String field, final TermEncoding termEncoding, final double minLat, final double maxLat, final double minLon, final double maxLon) {
    super(field);

    this.minEncoded = GeoPointField.encodeLatLon(minLat, minLon);
    final long maxEncoded = GeoPointField.encodeLatLon(maxLat, maxLon);

    this.minX = (int)BitUtil.deinterleave(minEncoded);
    this.maxX = (int)BitUtil.deinterleave(maxEncoded);
    this.minY = (int)BitUtil.deinterleave(minEncoded >>> 1);
    this.maxY = (int)BitUtil.deinterleave(maxEncoded >>> 1);

    this.minLat = minLat;
    this.maxLat = maxLat;
    this.minLon = minLon;
    this.maxLon = maxLon;

    this.maxShift = computeMaxShift();
    this.termEncoding = termEncoding;
    this.cellComparator = newCellComparator();

    this.rewriteMethod = GEO_CONSTANT_SCORE_REWRITE;
  }

  public static final RewriteMethod GEO_CONSTANT_SCORE_REWRITE = new RewriteMethod() {
    @Override
    public Query rewrite(IndexReader reader, MultiTermQuery query) {
      return new GeoPointTermQueryConstantScoreWrapper<>((GeoPointMultiTermQuery)query);
    }
  };

  @Override @SuppressWarnings("unchecked")
  protected TermsEnum getTermsEnum(final Terms terms, AttributeSource atts) throws IOException {
    return GeoPointTermsEnum.newInstance(terms.iterator(), this);
  }

  /**
   * Computes the maximum shift based on the diagonal distance of the bounding box
   */
  protected short computeMaxShift() {
    // in this case a factor of 4 brings the detail level to ~0.001/0.002 degrees lat/lon respectively (or ~111m/222m)
    final short shiftFactor;

    // compute diagonal distance
    double midLon = (minLon + maxLon) * 0.5;
    double midLat = (minLat + maxLat) * 0.5;

    if (SloppyMath.haversinMeters(minLat, minLon, midLat, midLon) > 1000000) {
      shiftFactor = 5;
    } else {
      shiftFactor = 4;
    }

    return (short)(GeoPointField.PRECISION_STEP * shiftFactor);
  }

  /**
   * Abstract method to construct the class that handles all geo point relations
   * (e.g., GeoPointInPolygon)
   */
  abstract protected CellComparator newCellComparator();

  /**
   * Base class for all geo point relation comparators
   */
  static abstract class CellComparator {
    protected final GeoPointMultiTermQuery geoPointQuery;

    CellComparator(GeoPointMultiTermQuery query) {
      this.geoPointQuery = query;
    }

    /**
     * Primary driver for cells intersecting shape boundaries
     */
    protected boolean cellIntersectsMBR(final double minLat, final double maxLat, final double minLon, final double maxLon) {
      return GeoRelationUtils.rectIntersects(minLat, maxLat, minLon, maxLon, geoPointQuery.minLat, geoPointQuery.maxLat,
                                             geoPointQuery.minLon, geoPointQuery.maxLon);
    }

    /** uses encoded values to check whether quad cell intersects the shape bounding box */
    protected boolean cellIntersectsMBR(final long min, final long max) {
      return !(Integer.compareUnsigned((int)BitUtil.deinterleave(max), geoPointQuery.minX) < 0
          || Integer.compareUnsigned((int)BitUtil.deinterleave(min), geoPointQuery.maxX) > 0
          || Integer.compareUnsigned((int)BitUtil.deinterleave(max >>> 1), geoPointQuery.minY) < 0
          || Integer.compareUnsigned((int)BitUtil.deinterleave(min >>> 1), geoPointQuery.maxY) > 0);
    }

    /**
     * Return whether quad-cell contains the bounding box of this shape
     */
    protected boolean cellContains(final double minLat, final double maxLat, final double minLon, final double maxLon) {
      return GeoRelationUtils.rectWithin(geoPointQuery.minLat, geoPointQuery.maxLat, geoPointQuery.minLon,
                                         geoPointQuery.maxLon, minLat, maxLat, minLon, maxLon);
    }

    /**
     * Determine whether the quad-cell crosses the shape
     */
    abstract protected boolean cellCrosses(final double minLat, final double maxLat, final double minLon, final double maxLon);

    /**
     * Determine whether quad-cell is within the shape
     */
    abstract protected boolean cellWithin(final double minLat, final double maxLat, final double minLon, final double maxLon);

    /**
     * Default shape is a rectangle, so this returns the same as {@code cellIntersectsMBR}
     */
    abstract protected boolean cellIntersectsShape(final double minLat, final double maxLat, final double minLon, final double maxLon);

    abstract protected Relation relate(final double minLat, final double maxLat, final double minLon, final double maxLon);

    abstract protected boolean postFilter(final double lat, final double lon);
  }
}
