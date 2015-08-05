package org.apache.lucene.search;

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

import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.GeoUtils;
import org.apache.lucene.util.ToStringUtils;

import java.io.IOException;
import java.util.Arrays;

/** Implements a simple point in polygon query on a GeoPoint field. This is based on
 * {@code GeoPointInBBoxQueryImpl} and is implemented using a
 * three phase approach. First, like {@code GeoPointInBBoxQueryImpl}
 * candidate terms are queried using a numeric range based on the morton codes
 * of the min and max lat/lon pairs. Terms passing this initial filter are passed
 * to a secondary filter that verifies whether the decoded lat/lon point falls within
 * (or on the boundary) of the bounding box query. Finally, the remaining candidate
 * term is passed to the final point in polygon check. All value comparisons are subject
 * to the same precision tolerance defined in {@value org.apache.lucene.util.GeoUtils#TOLERANCE}
 *
 * NOTES:
 *    1.  The polygon coordinates need to be in either clockwise or counter-clockwise order.
 *    2.  The polygon must not be self-crossing, otherwise the query may result in unexpected behavior
 *    3.  All latitude/longitude values must be in decimal degrees.
 *    4.  Complex computational geometry (e.g., dateline wrapping, polygon with holes) is not supported
 *    5.  For more advanced GeoSpatial indexing and query operations see spatial module
 *
 *    @lucene.experimental
 */
public final class GeoPointInPolygonQuery extends GeoPointInBBoxQueryImpl {
  // polygon position arrays - this avoids the use of any objects or
  // or geo library dependencies
  private final double[] x;
  private final double[] y;

  /**
   * Constructs a new GeoPolygonQuery that will match encoded {@link org.apache.lucene.document.GeoPointField} terms
   * that fall within or on the boundary of the polygon defined by the input parameters.
   */
  public GeoPointInPolygonQuery(final String field, final double[] polyLons, final double[] polyLats) {
    this(field, computeBBox(polyLons, polyLats), polyLons, polyLats);
  }

  /** Common constructor, used only internally. */
  private GeoPointInPolygonQuery(final String field, GeoBoundingBox bbox, final double[] polyLons, final double[] polyLats) {
    super(field, bbox.minLon, bbox.minLat, bbox.maxLon, bbox.maxLat);
    if (polyLats.length != polyLons.length) {
      throw new IllegalArgumentException("polyLats and polyLons must be equal length");
    }
    if (polyLats.length < 4) {
      throw new IllegalArgumentException("at least 4 polygon points required");
    }
    if (polyLats[0] != polyLats[polyLats.length-1]) {
      throw new IllegalArgumentException("first and last points of the polygon must be the same (it must close itself): polyLats[0]=" + polyLats[0] + " polyLats[" + (polyLats.length-1) + "]=" + polyLats[polyLats.length-1]);
    }
    if (polyLons[0] != polyLons[polyLons.length-1]) {
      throw new IllegalArgumentException("first and last points of the polygon must be the same (it must close itself): polyLons[0]=" + polyLons[0] + " polyLons[" + (polyLons.length-1) + "]=" + polyLons[polyLons.length-1]);
    }

    // convert polygon vertices to coordinates within tolerance
    this.x = toleranceConversion(polyLons);
    this.y = toleranceConversion(polyLats);
  }

  private double[] toleranceConversion(double[] vals) {
    for (int i=0; i<vals.length; ++i) {
      vals[i] = ((int)(vals[i]/GeoUtils.TOLERANCE))*GeoUtils.TOLERANCE;
    }
    return vals;
  }

  @Override @SuppressWarnings("unchecked")
  protected TermsEnum getTermsEnum(final Terms terms, AttributeSource atts) throws IOException {
    return new GeoPolygonTermsEnum(terms.iterator(), this.minLon, this.minLat, this.maxLon, this.maxLat);
  }

  @Override
  public void setRewriteMethod(RewriteMethod method) {
    throw new UnsupportedOperationException("cannot change rewrite method");
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;

    GeoPointInPolygonQuery that = (GeoPointInPolygonQuery) o;

    if (!Arrays.equals(x, that.x)) return false;
    if (!Arrays.equals(y, that.y)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (x != null ? Arrays.hashCode(x) : 0);
    result = 31 * result + (y != null ? Arrays.hashCode(y) : 0);
    return result;
  }

  @Override
  public String toString(String field) {
    assert x.length == y.length;

    final StringBuilder sb = new StringBuilder();
    sb.append(getClass().getSimpleName());
    sb.append(':');
    if (!getField().equals(field)) {
      sb.append(" field=");
      sb.append(getField());
      sb.append(':');
    }
    sb.append(" Points: ");
    for (int i=0; i<x.length; ++i) {
      sb.append("[")
        .append(x[i])
        .append(", ")
        .append(y[i])
        .append("] ");
    }
    sb.append(ToStringUtils.boost(getBoost()));

    return sb.toString();
  }

  /**
   * Custom {@link org.apache.lucene.index.TermsEnum} that computes morton hash ranges based on the defined edges of
   * the provided polygon.
   */
  private final class GeoPolygonTermsEnum extends GeoPointTermsEnum {
    GeoPolygonTermsEnum(final TermsEnum tenum, final double minLon, final double minLat,
                        final double maxLon, final double maxLat) {
      super(tenum, minLon, minLat, maxLon, maxLat);
    }

    @Override
    protected boolean cellCrosses(final double minLon, final double minLat, final double maxLon, final double maxLat) {
      return GeoUtils.rectCrossesPoly(minLon, minLat, maxLon, maxLat, x, y, GeoPointInPolygonQuery.this.minLon,
          GeoPointInPolygonQuery.this.minLat, GeoPointInPolygonQuery.this.maxLon, GeoPointInPolygonQuery.this.maxLat);
    }

    @Override
    protected boolean cellWithin(final double minLon, final double minLat, final double maxLon, final double maxLat) {
      return GeoUtils.rectWithinPoly(minLon, minLat, maxLon, maxLat, x, y, GeoPointInPolygonQuery.this.minLon,
          GeoPointInPolygonQuery.this.minLat, GeoPointInPolygonQuery.this.maxLon, GeoPointInPolygonQuery.this.maxLat);
    }

    @Override
    protected boolean cellIntersectsShape(final double minLon, final double minLat, final double maxLon, final double maxLat) {
      return cellWithin(minLon, minLat, maxLon, maxLat) || cellCrosses(minLon, minLat, maxLon, maxLat);
    }

    /**
     * The two-phase query approach. The parent
     * {@link org.apache.lucene.search.GeoPointTermsEnum#accept} method is called to match
     * encoded terms that fall within the bounding box of the polygon. Those documents that pass the initial
     * bounding box filter are then compared to the provided polygon using the
     * {@link org.apache.lucene.util.GeoUtils#pointInPolygon} method.
     */
    @Override
    protected boolean postFilter(final double lon, final double lat) {
      return GeoUtils.pointInPolygon(x, y, lat, lon);
    }
  }

  private static GeoBoundingBox computeBBox(double[] polyLons, double[] polyLats) {
    if (polyLons.length != polyLats.length) {
      throw new IllegalArgumentException("polyLons and polyLats must be equal length");
    }

    double minLon = Double.POSITIVE_INFINITY;
    double maxLon = Double.NEGATIVE_INFINITY;
    double minLat = Double.POSITIVE_INFINITY;
    double maxLat = Double.NEGATIVE_INFINITY;

    for (int i=0;i<polyLats.length;i++) {
      if (GeoUtils.isValidLon(polyLons[i]) == false) {
        throw new IllegalArgumentException("invalid polyLons[" + i + "]=" + polyLons[i]);
      }
      if (GeoUtils.isValidLat(polyLats[i]) == false) {
        throw new IllegalArgumentException("invalid polyLats[" + i + "]=" + polyLats[i]);
      }
      minLon = Math.min(polyLons[i], minLon);
      maxLon = Math.max(polyLons[i], maxLon);
      minLat = Math.min(polyLats[i], minLat);
      maxLat = Math.max(polyLats[i], maxLat);
    }

    return new GeoBoundingBox(minLon, maxLon, minLat, maxLat);
  }

  /**
   * API utility method for returning the array of longitudinal values for this GeoPolygon
   * The returned array is not a copy so do not change it!
   */
  public double[] getLons() {
    return this.x;
  }

  /**
   * API utility method for returning the array of latitudinal values for this GeoPolygon
   * The returned array is not a copy so do not change it!
   */
  public double[] getLats() {
    return this.y;
  }
}
