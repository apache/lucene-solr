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

import java.io.IOException;

import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.GeoUtils;
import org.apache.lucene.util.ToStringUtils;

/** Implements a simple bounding box query on a GeoPoint field. This is inspired by
 * {@link org.apache.lucene.search.NumericRangeQuery} and is implemented using a
 * two phase approach. First, candidate terms are queried using a numeric
 * range based on the morton codes of the min and max lat/lon pairs. Terms
 * passing this initial filter are passed to a final check that verifies whether
 * the decoded lat/lon falls within (or on the boundary) of the query bounding box.
 * The value comparisons are subject to a precision tolerance defined in
 * {@value org.apache.lucene.util.GeoUtils#TOLERANCE}
 *
 * NOTES:
 *    1.  All latitude/longitude values must be in decimal degrees.
 *    2.  Complex computational geometry (e.g., dateline wrapping) is not supported
 *    3.  For more advanced GeoSpatial indexing and query operations see spatial module
 *    4.  This is well suited for small rectangles, large bounding boxes result
 *        in too many terms
 *
 * @lucene.experimental
 */
public class GeoPointInBBoxQuery extends MultiTermQuery {
  // simple bounding box optimization - no objects used to avoid dependencies
  protected final double minLon;
  protected final double minLat;
  protected final double maxLon;
  protected final double maxLat;

  /**
   * Constructs a new GeoBBoxQuery that will match encoded GeoPoint terms that fall within or on the boundary
   * of the bounding box defined by the input parameters
   * @param field the field name
   * @param minLon lower longitude (x) value of the bounding box
   * @param minLat lower latitude (y) value of the bounding box
   * @param maxLon upper longitude (x) value of the bounding box
   * @param maxLat upper latitude (y) value of the bounding box
   */
  public GeoPointInBBoxQuery(final String field, final double minLon, final double minLat, final double maxLon, final double maxLat) {
    super(field);
    if (GeoUtils.isValidLon(minLon) == false) {
      throw new IllegalArgumentException("invalid minLon " + minLon);
    }
    if (GeoUtils.isValidLon(maxLon) == false) {
      throw new IllegalArgumentException("invalid maxLon " + maxLon);
    }
    if (GeoUtils.isValidLat(minLat) == false) {
      throw new IllegalArgumentException("invalid minLat " + minLat);
    }
    if (GeoUtils.isValidLat(maxLat) == false) {
      throw new IllegalArgumentException("invalid maxLat " + maxLat);
    }
    this.minLon = minLon;
    this.minLat = minLat;
    this.maxLon = maxLon;
    this.maxLat = maxLat;
  }

  @Override @SuppressWarnings("unchecked")
  protected TermsEnum getTermsEnum(final Terms terms, AttributeSource atts) throws IOException {
    final Long min = GeoUtils.mortonHash(minLon, minLat);
    final Long max = Math.abs(GeoUtils.mortonHash(maxLon, maxLat));
    if (min != null && max != null &&  min.compareTo(max) > 0) {
      return TermsEnum.EMPTY;
    }
    return new GeoPointTermsEnum(terms.iterator(), atts, minLon, minLat, maxLon, maxLat);
  }

  @Override
  @SuppressWarnings({"unchecked","rawtypes"})
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;

    GeoPointInBBoxQuery that = (GeoPointInBBoxQuery) o;

    if (Double.compare(that.maxLat, maxLat) != 0) return false;
    if (Double.compare(that.maxLon, maxLon) != 0) return false;
    if (Double.compare(that.minLat, minLat) != 0) return false;
    if (Double.compare(that.minLon, minLon) != 0) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    long temp;
    temp = Double.doubleToLongBits(minLon);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    temp = Double.doubleToLongBits(minLat);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    temp = Double.doubleToLongBits(maxLon);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    temp = Double.doubleToLongBits(maxLat);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    return result;
  }

  @Override
  public String toString(String field) {
    final StringBuilder sb = new StringBuilder();
    sb.append(getClass().getSimpleName());
    sb.append(':');
    if (!getField().equals(field)) {
      sb.append(" field=");
      sb.append(getField());
      sb.append(':');
    }
    return sb.append(" Lower Left: [")
        .append(minLon)
        .append(',')
        .append(minLat)
        .append(']')
        .append(" Upper Right: [")
        .append(maxLon)
        .append(',')
        .append(maxLat)
        .append("]")
        .append(ToStringUtils.boost(getBoost()))
        .toString();
  }
}
