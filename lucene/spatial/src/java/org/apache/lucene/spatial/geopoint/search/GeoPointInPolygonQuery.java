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

import java.util.Arrays;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Query;
import org.apache.lucene.spatial.geopoint.document.GeoPointField.TermEncoding;
import org.apache.lucene.spatial.util.GeoEncodingUtils;
import org.apache.lucene.spatial.util.GeoRect;
import org.apache.lucene.spatial.util.GeoUtils;

/** Implements a simple point in polygon query on a GeoPoint field. This is based on
 * {@code GeoPointInBBoxQueryImpl} and is implemented using a
 * three phase approach. First, like {@code GeoPointInBBoxQueryImpl}
 * candidate terms are queried using a numeric range based on the morton codes
 * of the min and max lat/lon pairs. Terms passing this initial filter are passed
 * to a secondary filter that verifies whether the decoded lat/lon point falls within
 * (or on the boundary) of the bounding box query. Finally, the remaining candidate
 * term is passed to the final point in polygon check. All value comparisons are subject
 * to the same precision tolerance defined in {@value GeoEncodingUtils#TOLERANCE}
 *
 * <p>NOTES:
 *    1.  The polygon coordinates need to be in either clockwise or counter-clockwise order.
 *    2.  The polygon must not be self-crossing, otherwise the query may result in unexpected behavior
 *    3.  All latitude/longitude values must be in decimal degrees.
 *    4.  Complex computational geometry (e.g., dateline wrapping, polygon with holes) is not supported
 *    5.  For more advanced GeoSpatial indexing and query operations see spatial module
 *
 * @lucene.experimental
 */
public final class GeoPointInPolygonQuery extends GeoPointInBBoxQuery {
  // polygon position arrays - this avoids the use of any objects or
  // or geo library dependencies
  /** array of y (latitude) values (in degrees) */
  protected final double[] polyLats;
  /** array of x (longitude) values (in degrees) */
  protected final double[] polyLons;

  /**
   * Constructs a new GeoPolygonQuery that will match encoded {@link org.apache.lucene.spatial.geopoint.document.GeoPointField} terms
   * that fall within or on the boundary of the polygon defined by the input parameters.
   */
  public GeoPointInPolygonQuery(final String field, final double[] polyLats, final double[] polyLons) {
    this(field, TermEncoding.PREFIX, GeoUtils.polyToBBox(polyLats, polyLons), polyLats, polyLons);
  }

  /**
   * Constructs a new GeoPolygonQuery that will match encoded {@link org.apache.lucene.spatial.geopoint.document.GeoPointField} terms
   * that fall within or on the boundary of the polygon defined by the input parameters.
   */
  public GeoPointInPolygonQuery(final String field, final TermEncoding termEncoding, final double[] polyLats, final double[] polyLons) {
    this(field, termEncoding, GeoUtils.polyToBBox(polyLats, polyLons), polyLats, polyLons);
  }

  /** Common constructor, used only internally. */
  private GeoPointInPolygonQuery(final String field, TermEncoding termEncoding, GeoRect bbox, final double[] polyLats, final double[] polyLons) {
    super(field, termEncoding, bbox.minLat, bbox.maxLat, bbox.minLon, bbox.maxLon);
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

    this.polyLons = polyLons;
    this.polyLats = polyLats;
  }

  /** throw exception if trying to change rewrite method */
  @Override
  public Query rewrite(IndexReader reader) {
    return new GeoPointInPolygonQueryImpl(field, termEncoding, this, this.minLat, this.maxLat, this.minLon, this.maxLon);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;

    GeoPointInPolygonQuery that = (GeoPointInPolygonQuery) o;

    if (!Arrays.equals(polyLats, that.polyLats)) return false;
    if (!Arrays.equals(polyLons, that.polyLons)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (polyLats != null ? Arrays.hashCode(polyLats) : 0);
    result = 31 * result + (polyLons != null ? Arrays.hashCode(polyLons) : 0);
    return result;
  }

  /** print out this polygon query */
  @Override
  public String toString(String field) {
    assert polyLats.length == polyLons.length;

    final StringBuilder sb = new StringBuilder();
    sb.append(getClass().getSimpleName());
    sb.append(':');
    if (!getField().equals(field)) {
      sb.append(" field=");
      sb.append(getField());
      sb.append(':');
    }
    sb.append(" Points: ");
    for (int i=0; i<polyLats.length; ++i) {
      sb.append("[")
          .append(polyLats[i])
          .append(", ")
          .append(polyLons[i])
          .append("] ");
    }

    return sb.toString();
  }

  /**
   * API utility method for returning the array of longitudinal values for this GeoPolygon
   * The returned array is not a copy so do not change it!
   */
  public double[] getLons() {
    return this.polyLons;
  }

  /**
   * API utility method for returning the array of latitudinal values for this GeoPolygon
   * The returned array is not a copy so do not change it!
   */
  public double[] getLats() {
    return this.polyLats;
  }
}
