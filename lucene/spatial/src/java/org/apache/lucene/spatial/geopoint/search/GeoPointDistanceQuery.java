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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.spatial.geopoint.document.GeoPointField.TermEncoding;
import org.apache.lucene.geo.Rectangle;
import org.apache.lucene.geo.GeoUtils;

/** Implements a simple point distance query on a GeoPoint field. This is based on
 * {@link GeoPointInBBoxQuery} and is implemented using a two phase approach. First,
 * like {@code GeoPointInBBoxQueryImpl} candidate terms are queried using the numeric ranges based on
 * the morton codes of the min and max lat/lon pairs that intersect the boundary of the point-radius
 * circle. Terms
 * passing this initial filter are then passed to a secondary {@code postFilter} method that verifies whether the
 * decoded lat/lon point fall within the specified query distance (see {@link org.apache.lucene.util.SloppyMath#haversinMeters(double, double, double, double)}.
 * Distance comparisons are subject to the accuracy of the haversine formula
 * (from R.W. Sinnott, "Virtues of the Haversine", Sky and Telescope, vol. 68, no. 2, 1984, p. 159)
 *
 * <p>Note: This query currently uses haversine which is a sloppy distance calculation (see above reference). For large
 * queries one can expect upwards of 400m error. Vincenty shrinks this to ~40m error but pays a penalty for computing
 * using the spheroid
 *
 * @lucene.experimental
 * @deprecated Use the higher performance {@code LatLonPoint.newDistanceQuery} instead.
 */
@Deprecated
public class GeoPointDistanceQuery extends GeoPointInBBoxQuery {
  /** latitude value (in degrees) for query location */
  protected final double centerLat;
  /** longitude value (in degrees) for query location */
  protected final double centerLon;
  /** distance (in meters) from lat, lon center location */
  protected final double radiusMeters;
  /** partial haversin computation */
  protected final double sortKey;

  // we must check these before passing to superclass or circleToBBox, or users can get a strange exception!
  private static double checkRadius(double radiusMeters) {
    if (Double.isFinite(radiusMeters) == false || radiusMeters < 0) {
      throw new IllegalArgumentException("invalid radiusMeters " + radiusMeters);
    }
    return radiusMeters;
  }
  
  private static double checkLatitude(double centerLat) {
    GeoUtils.checkLatitude(centerLat);
    return centerLat;
  }
  
  private static double checkLongitude(double centerLon) {
    GeoUtils.checkLongitude(centerLon);
    return centerLon;
  }
  
  /**
   * Constructs a Query for all {@link org.apache.lucene.spatial.geopoint.document.GeoPointField} types within a
   * distance (in meters) from a given point
   **/
  public GeoPointDistanceQuery(final String field, final double centerLat, final double centerLon, final double radiusMeters) {
    this(field, TermEncoding.PREFIX, checkLatitude(centerLat), checkLongitude(centerLon), checkRadius(radiusMeters));
  }

  /**
   * Constructs a Query for all {@link org.apache.lucene.spatial.geopoint.document.GeoPointField} types within a
   * distance (in meters) from a given point. Accepts optional
   * {@link org.apache.lucene.spatial.geopoint.document.GeoPointField.TermEncoding} parameter
   **/
  public GeoPointDistanceQuery(final String field, final TermEncoding termEncoding, final double centerLat, final double centerLon, final double radiusMeters) {
    this(field, termEncoding, Rectangle.fromPointDistance(centerLat, centerLon, checkRadius(radiusMeters)), centerLat, centerLon, radiusMeters);
  }

  private GeoPointDistanceQuery(final String field, final TermEncoding termEncoding, final Rectangle bbox,
                                 final double centerLat, final double centerLon, final double radiusMeters) {
    super(field, termEncoding, bbox.minLat, bbox.maxLat, bbox.minLon, bbox.maxLon);

    this.centerLat = centerLat;
    this.centerLon = centerLon;
    this.radiusMeters = radiusMeters;
    this.sortKey = GeoUtils.distanceQuerySortKey(radiusMeters);
  }

  @Override
  public Query rewrite(IndexReader reader) {
    // query crosses dateline; split into left and right queries
    if (maxLon < minLon) {
      BooleanQuery.Builder bqb = new BooleanQuery.Builder();

      // unwrap the longitude iff outside the specified min/max lon range
      double unwrappedLon = centerLon;
      if (unwrappedLon > maxLon) {
        // unwrap left
        unwrappedLon += -360.0D;
      }
      GeoPointDistanceQueryImpl left = new GeoPointDistanceQueryImpl(field, termEncoding, this, unwrappedLon,
                                                                     new Rectangle(minLat, maxLat, GeoUtils.MIN_LON_INCL, maxLon));
      bqb.add(new BooleanClause(left, BooleanClause.Occur.SHOULD));

      if (unwrappedLon < maxLon) {
        // unwrap right
        unwrappedLon += 360.0D;
      }
      GeoPointDistanceQueryImpl right = new GeoPointDistanceQueryImpl(field, termEncoding, this, unwrappedLon,
                                                                      new Rectangle(minLat, maxLat, minLon, GeoUtils.MAX_LON_INCL));
      bqb.add(new BooleanClause(right, BooleanClause.Occur.SHOULD));

      return bqb.build();
    }
    return new GeoPointDistanceQueryImpl(field, termEncoding, this, centerLon,
                                         new Rectangle(this.minLat, this.maxLat, this.minLon, this.maxLon));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof GeoPointDistanceQuery)) return false;
    if (!super.equals(o)) return false;

    GeoPointDistanceQuery that = (GeoPointDistanceQuery) o;

    if (Double.compare(that.centerLat, centerLat) != 0) return false;
    if (Double.compare(that.centerLon, centerLon) != 0) return false;
    if (Double.compare(that.radiusMeters, radiusMeters) != 0) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    long temp;
    temp = Double.doubleToLongBits(centerLon);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    temp = Double.doubleToLongBits(centerLat);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    temp = Double.doubleToLongBits(radiusMeters);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    return result;
  }

  @Override
  public String toString(String field) {
    final StringBuilder sb = new StringBuilder();
    sb.append(getClass().getSimpleName());
    sb.append(':');
    if (!this.field.equals(field)) {
      sb.append(" field=");
      sb.append(this.field);
      sb.append(':');
    }
    return sb.append( " Center: [")
        .append(centerLat)
        .append(',')
        .append(centerLon)
        .append(']')
        .append(" Distance: ")
        .append(radiusMeters)
        .append(" meters")
        .append("]")
        .toString();
  }

  /** getter method for center longitude value */
  public double getCenterLon() {
    return this.centerLon;
  }

  /** getter method for center latitude value */
  public double getCenterLat() {
    return this.centerLat;
  }

  /** getter method for distance value (in meters) */
  public double getRadiusMeters() {
    return this.radiusMeters;
  }
}
