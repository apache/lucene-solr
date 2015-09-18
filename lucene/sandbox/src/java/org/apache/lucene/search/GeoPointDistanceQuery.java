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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.util.GeoRect;
import org.apache.lucene.util.GeoUtils;

// nocommit fix javadocs below: lineCrossesSphere is not used anymore?

/** Implements a simple point distance query on a GeoPoint field. This is based on
 * {@link org.apache.lucene.search.GeoPointInBBoxQuery} and is implemented using a two phase approach. First,
 * like {@code GeoPointInBBoxQueryImpl} candidate terms are queried using the numeric ranges based on
 * the morton codes of the min and max lat/lon pairs that intersect the boundary of the point-radius
 * circle (see org.apache.lucene.util.GeoUtils#lineCrossesSphere). Terms
 * passing this initial filter are then passed to a secondary {@code postFilter} method that verifies whether the
 * decoded lat/lon point fall within the specified query distance (see {@link org.apache.lucene.util.SloppyMath#haversin}.
 * All morton value comparisons are subject to the same precision tolerance defined in
 * {@value org.apache.lucene.util.GeoUtils#TOLERANCE} and distance comparisons are subject to the accuracy of the
 * haversine formula (from R.W. Sinnott, "Virtues of the Haversine", Sky and Telescope, vol. 68, no. 2, 1984, p. 159)
 *
 *
 * Note: This query currently uses haversine which is a sloppy distance calculation (see above reference). For large
 * queries one can expect upwards of 400m error. Vincenty shrinks this to ~40m error but pays a penalty for computing
 * using the spheroid
 *
 *    @lucene.experimental
 */
public final class GeoPointDistanceQuery extends GeoPointInBBoxQuery {
  protected final double centerLon;
  protected final double centerLat;
  protected final double radiusMeters;

  /** NOTE: radius is in meters. */
  public GeoPointDistanceQuery(final String field, final double centerLon, final double centerLat, final double radiusMeters) {
    this(field, GeoUtils.circleToBBox(centerLon, centerLat, radiusMeters), centerLon, centerLat, radiusMeters);
  }

  private GeoPointDistanceQuery(final String field, GeoRect bbox, final double centerLon,
                                final double centerLat, final double radiusMeters) {
    super(field, bbox.minLon, bbox.minLat, bbox.maxLon, bbox.maxLat);

    if (GeoUtils.isValidLon(centerLon) == false) {
      throw new IllegalArgumentException("invalid centerLon " + centerLon);
    }

    if (GeoUtils.isValidLat(centerLat) == false) {
      throw new IllegalArgumentException("invalid centerLat " + centerLat);
    }

    if (radiusMeters <= 0.0) {
      throw new IllegalArgumentException("invalid radiusMeters " + radiusMeters);
    }

    this.centerLon = centerLon;
    this.centerLat = centerLat;
    this.radiusMeters = radiusMeters;
  }

  @Override
  public Query rewrite(IndexReader reader) {
    if (maxLon < minLon) {
      BooleanQuery.Builder bqb = new BooleanQuery.Builder();

      GeoPointDistanceQueryImpl left = new GeoPointDistanceQueryImpl(field, this, new GeoRect(GeoUtils.MIN_LON_INCL, maxLon,
          minLat, maxLat));
      bqb.add(new BooleanClause(left, BooleanClause.Occur.SHOULD));
      GeoPointDistanceQueryImpl right = new GeoPointDistanceQueryImpl(field, this, new GeoRect(minLon, GeoUtils.MAX_LON_INCL,
          minLat, maxLat));
      bqb.add(new BooleanClause(right, BooleanClause.Occur.SHOULD));
      return bqb.build();
    }
    return new GeoPointDistanceQueryImpl(field, this, new GeoRect(this.minLon, this.maxLon, this.minLat, this.maxLat));
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
        .append(centerLon)
        .append(',')
        .append(centerLat)
        .append(']')
        .append(" Distance: ")
        .append(radiusMeters)
        .append(" meters")
        .append("]")
        .toString();
  }

  public double getCenterLon() {
    return this.centerLon;
  }

  public double getCenterLat() {
    return this.centerLat;
  }

  public double getRadiusMeters() {
    return this.radiusMeters;
  }
}
