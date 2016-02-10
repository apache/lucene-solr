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
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.spatial.geopoint.document.GeoPointField.TermEncoding;
import org.apache.lucene.spatial.util.GeoDistanceUtils;
import org.apache.lucene.spatial.util.GeoRect;
import org.apache.lucene.spatial.util.GeoUtils;

/** Implements a simple point distance query on a GeoPoint field. This is based on
 * {@link GeoPointInBBoxQuery} and is implemented using a two phase approach. First,
 * like {@code GeoPointInBBoxQueryImpl} candidate terms are queried using the numeric ranges based on
 * the morton codes of the min and max lat/lon pairs that intersect the boundary of the point-radius
 * circle. Terms
 * passing this initial filter are then passed to a secondary {@code postFilter} method that verifies whether the
 * decoded lat/lon point fall within the specified query distance (see {@link org.apache.lucene.util.SloppyMath#haversin}.
 * All morton value comparisons are subject to the same precision tolerance defined in
 * {@value org.apache.lucene.spatial.util.GeoEncodingUtils#TOLERANCE} and distance comparisons are subject to the accuracy of the
 * haversine formula (from R.W. Sinnott, "Virtues of the Haversine", Sky and Telescope, vol. 68, no. 2, 1984, p. 159)
 *
 * <p>Note: This query currently uses haversine which is a sloppy distance calculation (see above reference). For large
 * queries one can expect upwards of 400m error. Vincenty shrinks this to ~40m error but pays a penalty for computing
 * using the spheroid
 *
 * @lucene.experimental */
public class GeoPointDistanceQuery extends GeoPointInBBoxQuery {
  /** longitude value (in degrees) for query location */
  protected final double centerLon;
  /** latitude value (in degrees) for query location */
  protected final double centerLat;
  /** distance (in meters) from lon, lat center location */
  protected final double radiusMeters;

  /**
   * Constructs a Query for all {@link org.apache.lucene.spatial.geopoint.document.GeoPointField} types within a
   * distance (in meters) from a given point
   **/
  public GeoPointDistanceQuery(final String field, final double centerLon, final double centerLat, final double radiusMeters) {
    this(field, TermEncoding.PREFIX, centerLon, centerLat, radiusMeters);
  }

  public GeoPointDistanceQuery(final String field, final TermEncoding termEncoding, final double centerLon, final double centerLat, final double radiusMeters) {
    this(field, termEncoding, GeoUtils.circleToBBox(centerLon, centerLat, radiusMeters), centerLon, centerLat, radiusMeters);
  }

  private GeoPointDistanceQuery(final String field, final TermEncoding termEncoding, final GeoRect bbox, final double centerLon,
                                final double centerLat, final double radiusMeters) {
    super(field, termEncoding, bbox.minLon, bbox.minLat, bbox.maxLon, bbox.maxLat);
    {
      // check longitudinal overlap (restrict distance to maximum longitudinal radius)
      // todo this restriction technically shouldn't be needed,
      // its only purpose is to ensure the bounding box doesn't self overlap.
      final double maxRadius = GeoDistanceUtils.maxRadialDistanceMeters(centerLon, centerLat);
      if (radiusMeters > maxRadius) {
        throw new IllegalArgumentException("radiusMeters " + radiusMeters + " exceeds maxRadius [" + maxRadius
            + "] at location [" + centerLon + " " + centerLat + "]");
      }
    }

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
  public Query rewrite(IndexReader reader) throws IOException {
    if (getBoost() != 1f) {
      return super.rewrite(reader);
    }
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
          new GeoRect(GeoUtils.MIN_LON_INCL, maxLon, minLat, maxLat));
      bqb.add(new BooleanClause(left, BooleanClause.Occur.SHOULD));

      if (unwrappedLon < maxLon) {
        // unwrap right
        unwrappedLon += 360.0D;
      }
      GeoPointDistanceQueryImpl right = new GeoPointDistanceQueryImpl(field, termEncoding, this, unwrappedLon,
          new GeoRect(minLon, GeoUtils.MAX_LON_INCL, minLat, maxLat));
      bqb.add(new BooleanClause(right, BooleanClause.Occur.SHOULD));

      return bqb.build();
    }
    return new GeoPointDistanceQueryImpl(field, termEncoding, this, centerLon,
        new GeoRect(this.minLon, this.maxLon, this.minLat, this.maxLat));
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
