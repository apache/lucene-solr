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
package org.apache.lucene.search;

import java.io.IOException;

import org.apache.lucene.index.IndexReader;

/** Implements a point distance range query on a GeoPoint field. This is based on
 * {@code org.apache.lucene.search.GeoPointDistanceQuery} and is implemented using a
 * {@code org.apache.lucene.search.BooleanClause.MUST_NOT} clause to exclude any points that fall within
 * minRadiusMeters from the provided point.
 *
 *    @lucene.experimental
 */
public final class GeoPointDistanceRangeQuery extends GeoPointDistanceQuery {
  protected final double minRadiusMeters;

  public GeoPointDistanceRangeQuery(final String field, final double centerLon, final double centerLat,
                                    final double minRadiusMeters, final double maxRadius) {
    super(field, centerLon, centerLat, maxRadius);
    this.minRadiusMeters = minRadiusMeters;
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    if (getBoost() != 1f) {
      return super.rewrite(reader);
    }
    Query q = super.rewrite(reader);
    if (minRadiusMeters == 0.0) {
      return q;
    }

    // add an exclusion query
    BooleanQuery.Builder bqb = new BooleanQuery.Builder();

    // create a new exclusion query
    GeoPointDistanceQuery exclude = new GeoPointDistanceQuery(field, centerLon, centerLat, minRadiusMeters);
    bqb.add(new BooleanClause(q, BooleanClause.Occur.MUST));
    bqb.add(new BooleanClause(exclude, BooleanClause.Occur.MUST_NOT));

    return bqb.build();
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
        .append(" From Distance: ")
        .append(minRadiusMeters)
        .append(" m")
        .append(" To Distance: ")
        .append(radiusMeters)
        .append(" m")
        .append(" Lower Left: [")
        .append(minLon)
        .append(',')
        .append(minLat)
        .append(']')
        .append(" Upper Right: [")
        .append(maxLon)
        .append(',')
        .append(maxLat)
        .append("]")
        .toString();
  }

  public double getMinRadiusMeters() {
    return this.minRadiusMeters;
  }

  public double getMaxRadiusMeters() {
    return this.radiusMeters;
  }
}
