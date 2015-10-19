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
import java.util.List;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.util.GeoProjectionUtils;

/** Implements a point distance range query on a GeoPoint field. This is based on
 * {@code org.apache.lucene.search.GeoPointDistanceQuery} and is implemented using a
 * {@code org.apache.lucene.search.BooleanClause.MUST_NOT} clause to exclude any points that fall within
 * minRadius from the provided point.
 *
 *    @lucene.experimental
 */
public final class GeoPointDistanceRangeQuery extends GeoPointDistanceQuery {
  protected final double minRadius;

  public GeoPointDistanceRangeQuery(final String field, final double centerLon, final double centerLat,
                                    final double minRadius, final double maxRadius) {
    super(field, centerLon, centerLat, maxRadius);
    this.minRadius = minRadius;
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    if (getBoost() != 1f) {
      return super.rewrite(reader);
    }
    Query q = super.rewrite(reader);
    if (minRadius == 0.0) {
      return q;
    }

    final double radius;
    if (q instanceof BooleanQuery) {
      final List<BooleanClause> clauses = ((BooleanQuery)q).clauses();
      assert clauses.size() > 0;
      radius = ((GeoPointDistanceQueryImpl)(clauses.get(0).getQuery())).getRadius();
    } else {
      radius = ((GeoPointDistanceQueryImpl)q).getRadius();
    }

    // add an exclusion query
    BooleanQuery.Builder bqb = new BooleanQuery.Builder();

    // create a new exclusion query
    GeoPointDistanceQuery exclude = new GeoPointDistanceQuery(field, centerLon, centerLat, minRadius);
    // full map search
    if (radius >= GeoProjectionUtils.SEMIMINOR_AXIS) {
      bqb.add(new BooleanClause(new GeoPointInBBoxQuery(this.field, -180.0, -90.0, 180.0, 90.0), BooleanClause.Occur.MUST));
    } else {
      bqb.add(new BooleanClause(q, BooleanClause.Occur.MUST));
    }
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
        .append(minRadius)
        .append(" m")
        .append(" To Distance: ")
        .append(radius)
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
    return this.minRadius;
  }

  public double getMaxRadiusMeters() {
    return this.radius;
  }
}
