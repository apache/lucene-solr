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

import org.apache.lucene.geo.Rectangle;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.spatial.geopoint.document.GeoPointField.TermEncoding;
import org.apache.lucene.util.SloppyMath;

/** Package private implementation for the public facing GeoPointDistanceQuery delegate class.
 *
 *    @lucene.experimental
 * @deprecated Use the higher performance {@code LatLonPoint#.newDistanceQuery} instead. */
@Deprecated
final class GeoPointDistanceQueryImpl extends GeoPointInBBoxQueryImpl {
  private final GeoPointDistanceQuery distanceQuery;
  private final double centerLon;

  // optimization, used for detecting axis cross
  final double axisLat;

  GeoPointDistanceQueryImpl(final String field, final TermEncoding termEncoding, final GeoPointDistanceQuery q,
                            final double centerLonUnwrapped, final Rectangle bbox) {
    super(field, termEncoding, bbox.minLat, bbox.maxLat, bbox.minLon, bbox.maxLon);
    distanceQuery = q;
    centerLon = centerLonUnwrapped;
    axisLat = Rectangle.axisLat(distanceQuery.centerLat, distanceQuery.radiusMeters);
  }

  @Override
  public void setRewriteMethod(MultiTermQuery.RewriteMethod method) {
    throw new UnsupportedOperationException("cannot change rewrite method");
  }

  @Override
  protected CellComparator newCellComparator() {
    return new GeoPointRadiusCellComparator(this);
  }

  private final class GeoPointRadiusCellComparator extends CellComparator {
    GeoPointRadiusCellComparator(GeoPointDistanceQueryImpl query) {
      super(query);
    }

    @Override
    protected boolean cellCrosses(final double minLat, final double maxLat, final double minLon, final double maxLon) {
      // bounding box check
      if (maxLat < GeoPointDistanceQueryImpl.this.minLat ||
          maxLon < GeoPointDistanceQueryImpl.this.minLon ||
          minLat > GeoPointDistanceQueryImpl.this.maxLat ||
          minLon > GeoPointDistanceQueryImpl.this.maxLon) {
        return false;
      } else if ((centerLon < minLon || centerLon > maxLon) && (axisLat+ Rectangle.AXISLAT_ERROR < minLat || axisLat- Rectangle.AXISLAT_ERROR > maxLat)) {
        if (SloppyMath.haversinMeters(distanceQuery.centerLat, centerLon, minLat, minLon) > distanceQuery.radiusMeters &&
            SloppyMath.haversinMeters(distanceQuery.centerLat, centerLon, minLat, maxLon) > distanceQuery.radiusMeters &&
            SloppyMath.haversinMeters(distanceQuery.centerLat, centerLon, maxLat, minLon) > distanceQuery.radiusMeters &&
            SloppyMath.haversinMeters(distanceQuery.centerLat, centerLon, maxLat, maxLon) > distanceQuery.radiusMeters) {
          return false;
        }
      }
      return true;
    }

    @Override
    protected boolean cellWithin(final double minLat, final double maxLat, final double minLon, final double maxLon) {
      if (maxLon - centerLon < 90 && centerLon - minLon < 90 &&
          SloppyMath.haversinMeters(distanceQuery.centerLat, centerLon, minLat, minLon) <= distanceQuery.radiusMeters &&
          SloppyMath.haversinMeters(distanceQuery.centerLat, centerLon, minLat, maxLon) <= distanceQuery.radiusMeters &&
          SloppyMath.haversinMeters(distanceQuery.centerLat, centerLon, maxLat, minLon) <= distanceQuery.radiusMeters &&
          SloppyMath.haversinMeters(distanceQuery.centerLat, centerLon, maxLat, maxLon) <= distanceQuery.radiusMeters) {
        // we are fully enclosed, collect everything within this subtree
        return true;
      }
      return false;
    }

    @Override
    protected boolean cellIntersectsShape(final double minLat, final double maxLat, final double minLon, final double maxLon) {
      return cellCrosses(minLat, maxLat, minLon, maxLon);
    }

    @Override
    protected Relation relate(final double minLat, final double maxLat, final double minLon, final double maxLon) {
      // bounding check
      if (cellIntersectsMBR(minLat, maxLat, minLon, maxLon) == false) {
        return Relation.CELL_OUTSIDE_QUERY;
      }
      if ((centerLon < minLon || centerLon > maxLon) && (axisLat + Rectangle.AXISLAT_ERROR < minLat
          || axisLat- Rectangle.AXISLAT_ERROR > maxLat)) {
        if (SloppyMath.haversinSortKey(distanceQuery.centerLat, centerLon, minLat, minLon) > distanceQuery.sortKey &&
            SloppyMath.haversinSortKey(distanceQuery.centerLat, centerLon, minLat, maxLon) > distanceQuery.sortKey &&
            SloppyMath.haversinSortKey(distanceQuery.centerLat, centerLon, maxLat, minLon) > distanceQuery.sortKey &&
            SloppyMath.haversinSortKey(distanceQuery.centerLat, centerLon, maxLat, maxLon) > distanceQuery.sortKey) {
          return Relation.CELL_OUTSIDE_QUERY;
        }
      }

      if (maxLon - centerLon < 90 && centerLon - minLon < 90 &&
          SloppyMath.haversinSortKey(distanceQuery.centerLat, centerLon, minLat, minLon) <= distanceQuery.sortKey &&
          SloppyMath.haversinSortKey(distanceQuery.centerLat, centerLon, minLat, maxLon) <= distanceQuery.sortKey &&
          SloppyMath.haversinSortKey(distanceQuery.centerLat, centerLon, maxLat, minLon) <= distanceQuery.sortKey &&
          SloppyMath.haversinSortKey(distanceQuery.centerLat, centerLon, maxLat, maxLon) <= distanceQuery.sortKey) {
        // we are fully enclosed, collect everything within this subtree
        return Relation.CELL_INSIDE_QUERY;
      }

      return Relation.CELL_CROSSES_QUERY;
    }


    /**
     * The two-phase query approach. The parent {@link GeoPointTermsEnum} class matches
     * encoded terms that fall within the minimum bounding box of the point-radius circle. Those documents that pass
     * the initial bounding box filter are then post filter compared to the provided distance using the
     * {@link org.apache.lucene.util.SloppyMath#haversinMeters(double, double, double, double)} method.
     */
    @Override
    protected boolean postFilter(final double lat, final double lon) {
      // check bbox
      if (lat < minLat || lat > maxLat || lon < minLon || lon > maxLon) {
        return false;
      }

      // first check the partial distance, if its more than that, it can't be <= radiusMeters
      double h1 = SloppyMath.haversinSortKey(distanceQuery.centerLat, centerLon, lat, lon);
      if (h1 <= distanceQuery.sortKey) {
        return true;
      }

      // fully confirm with part 2:
      return false;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof GeoPointDistanceQueryImpl)) return false;
    if (!super.equals(o)) return false;

    GeoPointDistanceQueryImpl that = (GeoPointDistanceQueryImpl) o;

    if (!distanceQuery.equals(that.distanceQuery)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + distanceQuery.hashCode();
    return result;
  }

  public double getRadiusMeters() {
    return distanceQuery.getRadiusMeters();
  }
}
