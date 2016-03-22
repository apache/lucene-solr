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

import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.spatial.geopoint.document.GeoPointField.TermEncoding;
import org.apache.lucene.spatial.util.GeoRect;
import org.apache.lucene.util.SloppyMath;

/** Package private implementation for the public facing GeoPointDistanceQuery delegate class.
 *
 *    @lucene.experimental
 */
final class GeoPointDistanceQueryImpl extends GeoPointInBBoxQueryImpl {
  private final GeoPointDistanceQuery distanceQuery;
  private final double centerLon;

  GeoPointDistanceQueryImpl(final String field, final TermEncoding termEncoding, final GeoPointDistanceQuery q,
                            final double centerLonUnwrapped, final GeoRect bbox) {
    super(field, termEncoding, bbox.minLat, bbox.maxLat, bbox.minLon, bbox.maxLon);
    distanceQuery = q;
    centerLon = centerLonUnwrapped;
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
      if (maxLat < GeoPointDistanceQueryImpl.this.minLat ||
          maxLon < GeoPointDistanceQueryImpl.this.minLon ||
          minLat > GeoPointDistanceQueryImpl.this.maxLat ||
          minLon > GeoPointDistanceQueryImpl.this.maxLon) {
        return false;
      } else {
        return true;
      }
    }

    @Override
    protected boolean cellWithin(final double minLat, final double maxLat, final double minLon, final double maxLon) {
      // TODO: we call cellCrosses because of how the termsEnum logic works, helps us avoid some haversin() calls here.
      if (cellCrosses(minLat, maxLat, minLon, maxLon) && maxLon - centerLon < 90 && centerLon - minLon < 90 &&
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

    /**
     * The two-phase query approach. The parent {@link GeoPointTermsEnum} class matches
     * encoded terms that fall within the minimum bounding box of the point-radius circle. Those documents that pass
     * the initial bounding box filter are then post filter compared to the provided distance using the
     * {@link org.apache.lucene.util.SloppyMath#haversinMeters(double, double, double, double)} method.
     */
    @Override
    protected boolean postFilter(final double lat, final double lon) {
      return SloppyMath.haversinMeters(distanceQuery.centerLat, centerLon, lat, lon) <= distanceQuery.radiusMeters;
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
