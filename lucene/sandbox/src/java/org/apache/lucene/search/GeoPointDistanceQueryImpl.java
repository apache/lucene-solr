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

import org.apache.lucene.document.GeoPointField;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.GeoRect;
import org.apache.lucene.util.GeoRelationUtils;
import org.apache.lucene.util.GeoUtils;
import org.apache.lucene.util.SloppyMath;

/** Package private implementation for the public facing GeoPointDistanceQuery delegate class.
 *
 *    @lucene.experimental
 */
final class GeoPointDistanceQueryImpl extends GeoPointInBBoxQueryImpl {
  private final GeoPointDistanceQuery query;
  private final double centerLon;

  GeoPointDistanceQueryImpl(final String field, final GeoPointDistanceQuery q, final double centerLonUnwrapped,
                            final GeoRect bbox) {
    super(field, bbox.minLon, bbox.minLat, bbox.maxLon, bbox.maxLat);
    query = q;
    centerLon = centerLonUnwrapped;
  }

  @Override @SuppressWarnings("unchecked")
  protected TermsEnum getTermsEnum(final Terms terms, AttributeSource atts) throws IOException {
    return new GeoPointRadiusTermsEnum(terms.iterator(), this.minLon, this.minLat, this.maxLon, this.maxLat);
  }

  @Override
  public void setRewriteMethod(RewriteMethod method) {
    throw new UnsupportedOperationException("cannot change rewrite method");
  }

  private final class GeoPointRadiusTermsEnum extends GeoPointTermsEnum {
    GeoPointRadiusTermsEnum(final TermsEnum tenum, final double minLon, final double minLat,
                            final double maxLon, final double maxLat) {
      super(tenum, minLon, minLat, maxLon, maxLat);
    }

    /**
     * Computes the maximum shift for the given pointDistanceQuery. This prevents unnecessary depth traversal
     * given the size of the distance query.
     */
    @Override
    protected short computeMaxShift() {
      final short shiftFactor;

      if (query.radiusMeters > 1000000) {
        shiftFactor = 5;
      } else {
        shiftFactor = 4;
      }

      return (short)(GeoPointField.PRECISION_STEP * shiftFactor);
    }

    @Override
    protected boolean cellCrosses(final double minLon, final double minLat, final double maxLon, final double maxLat) {
      return GeoRelationUtils.rectCrossesCircle(minLon, minLat, maxLon, maxLat,
          centerLon, query.centerLat, query.radiusMeters, true);
    }

    @Override
    protected boolean cellWithin(final double minLon, final double minLat, final double maxLon, final double maxLat) {
      return GeoRelationUtils.rectWithinCircle(minLon, minLat, maxLon, maxLat,
          centerLon, query.centerLat, query.radiusMeters, true);
    }

    @Override
    protected boolean cellIntersectsShape(final double minLon, final double minLat, final double maxLon, final double maxLat) {
      return (cellContains(minLon, minLat, maxLon, maxLat)
          || cellWithin(minLon, minLat, maxLon, maxLat) || cellCrosses(minLon, minLat, maxLon, maxLat));
    }

    /**
     * The two-phase query approach. The parent {@link org.apache.lucene.search.GeoPointTermsEnum} class matches
     * encoded terms that fall within the minimum bounding box of the point-radius circle. Those documents that pass
     * the initial bounding box filter are then post filter compared to the provided distance using the
     * {@link org.apache.lucene.util.SloppyMath#haversin} method.
     */
    @Override
    protected boolean postFilter(final double lon, final double lat) {
      return (SloppyMath.haversin(query.centerLat, centerLon, lat, lon) * 1000.0 <= query.radiusMeters);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof GeoPointDistanceQueryImpl)) return false;
    if (!super.equals(o)) return false;

    GeoPointDistanceQueryImpl that = (GeoPointDistanceQueryImpl) o;

    if (!query.equals(that.query)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + query.hashCode();
    return result;
  }

  public double getRadiusMeters() {
    return query.getRadiusMeters();
  }
}
