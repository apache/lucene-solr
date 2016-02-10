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
import org.apache.lucene.spatial.util.GeoRelationUtils;

/** Package private implementation for the public facing GeoPointInPolygonQuery delegate class.
 *
 *    @lucene.experimental
 */
final class GeoPointInPolygonQueryImpl extends GeoPointInBBoxQueryImpl {
  private final GeoPointInPolygonQuery polygonQuery;

  GeoPointInPolygonQueryImpl(final String field, final TermEncoding termEncoding, final GeoPointInPolygonQuery q,
                             final double minLon, final double minLat, final double maxLon, final double maxLat) {
    super(field, termEncoding, minLon, minLat, maxLon, maxLat);
    polygonQuery = q;
  }

  @Override
  public void setRewriteMethod(MultiTermQuery.RewriteMethod method) {
    throw new UnsupportedOperationException("cannot change rewrite method");
  }

  @Override
  protected CellComparator newCellComparator() {
    return new GeoPolygonCellComparator(this);
  }

  /**
   * Custom {@code org.apache.lucene.spatial.geopoint.search.GeoPointMultiTermQuery.CellComparator} that computes morton hash
   * ranges based on the defined edges of the provided polygon.
   */
  private final class GeoPolygonCellComparator extends CellComparator {
    GeoPolygonCellComparator(GeoPointMultiTermQuery query) {
      super(query);
    }

    @Override
    protected boolean cellCrosses(final double minLon, final double minLat, final double maxLon, final double maxLat) {
      return GeoRelationUtils.rectCrossesPolyApprox(minLon, minLat, maxLon, maxLat, polygonQuery.x, polygonQuery.y,
          polygonQuery.minLon, polygonQuery.minLat, polygonQuery.maxLon, polygonQuery.maxLat);
    }

    @Override
    protected boolean cellWithin(final double minLon, final double minLat, final double maxLon, final double maxLat) {
      return GeoRelationUtils.rectWithinPolyApprox(minLon, minLat, maxLon, maxLat, polygonQuery.x, polygonQuery.y,
          polygonQuery.minLon, polygonQuery.minLat, polygonQuery.maxLon, polygonQuery.maxLat);
    }

    @Override
    protected boolean cellIntersectsShape(final double minLon, final double minLat, final double maxLon, final double maxLat) {
      return cellContains(minLon, minLat, maxLon, maxLat) || cellWithin(minLon, minLat, maxLon, maxLat)
          || cellCrosses(minLon, minLat, maxLon, maxLat);
    }

    /**
     * The two-phase query approach. The parent
     * {@link org.apache.lucene.spatial.geopoint.search.GeoPointTermsEnum#accept} method is called to match
     * encoded terms that fall within the bounding box of the polygon. Those documents that pass the initial
     * bounding box filter are then compared to the provided polygon using the
     * {@link org.apache.lucene.spatial.util.GeoRelationUtils#pointInPolygon} method.
     */
    @Override
    protected boolean postFilter(final double lon, final double lat) {
      return GeoRelationUtils.pointInPolygon(polygonQuery.x, polygonQuery.y, lat, lon);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;

    GeoPointInPolygonQueryImpl that = (GeoPointInPolygonQueryImpl) o;

    return !(polygonQuery != null ? !polygonQuery.equals(that.polygonQuery) : that.polygonQuery != null);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (polygonQuery != null ? polygonQuery.hashCode() : 0);
    return result;
  }
}
