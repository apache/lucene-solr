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
import org.apache.lucene.util.SloppyMath;
import org.apache.lucene.spatial.geopoint.document.GeoPointField;
import org.apache.lucene.spatial.geopoint.document.GeoPointField.TermEncoding;
import org.apache.lucene.spatial.util.GeoRelationUtils;

/** Package private implementation for the public facing GeoPointInBBoxQuery delegate class.
 *
 *    @lucene.experimental
 */
class GeoPointInBBoxQueryImpl extends GeoPointMultiTermQuery {
  /**
   * Constructs a new GeoBBoxQuery that will match encoded GeoPoint terms that fall within or on the boundary
   * of the bounding box defined by the input parameters
   * @param field the field name
   * @param minLon lower longitude (x) value of the bounding box
   * @param minLat lower latitude (y) value of the bounding box
   * @param maxLon upper longitude (x) value of the bounding box
   * @param maxLat upper latitude (y) value of the bounding box
   */
  GeoPointInBBoxQueryImpl(final String field, final TermEncoding termEncoding, final double minLon, final double minLat, final double maxLon, final double maxLat) {
    super(field, termEncoding, minLon, minLat, maxLon, maxLat);
  }

  @Override
  public void setRewriteMethod(MultiTermQuery.RewriteMethod method) {
    throw new UnsupportedOperationException("cannot change rewrite method");
  }

  @Override
  protected short computeMaxShift() {
    final short shiftFactor;

    // compute diagonal radius
    double midLon = (minLon + maxLon) * 0.5;
    double midLat = (minLat + maxLat) * 0.5;

    if (SloppyMath.haversin(minLat, minLon, midLat, midLon)*1000 > 1000000) {
      shiftFactor = 5;
    } else {
      shiftFactor = 4;
    }

    return (short)(GeoPointField.PRECISION_STEP * shiftFactor);
  }

  @Override
  protected CellComparator newCellComparator() {
    return new GeoPointInBBoxCellComparator(this);
  }

  private final class GeoPointInBBoxCellComparator extends CellComparator {
    GeoPointInBBoxCellComparator(GeoPointMultiTermQuery query) {
      super(query);
    }

    /**
     * Determine whether the quad-cell crosses the shape
     */
    @Override
    protected boolean cellCrosses(final double minLon, final double minLat, final double maxLon, final double maxLat) {
      return GeoRelationUtils.rectCrosses(minLon, minLat, maxLon, maxLat, GeoPointInBBoxQueryImpl.this.minLon,
          GeoPointInBBoxQueryImpl.this.minLat, GeoPointInBBoxQueryImpl.this.maxLon, GeoPointInBBoxQueryImpl.this.maxLat);    }

    /**
     * Determine whether quad-cell is within the shape
     */
    @Override
    protected boolean cellWithin(final double minLon, final double minLat, final double maxLon, final double maxLat) {
      return GeoRelationUtils.rectWithin(minLon, minLat, maxLon, maxLat, GeoPointInBBoxQueryImpl.this.minLon,
          GeoPointInBBoxQueryImpl.this.minLat, GeoPointInBBoxQueryImpl.this.maxLon, GeoPointInBBoxQueryImpl.this.maxLat);    }

    @Override
    protected boolean cellIntersectsShape(final double minLon, final double minLat, final double maxLon, final double maxLat) {
      return cellIntersectsMBR(minLon, minLat, maxLon, maxLat);
    }

    @Override
    protected boolean postFilter(final double lon, final double lat) {
      return GeoRelationUtils.pointInRectPrecise(lon, lat, minLon, minLat, maxLon, maxLat);
    }
  }

  @Override
  @SuppressWarnings({"unchecked","rawtypes"})
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;

    GeoPointInBBoxQueryImpl that = (GeoPointInBBoxQueryImpl) o;

    if (Double.compare(that.maxLat, maxLat) != 0) return false;
    if (Double.compare(that.maxLon, maxLon) != 0) return false;
    if (Double.compare(that.minLat, minLat) != 0) return false;
    if (Double.compare(that.minLon, minLon) != 0) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    long temp;
    temp = Double.doubleToLongBits(minLon);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    temp = Double.doubleToLongBits(minLat);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    temp = Double.doubleToLongBits(maxLon);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    temp = Double.doubleToLongBits(maxLat);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    return result;
  }

  @Override
  public String toString(String field) {
    final StringBuilder sb = new StringBuilder();
    sb.append(getClass().getSimpleName());
    sb.append(':');
    if (!getField().equals(field)) {
      sb.append(" field=");
      sb.append(getField());
      sb.append(':');
    }
    return sb.append(" Lower Left: [")
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
}
