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
package org.apache.lucene.document;

import java.util.Objects;

import org.apache.lucene.document.LatLonShape.QueryRelation;
import org.apache.lucene.geo.Component;
import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.util.NumericUtils;

/**
 * Finds all previously indexed shapes that intersect the specified arbitrary component tree.
 *
 * <p>The field must be indexed using
 * {@link LatLonShape#createIndexableFields} added per document.
 *
 *  @lucene.experimental
 **/
final class LatLonShapeComponentQuery extends LatLonShapeQuery {
  final private Component componentTree;

  /**
   * Creates a query that matches all indexed shapes to the provided polygons
   */
  public LatLonShapeComponentQuery(String field, QueryRelation queryRelation, Component componentTree) {
    super(field, queryRelation);
    if (componentTree == null) {
      throw new IllegalArgumentException("componentTree must not be null");
    }
    this.componentTree = componentTree;
  }

  @Override
  protected Relation relateRangeBBoxToQuery(int minXOffset, int minYOffset, byte[] minTriangle,
                                            int maxXOffset, int maxYOffset, byte[] maxTriangle) {

    double minLat = GeoEncodingUtils.decodeLatitude(NumericUtils.sortableBytesToInt(minTriangle, minYOffset));
    double minLon = GeoEncodingUtils.decodeLongitude(NumericUtils.sortableBytesToInt(minTriangle, minXOffset));
    double maxLat = GeoEncodingUtils.decodeLatitude(NumericUtils.sortableBytesToInt(maxTriangle, maxYOffset));
    double maxLon = GeoEncodingUtils.decodeLongitude(NumericUtils.sortableBytesToInt(maxTriangle, maxXOffset));

    // check internal node against query
    return componentTree.relate(minLat, maxLat, minLon, maxLon);
  }

  @Override
  protected boolean queryMatches(byte[] t, int[] scratchTriangle, QueryRelation queryRelation) {
    LatLonShape.decodeTriangle(t, scratchTriangle);

    double alat = GeoEncodingUtils.decodeLatitude(scratchTriangle[0]);
    double alon = GeoEncodingUtils.decodeLongitude(scratchTriangle[1]);
    double blat = GeoEncodingUtils.decodeLatitude(scratchTriangle[2]);
    double blon = GeoEncodingUtils.decodeLongitude(scratchTriangle[3]);
    double clat = GeoEncodingUtils.decodeLatitude(scratchTriangle[4]);
    double clon = GeoEncodingUtils.decodeLongitude(scratchTriangle[5]);

    if (queryRelation == QueryRelation.WITHIN) {
      return componentTree.relateTriangle(alon, alat, blon, blat, clon, clat) == Relation.CELL_INSIDE_QUERY;
    }
    // INTERSECTS
    return componentTree.relateTriangle(alon, alat, blon, blat, clon, clat) != Relation.CELL_OUTSIDE_QUERY;
  }

  @Override
  public String toString(String field) {
    final StringBuilder sb = new StringBuilder();
    sb.append(getClass().getSimpleName());
    sb.append(':');
    if (this.field.equals(field) == false) {
      sb.append(" field=");
      sb.append(this.field);
      sb.append(':');
    }
    sb.append(componentTree.toString());
    return sb.toString();
  }

  @Override
  protected boolean equalsTo(Object o) {
    return super.equalsTo(o) &&  Objects.equals(componentTree, ((LatLonShapeComponentQuery)o).componentTree);
  }

  @Override
  public int hashCode() {
    int hash = super.hashCode();
    hash = 31 * hash + Objects.hashCode(componentTree);
    return hash;
  }
}