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

import org.apache.lucene.component2D.Component2D;
import org.apache.lucene.document.LatLonShape.QueryRelation;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.util.NumericUtils;

/**
 * Finds all previously indexed shapes that intersect the specified arbitrary component2D tree.
 *
 * <p>The field must be indexed using
 * {@link LatLonShape#createIndexableFields} added per document.
 *
 *  @lucene.experimental
 **/
final class LatLonShapeComponentQuery extends LatLonShapeQuery {
  final private Component2D component;

  /**
   * Creates a query that matches all indexed shapes to the provided polygons
   */
  public LatLonShapeComponentQuery(String field, QueryRelation queryRelation, Component2D component) {
    super(field, queryRelation);
    if (component == null) {
      throw new IllegalArgumentException("componentTree must not be null");
    }
    this.component = component;
  }

  @Override
  protected Relation relateRangeBBoxToQuery(int minXOffset, int minYOffset, byte[] minTriangle,
                                            int maxXOffset, int maxYOffset, byte[] maxTriangle) {

    int minLat = NumericUtils.sortableBytesToInt(minTriangle, minYOffset);
    int minLon = NumericUtils.sortableBytesToInt(minTriangle, minXOffset);
    int maxLat = NumericUtils.sortableBytesToInt(maxTriangle, maxYOffset);
    int maxLon = NumericUtils.sortableBytesToInt(maxTriangle, maxXOffset);

    // check internal node against query
    return component.relate(minLon, maxLon, minLat, maxLat);
  }

  @Override
  protected boolean queryMatches(byte[] t, int[] scratchTriangle, QueryRelation queryRelation) {
    LatLonShape.decodeTriangle(t, scratchTriangle);

    int alat = scratchTriangle[0];
    int alon = scratchTriangle[1];
    int blat = scratchTriangle[2];
    int blon = scratchTriangle[3];
    int clat = scratchTriangle[4];
    int clon = scratchTriangle[5];

    int minLat = StrictMath.min(StrictMath.min(alat, blat), clat);
    int minLon = StrictMath.min(StrictMath.min(alon, blon), clon);
    int maxLat = StrictMath.max(StrictMath.max(alat, blat), clat);
    int maxLon = StrictMath.max(StrictMath.max(alon, blon), clon);

    if (queryRelation == QueryRelation.WITHIN) {
      return component.relateTriangle(minLon, maxLon, minLat, maxLat, alon, alat, blon, blat, clon, clat) == Relation.CELL_INSIDE_QUERY;
    }
    // INTERSECTS
    return component.relateTriangle(minLon, maxLon, minLat, maxLat, alon, alat, blon, blat, clon, clat) != Relation.CELL_OUTSIDE_QUERY;
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
    sb.append(component.toString());
    return sb.toString();
  }

  @Override
  protected boolean equalsTo(Object o) {
    return super.equalsTo(o) &&  Objects.equals(component, ((LatLonShapeComponentQuery)o).component);
  }

  @Override
  public int hashCode() {
    int hash = super.hashCode();
    hash = 31 * hash + Objects.hashCode(component);
    return hash;
  }
}