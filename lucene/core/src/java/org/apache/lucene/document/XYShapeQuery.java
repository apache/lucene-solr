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

import java.util.Arrays;

import org.apache.lucene.document.ShapeField.QueryRelation;
import org.apache.lucene.geo.Component2D;
import org.apache.lucene.geo.XYEncodingUtils;
import org.apache.lucene.geo.XYGeometry;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.util.NumericUtils;

import static org.apache.lucene.geo.XYEncodingUtils.decode;

/**
 * Finds all previously indexed cartesian shapes that comply the given {@link QueryRelation} with
 * the specified array of {@link XYGeometry}.
 *
 * <p>The field must be indexed using {@link XYShape#createIndexableFields} added per document.
 **/
final class XYShapeQuery extends ShapeQuery {
  final XYGeometry[] geometries;
  final private Component2D component2D;

  /**
   * Creates a query that matches all indexed shapes to the provided polygons
   */
  XYShapeQuery(String field, QueryRelation queryRelation, XYGeometry... geometries) {
    super(field, queryRelation);
    this.component2D = XYGeometry.create(geometries);
    this.geometries = geometries.clone();
  }

  @Override
  protected Relation relateRangeBBoxToQuery(int minXOffset, int minYOffset, byte[] minTriangle,
                                            int maxXOffset, int maxYOffset, byte[] maxTriangle) {

    double minLat = XYEncodingUtils.decode(NumericUtils.sortableBytesToInt(minTriangle, minYOffset));
    double minLon = XYEncodingUtils.decode(NumericUtils.sortableBytesToInt(minTriangle, minXOffset));
    double maxLat = XYEncodingUtils.decode(NumericUtils.sortableBytesToInt(maxTriangle, maxYOffset));
    double maxLon = XYEncodingUtils.decode(NumericUtils.sortableBytesToInt(maxTriangle, maxXOffset));

    // check internal node against query
    return component2D.relate(minLon, maxLon, minLat, maxLat);
  }

  @Override
  protected boolean queryMatches(byte[] t, ShapeField.DecodedTriangle scratchTriangle, QueryRelation queryRelation) {
    ShapeField.decodeTriangle(t, scratchTriangle);

    double alat = decode(scratchTriangle.aY);
    double alon = decode(scratchTriangle.aX);
    double blat = decode(scratchTriangle.bY);
    double blon = decode(scratchTriangle.bX);
    double clat = decode(scratchTriangle.cY);
    double clon = decode(scratchTriangle.cX);

    switch (queryRelation) {
      case INTERSECTS: return component2D.relateTriangle(alon, alat, blon, blat, clon, clat) != Relation.CELL_OUTSIDE_QUERY;
      case WITHIN: return component2D.relateTriangle(alon, alat, blon, blat, clon, clat) == Relation.CELL_INSIDE_QUERY;
      case DISJOINT: return component2D.relateTriangle(alon, alat, blon, blat, clon, clat) == Relation.CELL_OUTSIDE_QUERY;
      default: throw new IllegalArgumentException("Unsupported query type :[" + queryRelation + "]");
    }
  }

  @Override
  protected Component2D.WithinRelation queryWithin(byte[] t, ShapeField.DecodedTriangle scratchTriangle) {
    ShapeField.decodeTriangle(t, scratchTriangle);

    double alat = decode(scratchTriangle.aY);
    double alon = decode(scratchTriangle.aX);
    double blat = decode(scratchTriangle.bY);
    double blon = decode(scratchTriangle.bX);
    double clat = decode(scratchTriangle.cY);
    double clon = decode(scratchTriangle.cX);

    return component2D.withinTriangle(alon, alat, scratchTriangle.ab, blon, blat, scratchTriangle.bc, clon, clat, scratchTriangle.ca);
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
    sb.append("[");
    for (int i = 0; i < geometries.length; i++) {
      sb.append(geometries[i].toString());
      sb.append(',');
    }
    sb.append(']');
    return sb.toString();
  }

  @Override
  protected boolean equalsTo(Object o) {
    return super.equalsTo(o) && Arrays.equals(geometries, ((XYShapeQuery)o).geometries);
  }

  @Override
  public int hashCode() {
    int hash = super.hashCode();
    hash = 31 * hash + Arrays.hashCode(geometries);
    return hash;
  }
}