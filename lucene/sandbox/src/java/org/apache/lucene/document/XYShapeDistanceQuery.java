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

import org.apache.lucene.geo.XYCircle;
import org.apache.lucene.geo.XYCircle2D;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.util.NumericUtils;

import static org.apache.lucene.geo.XYEncodingUtils.decode;

/**
 * Finds all previously indexed shapes that intersect the specified distance query.
 *
 * <p>The field must be indexed using
 * {@link LatLonShape#createIndexableFields} added per document.
 *
 *  @lucene.experimental
 **/
final class XYShapeDistanceQuery extends ShapeQuery {
  final XYCircle circle;
  final XYCircle2D circle2D;

  public XYShapeDistanceQuery(String field, ShapeField.QueryRelation queryRelation, XYCircle circle) {
    super(field, queryRelation);
    this.circle = circle;
    this.circle2D = XYCircle2D.create(circle);
  }

  @Override
  protected Relation relateRangeBBoxToQuery(int minXOffset, int minYOffset, byte[] minTriangle,
                                            int maxXOffset, int maxYOffset, byte[] maxTriangle) {
    double minY = decode(NumericUtils.sortableBytesToInt(minTriangle, minYOffset));
    double minX = decode(NumericUtils.sortableBytesToInt(minTriangle, minXOffset));
    double maxY = decode(NumericUtils.sortableBytesToInt(maxTriangle, maxYOffset));
    double maxX = decode(NumericUtils.sortableBytesToInt(maxTriangle, maxXOffset));
    return circle2D.relate(minX, maxX, minY, maxY);
  }

  @Override
  protected boolean queryMatches(byte[] triangle, ShapeField.DecodedTriangle scratchTriangle, ShapeField.QueryRelation queryRelation) {
    // decode indexed triangle
    ShapeField.decodeTriangle(triangle, scratchTriangle);

    double alat = decode(scratchTriangle.aY);
    double alon = decode(scratchTriangle.aX);
    double blat = decode(scratchTriangle.bY);
    double blon = decode(scratchTriangle.bX);
    double clat = decode(scratchTriangle.cY);
    double clon = decode(scratchTriangle.cX);

    switch (queryRelation) {
      case INTERSECTS: return circle2D.relateTriangle(alon, alat, blon, blat, clon, clat) != Relation.CELL_OUTSIDE_QUERY;
      case WITHIN: return circle2D.relateTriangle(alon, alat, blon, blat, clon, clat) == Relation.CELL_INSIDE_QUERY;
      case DISJOINT: return circle2D.relateTriangle(alon, alat, blon, blat, clon, clat) == Relation.CELL_OUTSIDE_QUERY;
      default: throw new IllegalArgumentException("Unsupported query type :[" + queryRelation + "]");
    }
  }

  @Override
  public boolean equals(Object o) {
    return sameClassAs(o) && equalsTo(getClass().cast(o));
  }

  @Override
  protected boolean equalsTo(Object o) {
    return super.equalsTo(o) && circle.equals(((XYShapeDistanceQuery)o).circle);
  }

  @Override
  public int hashCode() {
    int hash = super.hashCode();
    hash = 31 * hash + circle.hashCode();
    return hash;
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
    sb.append(circle.toString());
    return sb.toString();
  }
}
