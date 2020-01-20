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


import org.apache.lucene.geo.Component2D;
import org.apache.lucene.geo.XYCircle;
import org.apache.lucene.geo.XYCircle2D;
import org.apache.lucene.geo.XYEncodingUtils;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.util.NumericUtils;

/**
 * Finds all previously indexed shapes that intersect the specified distance query.
 *
 * <p>The field must be indexed using
 * {@link XYShape#createIndexableFields} added per document.
 *
 **/
final class XYShapeDistanceQuery extends ShapeQuery {
  final XYCircle circle;
  final Component2D circle2D;

  public XYShapeDistanceQuery(String field, ShapeField.QueryRelation queryRelation, XYCircle circle) {
    super(field, queryRelation);
    this.circle = circle;
    this.circle2D = XYCircle2D.create(circle);
  }

  @Override
  protected Relation relateRangeBBoxToQuery(int minXOffset, int minYOffset, byte[] minTriangle,
                                            int maxXOffset, int maxYOffset, byte[] maxTriangle) {

    double minY = XYEncodingUtils.decode(NumericUtils.sortableBytesToInt(minTriangle, minYOffset));
    double minX = XYEncodingUtils.decode(NumericUtils.sortableBytesToInt(minTriangle, minXOffset));
    double maxY = XYEncodingUtils.decode(NumericUtils.sortableBytesToInt(maxTriangle, maxYOffset));
    double maxX = XYEncodingUtils.decode(NumericUtils.sortableBytesToInt(maxTriangle, maxXOffset));

    // check internal node against query
    return circle2D.relate(minX, maxX, minY, maxY);
  }

  @Override
  protected boolean queryMatches(byte[] t, ShapeField.DecodedTriangle scratchTriangle, ShapeField.QueryRelation queryRelation) {
    ShapeField.decodeTriangle(t, scratchTriangle);

    double aY = XYEncodingUtils.decode(scratchTriangle.aY);
    double aX = XYEncodingUtils.decode(scratchTriangle.aX);
    double bY = XYEncodingUtils.decode(scratchTriangle.bY);
    double bX = XYEncodingUtils.decode(scratchTriangle.bX);
    double cY = XYEncodingUtils.decode(scratchTriangle.cY);
    double cX = XYEncodingUtils.decode(scratchTriangle.cX);

    switch (queryRelation) {
      case INTERSECTS: return circle2D.relateTriangle(aX, aY, bX, bY, cX, cY) != Relation.CELL_OUTSIDE_QUERY;
      case WITHIN: return circle2D.relateTriangle(aX, aY, bX, bY, cX, cY) == Relation.CELL_INSIDE_QUERY;
      case DISJOINT: return circle2D.relateTriangle(aX, aY, bX, bY, cX, cY) == Relation.CELL_OUTSIDE_QUERY;
      default: throw new IllegalArgumentException("Unsupported query type :[" + queryRelation + "]");
    }
  }

  @Override
  protected Component2D.WithinRelation queryWithin(byte[] t, ShapeField.DecodedTriangle scratchTriangle) {
    ShapeField.decodeTriangle(t, scratchTriangle);

    double aY = XYEncodingUtils.decode(scratchTriangle.aY);
    double aX = XYEncodingUtils.decode(scratchTriangle.aX);
    double bY = XYEncodingUtils.decode(scratchTriangle.bY);
    double bX = XYEncodingUtils.decode(scratchTriangle.bX);
    double cY = XYEncodingUtils.decode(scratchTriangle.cY);
    double cX = XYEncodingUtils.decode(scratchTriangle.cX);

    return circle2D.withinTriangle(aX, aY, scratchTriangle.ab, bX, bY, scratchTriangle.bc, cX, cY, scratchTriangle.ca);
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
}
