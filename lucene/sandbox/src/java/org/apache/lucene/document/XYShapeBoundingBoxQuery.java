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

import org.apache.lucene.document.ShapeField.QueryRelation;
import org.apache.lucene.geo.Component2D;
import org.apache.lucene.geo.XYRectangle;
import org.apache.lucene.geo.XYRectangle2D;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.util.NumericUtils;

import static org.apache.lucene.geo.XYEncodingUtils.decode;

/**
 * Finds all previously indexed cartesian shapes that intersect the specified bounding box.
 *
 * <p>The field must be indexed using
 * {@link org.apache.lucene.document.XYShape#createIndexableFields} added per document.
 *
 *  @lucene.experimental
 **/
public class XYShapeBoundingBoxQuery extends ShapeQuery {
  final Component2D rectangle2D;
  final private XYRectangle rectangle;


  public XYShapeBoundingBoxQuery(String field, QueryRelation queryRelation, double minX, double maxX, double minY, double maxY) {
    super(field, queryRelation);
    this.rectangle = new XYRectangle(minX, maxX, minY, maxY);
    this.rectangle2D = XYRectangle2D.create(this.rectangle);
  }

  @Override
  protected PointValues.Relation relateRangeBBoxToQuery(int minXOffset, int minYOffset, byte[] minTriangle,
                                                        int maxXOffset, int maxYOffset, byte[] maxTriangle) {
    double minY = decode(NumericUtils.sortableBytesToInt(minTriangle, minYOffset));
    double minX = decode(NumericUtils.sortableBytesToInt(minTriangle, minXOffset));
    double maxY = decode(NumericUtils.sortableBytesToInt(maxTriangle, maxYOffset));
    double maxX = decode(NumericUtils.sortableBytesToInt(maxTriangle, maxXOffset));
    // check internal node against query
    PointValues.Relation rel = rectangle2D.relate(minX, maxX, minY, maxY);
    // TODO: Check if this really helps
    if (queryRelation == QueryRelation.INTERSECTS && rel == PointValues.Relation.CELL_CROSSES_QUERY) {
      // for intersects we can restrict the conditions by using the inner box
      double innerMaxY = decode(NumericUtils.sortableBytesToInt(maxTriangle, minYOffset));
      if (rectangle2D.relate(minX, maxX, minY, innerMaxY) == PointValues.Relation.CELL_INSIDE_QUERY) {
        return PointValues.Relation.CELL_INSIDE_QUERY;
      }
      double innerMaX = decode(NumericUtils.sortableBytesToInt(maxTriangle, minXOffset));
      if (rectangle2D.relate(minX, innerMaX, minY, maxY) == PointValues.Relation.CELL_INSIDE_QUERY) {
        return PointValues.Relation.CELL_INSIDE_QUERY;
      }
      double innerMinY = decode(NumericUtils.sortableBytesToInt(minTriangle, maxYOffset));
      if (rectangle2D.relate(minX, maxX, innerMinY, maxY) == PointValues.Relation.CELL_INSIDE_QUERY) {
        return PointValues.Relation.CELL_INSIDE_QUERY;
      }
      double innerMinX = decode(NumericUtils.sortableBytesToInt(minTriangle, maxXOffset));
      if (rectangle2D.relate(innerMinX, maxX, minY, maxY) == PointValues.Relation.CELL_INSIDE_QUERY) {
        return PointValues.Relation.CELL_INSIDE_QUERY;
      }
    }
    return rel;
  }

  /** returns true if the query matches the encoded triangle */
  @Override
  protected boolean queryMatches(byte[] t, ShapeField.DecodedTriangle scratchTriangle, QueryRelation queryRelation) {
    // decode indexed triangle
    ShapeField.decodeTriangle(t, scratchTriangle);

    double aY = decode(scratchTriangle.aY);
    double aX = decode(scratchTriangle.aX);
    double bY = decode(scratchTriangle.bY);
    double bX = decode(scratchTriangle.bX);
    double cY = decode(scratchTriangle.cY);
    double cX = decode(scratchTriangle.cX);

    switch (queryRelation) {
      case INTERSECTS: return rectangle2D.relateTriangle(aX, aY, bX, bY, cX, cY) != PointValues.Relation.CELL_OUTSIDE_QUERY;
      case WITHIN: return rectangle2D.contains(aX, aY) && rectangle2D.contains(bX, bY) && rectangle2D.contains(cX, cY);
      case DISJOINT: return rectangle2D.relateTriangle(aX, aY, bX, bY, cX, cY) == PointValues.Relation.CELL_OUTSIDE_QUERY;
      default: throw new IllegalArgumentException("Unsupported query type :[" + queryRelation + "]");
    }
  }

  @Override
  protected Component2D.WithinRelation queryWithin(byte[] t, ShapeField.DecodedTriangle scratchTriangle) {
    ShapeField.decodeTriangle(t, scratchTriangle);

    double aY = decode(scratchTriangle.aY);
    double aX = decode(scratchTriangle.aX);
    double bY = decode(scratchTriangle.bY);
    double bX = decode(scratchTriangle.bX);
    double cY = decode(scratchTriangle.cY);
    double cX = decode(scratchTriangle.cX);

    return rectangle2D.withinTriangle(aX, aY, scratchTriangle.ab, bX, bY, scratchTriangle.bc, cX, cY, scratchTriangle.ca);
  }

  @Override
  public boolean equals(Object o) {
    return sameClassAs(o) && equalsTo(getClass().cast(o));
  }

  @Override
  protected boolean equalsTo(Object o) {
    return super.equalsTo(o) && rectangle.equals(((XYShapeBoundingBoxQuery)o).rectangle);
  }

  @Override
  public int hashCode() {
    int hash = super.hashCode();
    hash = 31 * hash + rectangle.hashCode();
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
    sb.append(rectangle.toString());
    return sb.toString();
  }
}