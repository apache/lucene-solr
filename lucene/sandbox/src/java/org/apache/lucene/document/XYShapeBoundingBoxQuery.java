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
import org.apache.lucene.geo.XYRectangle;
import org.apache.lucene.geo.XYRectangle2D;
import org.apache.lucene.index.PointValues;

/**
 * Finds all previously indexed cartesian shapes that intersect the specified bounding box.
 *
 * <p>The field must be indexed using
 * {@link org.apache.lucene.document.XYShape#createIndexableFields} added per document.
 *
 *  @lucene.experimental
 **/
public class XYShapeBoundingBoxQuery extends ShapeQuery {
  final XYRectangle2D rectangle2D;

  public XYShapeBoundingBoxQuery(String field, QueryRelation queryRelation, double minX, double maxX, double minY, double maxY) {
    super(field, queryRelation);
    XYRectangle rectangle = new XYRectangle(minX, maxX, minY, maxY);
    this.rectangle2D = XYRectangle2D.create(rectangle);
  }

  @Override
  protected PointValues.Relation relateRangeBBoxToQuery(int minXOffset, int minYOffset, byte[] minTriangle,
                                                        int maxXOffset, int maxYOffset, byte[] maxTriangle) {
    if (queryRelation == QueryRelation.INTERSECTS || queryRelation == QueryRelation.DISJOINT) {
      return rectangle2D.intersectRangeBBox(minXOffset, minYOffset, minTriangle, maxXOffset, maxYOffset, maxTriangle);
    }
    return rectangle2D.relateRangeBBox(minXOffset, minYOffset, minTriangle, maxXOffset, maxYOffset, maxTriangle);
  }

  /** returns true if the query matches the encoded triangle */
  @Override
  protected boolean queryMatches(byte[] t, ShapeField.DecodedTriangle scratchTriangle, QueryRelation queryRelation) {
    // decode indexed triangle
    ShapeField.decodeTriangle(t, scratchTriangle);

    int aY = scratchTriangle.aY;
    int aX = scratchTriangle.aX;
    int bY = scratchTriangle.bY;
    int bX = scratchTriangle.bX;
    int cY = scratchTriangle.cY;
    int cX = scratchTriangle.cX;

    switch (queryRelation) {
      case INTERSECTS: return rectangle2D.intersectsTriangle(aX, aY, bX, bY, cX, cY);
      case WITHIN: return rectangle2D.containsTriangle(aX, aY, bX, bY, cX, cY);
      case DISJOINT: return rectangle2D.intersectsTriangle(aX, aY, bX, bY, cX, cY) == false;
      default: throw new IllegalArgumentException("Unsupported query type :[" + queryRelation + "]");
    }
  }

  @Override
  public boolean equals(Object o) {
    return sameClassAs(o) && equalsTo(getClass().cast(o));
  }

  @Override
  protected boolean equalsTo(Object o) {
    return super.equalsTo(o) && rectangle2D.equals(((XYShapeBoundingBoxQuery)o).rectangle2D);
  }

  @Override
  public int hashCode() {
    int hash = super.hashCode();
    hash = 31 * hash + rectangle2D.hashCode();
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
    sb.append(rectangle2D.toString());
    return sb.toString();
  }
}
