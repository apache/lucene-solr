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

import org.apache.lucene.geo.Circle;
import org.apache.lucene.geo.Circle2D;
import org.apache.lucene.index.PointValues.Relation;

/**
 * Finds all previously indexed shapes that intersect the specified distance query.
 *
 * <p>The field must be indexed using
 * {@link LatLonShape#createIndexableFields} added per document.
 *
 *  @lucene.experimental
 **/
final class LatLonShapeDistanceQuery extends ShapeQuery {
  final Circle circle;
  final Circle2D circle2D;

  public LatLonShapeDistanceQuery(String field, ShapeField.QueryRelation queryRelation, Circle circle) {
    super(field, queryRelation);
    this.circle = circle;
    this.circle2D = Circle2D.create(circle);
  }

  @Override
  protected Relation relateRangeBBoxToQuery(int minXOffset, int minYOffset, byte[] minTriangle,
                                            int maxXOffset, int maxYOffset, byte[] maxTriangle) {
    return circle2D.relateRangeBBox(minXOffset, minYOffset, minTriangle, maxXOffset, maxYOffset, maxTriangle);
  }

  @Override
  protected boolean queryMatches(byte[] triangle, ShapeField.DecodedTriangle scratchTriangle, ShapeField.QueryRelation queryRelation) {
    // decode indexed triangle
    ShapeField.decodeTriangle(triangle, scratchTriangle);

    int aY = scratchTriangle.aY;
    int aX = scratchTriangle.aX;
    int bY = scratchTriangle.bY;
    int bX = scratchTriangle.bX;
    int cY = scratchTriangle.cY;
    int cX = scratchTriangle.cX;


    switch (queryRelation) {
      case INTERSECTS: return circle2D.intersectsTriangle(aX, aY, bX, bY, cX, cY);
      case WITHIN: return circle2D.containsTriangle(aX, aY, bX, bY, cX, cY);
      case DISJOINT: return circle2D.intersectsTriangle(aX, aY, bX, bY, cX, cY) == false;
      default: throw new IllegalArgumentException("Unsupported query type :[" + queryRelation + "]");
    }
  }

  @Override
  public boolean equals(Object o) {
    return sameClassAs(o) && equalsTo(getClass().cast(o));
  }

  @Override
  protected boolean equalsTo(Object o) {
    return super.equalsTo(o) && circle.equals(((LatLonShapeDistanceQuery)o).circle);
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
