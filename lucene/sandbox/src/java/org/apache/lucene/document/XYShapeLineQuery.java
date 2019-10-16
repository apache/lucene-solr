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
import org.apache.lucene.geo.Line2D;
import org.apache.lucene.geo.XYLine;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.util.NumericUtils;

import static org.apache.lucene.geo.XYEncodingUtils.decode;

/**
 * Finds all previously indexed cartesian shapes that intersect the specified arbitrary {@code XYLine}.
 * <p>
 * Note:
 * <ul>
 *    <li>{@code QueryRelation.WITHIN} queries are not yet supported</li>
 * </ul>
 * <p>
 * todo:
 * <ul>
 *   <li>Add distance support for buffered queries</li>
 * </ul>
 * <p>The field must be indexed using
 * {@link org.apache.lucene.document.XYShape#createIndexableFields} added per document.
 *
 *  @lucene.experimental
 **/
final class XYShapeLineQuery extends ShapeQuery {
  final XYLine[] lines;
  final private Component2D line2D;

  public XYShapeLineQuery(String field, QueryRelation queryRelation, XYLine... lines) {
    super(field, queryRelation);
    /** line queries do not support within relations, only intersects and disjoint */
    if (queryRelation == QueryRelation.WITHIN) {
      throw new IllegalArgumentException("XYShapeLineQuery does not support " + QueryRelation.WITHIN + " queries");
    }

    if (lines == null) {
      throw new IllegalArgumentException("lines must not be null");
    }
    if (lines.length == 0) {
      throw new IllegalArgumentException("lines must not be empty");
    }
    for (int i = 0; i < lines.length; ++i) {
      if (lines[i] == null) {
        throw new IllegalArgumentException("line[" + i + "] must not be null");
      } else if (lines[i].minX > lines[i].maxX) {
        throw new IllegalArgumentException("XYShapeLineQuery: minX cannot be greater than maxX.");
      } else if (lines[i].minY > lines[i].maxY) {
        throw new IllegalArgumentException("XYShapeLineQuery: minY cannot be greater than maxY.");
      }
    }
    this.lines = lines.clone();
    this.line2D = Line2D.create(lines);
  }

  @Override
  protected Relation relateRangeBBoxToQuery(int minXOffset, int minYOffset, byte[] minTriangle,
                                            int maxXOffset, int maxYOffset, byte[] maxTriangle) {
    double minY = decode(NumericUtils.sortableBytesToInt(minTriangle, minYOffset));
    double minX = decode(NumericUtils.sortableBytesToInt(minTriangle, minXOffset));
    double maxY = decode(NumericUtils.sortableBytesToInt(maxTriangle, maxYOffset));
    double maxX = decode(NumericUtils.sortableBytesToInt(maxTriangle, maxXOffset));

    // check internal node against query
    return line2D.relate(minX, maxX, minY, maxY);
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
      case INTERSECTS: return line2D.relateTriangle(alon, alat, blon, blat, clon, clat) != Relation.CELL_OUTSIDE_QUERY;
      case WITHIN: return line2D.relateTriangle(alon, alat, blon, blat, clon, clat) == Relation.CELL_INSIDE_QUERY;
      case DISJOINT: return line2D.relateTriangle(alon, alat, blon, blat, clon, clat) == Relation.CELL_OUTSIDE_QUERY;
      default: throw new IllegalArgumentException("Unsupported query type :[" + queryRelation + "]");
    }
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
    sb.append("XYLine(").append(lines[0].toGeoJSON()).append(")");
    return sb.toString();
  }

  @Override
  protected boolean equalsTo(Object o) {
    return super.equalsTo(o) && Arrays.equals(lines, ((XYShapeLineQuery)o).lines);
  }

  @Override
  public int hashCode() {
    int hash = super.hashCode();
    hash = 31 * hash + Arrays.hashCode(lines);
    return hash;
  }
}
