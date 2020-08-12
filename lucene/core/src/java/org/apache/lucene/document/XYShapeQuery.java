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

    double minY = XYEncodingUtils.decode(NumericUtils.sortableBytesToInt(minTriangle, minYOffset));
    double minX = XYEncodingUtils.decode(NumericUtils.sortableBytesToInt(minTriangle, minXOffset));
    double maxY = XYEncodingUtils.decode(NumericUtils.sortableBytesToInt(maxTriangle, maxYOffset));
    double maxX = XYEncodingUtils.decode(NumericUtils.sortableBytesToInt(maxTriangle, maxXOffset));

    // check internal node against query
    return component2D.relate(minX, maxX, minY, maxY);
  }

  @Override
  protected boolean queryIntersects(byte[] t, ShapeField.DecodedTriangle scratchTriangle) {
    ShapeField.decodeTriangle(t, scratchTriangle);

    switch (scratchTriangle.type) {
      case POINT: {
        double y = decode(scratchTriangle.aY);
        double x = decode(scratchTriangle.aX);
        return component2D.contains(x, y);
      }
      case LINE: {
        double aY = decode(scratchTriangle.aY);
        double aX = decode(scratchTriangle.aX);
        double bY = decode(scratchTriangle.bY);
        double bX = decode(scratchTriangle.bX);
        return component2D.intersectsLine(aX, aY, bX, bY);
      }
      case TRIANGLE: {
        double aY = decode(scratchTriangle.aY);
        double aX = decode(scratchTriangle.aX);
        double bY = decode(scratchTriangle.bY);
        double bX = decode(scratchTriangle.bX);
        double cY = decode(scratchTriangle.cY);
        double cX = decode(scratchTriangle.cX);
        return component2D.intersectsTriangle(aX, aY, bX, bY, cX, cY);
      }
      default: throw new IllegalArgumentException("Unsupported triangle type :[" + scratchTriangle.type + "]");
    }
  }

  @Override
  protected boolean queryContains(byte[] t, ShapeField.DecodedTriangle scratchTriangle) {
    ShapeField.decodeTriangle(t, scratchTriangle);

    switch (scratchTriangle.type) {
      case POINT: {
        double y = decode(scratchTriangle.aY);
        double x = decode(scratchTriangle.aX);
        return component2D.contains(x, y);
      }
      case LINE: {
        double aY = decode(scratchTriangle.aY);
        double aX = decode(scratchTriangle.aX);
        double bY = decode(scratchTriangle.bY);
        double bX = decode(scratchTriangle.bX);
        return component2D.containsLine(aX, aY, bX, bY);
      }
      case TRIANGLE: {
        double aY = decode(scratchTriangle.aY);
        double aX = decode(scratchTriangle.aX);
        double bY = decode(scratchTriangle.bY);
        double bX = decode(scratchTriangle.bX);
        double cY = decode(scratchTriangle.cY);
        double cX = decode(scratchTriangle.cX);
        return component2D.containsTriangle(aX, aY, bX, bY, cX, cY);
      }
      default: throw new IllegalArgumentException("Unsupported triangle type :[" + scratchTriangle.type + "]");
    }
  }

  @Override
  protected Component2D.WithinRelation queryWithin(byte[] t, ShapeField.DecodedTriangle scratchTriangle) {
    ShapeField.decodeTriangle(t, scratchTriangle);

    switch (scratchTriangle.type) {
      case POINT: {
        double y = decode(scratchTriangle.aY);
        double x = decode(scratchTriangle.aX);
        return component2D.withinPoint(x, y);
      }
      case LINE: {
        double aY = decode(scratchTriangle.aY);
        double aX = decode(scratchTriangle.aX);
        double bY = decode(scratchTriangle.bY);
        double bX = decode(scratchTriangle.bX);
        return component2D.withinLine(aX, aY, scratchTriangle.ab, bX, bY);
      }
      case TRIANGLE: {
        double aY = decode(scratchTriangle.aY);
        double aX = decode(scratchTriangle.aX);
        double bY = decode(scratchTriangle.bY);
        double bX = decode(scratchTriangle.bX);
        double cY = decode(scratchTriangle.cY);
        double cX = decode(scratchTriangle.cX);
        return component2D.withinTriangle(aX, aY, scratchTriangle.ab, bX, bY, scratchTriangle.bc, cX, cY, scratchTriangle.ca);
      }
      default: throw new IllegalArgumentException("Unsupported triangle type :[" + scratchTriangle.type + "]");
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