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

import org.apache.lucene.geo.Line;
import org.apache.lucene.geo.Polygon;
import org.apache.lucene.geo.Polygon2D;
import org.apache.lucene.index.PointValues.Relation;

import static org.apache.lucene.geo.GeoEncodingUtils.decodeLatitude;
import static org.apache.lucene.geo.GeoEncodingUtils.decodeLongitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLatitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLongitude;

/** random bounding box and polygon query tests for random generated {@link Line} types */
public class TestLatLonLineShapeQueries extends BaseLatLonShapeTestCase {

  protected final LineValidator VALIDATOR = new LineValidator();

  @Override
  protected ShapeType getShapeType() {
    return ShapeType.LINE;
  }

  @Override
  protected Field[] createIndexableFields(String field, Object line) {
    return LatLonShape.createIndexableFields(field, (Line)line);
  }

  @Override
  protected Validator getValidator() {
    return VALIDATOR;
  }

  protected class LineValidator implements Validator {
    @Override
    public boolean testBBoxQuery(double minLat, double maxLat, double minLon, double maxLon, Object shape) {
      // to keep it simple we convert the bbox into a polygon and use poly2d
      Polygon2D p = Polygon2D.create(new Polygon[] {new Polygon(new double[] {minLat, minLat, maxLat, maxLat, minLat},
          new double[] {minLon, maxLon, maxLon, minLon, minLon})});
      return testLine(p, (Line)shape);
    }

    @Override
    public boolean testPolygonQuery(Polygon2D poly2d, Object shape) {
      return testLine(poly2d, (Line) shape);
    }

    private boolean testLine(Polygon2D queryPoly, Line line) {
      double ax, ay, bx, by, temp;
      for (int i = 0, j = 1; j < line.numPoints(); ++i, ++j) {
        ay = decodeLatitude(encodeLatitude(line.getLat(i)));
        ax = decodeLongitude(encodeLongitude(line.getLon(i)));
        by = decodeLatitude(encodeLatitude(line.getLat(j)));
        bx = decodeLongitude(encodeLongitude(line.getLon(j)));
        if (ay > by) {
          temp = ay;
          ay = by;
          by = temp;
          temp = ax;
          ax = bx;
          bx = temp;
        } else if (ay == by) {
          if (ax > bx) {
            temp = ay;
            ay = by;
            by = temp;
            temp = ax;
            ax = bx;
            bx = temp;
          }
        }
        if (queryPoly.relateTriangle(ax, ay, bx, by, ax, ay) != Relation.CELL_OUTSIDE_QUERY) {
          return true;
        }
      }
      return false;
    }
  }
}
