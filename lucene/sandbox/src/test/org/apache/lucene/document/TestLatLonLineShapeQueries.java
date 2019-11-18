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


import com.carrotsearch.randomizedtesting.generators.RandomNumbers;
import org.apache.lucene.document.ShapeField.QueryRelation;
import org.apache.lucene.geo.Component2D;
import org.apache.lucene.geo.GeoTestUtil;
import org.apache.lucene.geo.Line;
import org.apache.lucene.geo.Rectangle;
import org.apache.lucene.geo.Rectangle2D;
import org.apache.lucene.index.PointValues.Relation;

/** random bounding box, line, and polygon query tests for random generated {@link Line} types */
@SuppressWarnings("SimpleText")
public class TestLatLonLineShapeQueries extends BaseLatLonShapeTestCase {

  @Override
  protected ShapeType getShapeType() {
    return ShapeType.LINE;
  }

  @Override
  protected Line randomQueryLine(Object... shapes) {
    if (random().nextInt(100) == 42) {
      // we want to ensure some cross, so randomly generate lines that share vertices with the indexed point set
      int maxBound = (int)Math.floor(shapes.length * 0.1d);
      if (maxBound < 2) {
        maxBound = shapes.length;
      }
      double[] lats = new double[RandomNumbers.randomIntBetween(random(), 2, maxBound)];
      double[] lons = new double[lats.length];
      for (int i = 0, j = 0; j < lats.length && i < shapes.length; ++i, ++j) {
        Line l = (Line) (shapes[i]);
        if (random().nextBoolean() && l != null) {
          int v = random().nextInt(l.numPoints() - 1);
          lats[j] = l.getLat(v);
          lons[j] = l.getLon(v);
        } else {
          lats[j] = GeoTestUtil.nextLatitude();
          lons[j] = GeoTestUtil.nextLongitude();
        }
      }
      return new Line(lats, lons);
    }
    return nextLine();
  }

  @Override
  protected Field[] createIndexableFields(String field, Object line) {
    return LatLonShape.createIndexableFields(field, (Line)line);
  }

  @Override
  protected Validator getValidator() {
    return new LineValidator(this.ENCODER);
  }

  protected static class LineValidator extends Validator {
    protected LineValidator(Encoder encoder) {
      super(encoder);
    }

    @Override
    public boolean testBBoxQuery(double minLat, double maxLat, double minLon, double maxLon, Object shape) {
      Line line = (Line)shape;
      Rectangle2D rectangle2D = Rectangle2D.create(new Rectangle(minLat, maxLat, minLon, maxLon));
      for (int i = 0, j = 1; j < line.numPoints(); ++i, ++j) {
        ShapeField.DecodedTriangle decoded = encoder.encodeDecodeTriangle(line.getLon(i), line.getLat(i), true, line.getLon(j), line.getLat(j), true, line.getLon(i), line.getLat(i), true);
        if (queryRelation == QueryRelation.WITHIN) {
          if (rectangle2D.containsTriangle(decoded.aX, decoded.aY, decoded.bX, decoded.bY, decoded.cX, decoded.cY) == false) {
            return false;
          }
        } else {
          if (rectangle2D.intersectsTriangle(decoded.aX, decoded.aY, decoded.bX, decoded.bY, decoded.cX, decoded.cY) == true) {
            return queryRelation == QueryRelation.INTERSECTS;
          }
        }
      }
      return queryRelation != QueryRelation.INTERSECTS;
    }

    @Override
    public boolean testComponentQuery(Component2D component2D, Object shape) {
      Line line = (Line) shape;
      for (int i = 0, j = 1; j < line.numPoints(); ++i, ++j) {
        double[] qTriangle = encoder.quantizeTriangle(line.getLon(i), line.getLat(i), true, line.getLon(j), line.getLat(j), true, line.getLon(i), line.getLat(i), true);
        Relation r = component2D.relateTriangle(qTriangle[1], qTriangle[0], qTriangle[3], qTriangle[2], qTriangle[5], qTriangle[4]);
        if (queryRelation == QueryRelation.DISJOINT) {
          if (r != Relation.CELL_OUTSIDE_QUERY) return false;
        } else if (queryRelation == QueryRelation.WITHIN) {
          if (r != Relation.CELL_INSIDE_QUERY) return false;
        } else {
          if (r != Relation.CELL_OUTSIDE_QUERY) return true;
        }
      }
      return queryRelation == QueryRelation.INTERSECTS ? false : true;
    }
  }
}
