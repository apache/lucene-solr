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
import org.apache.lucene.geo.ShapeTestUtil;
import org.apache.lucene.geo.XYLine;
import org.apache.lucene.index.PointValues.Relation;

/** random cartesian bounding box, line, and polygon query tests for random generated {@code x, y} points */
public class TestXYPointShapeQueries extends BaseXYShapeTestCase {

  @Override
  protected ShapeType getShapeType() {
    return ShapeType.POINT;
  }

  @Override
  protected XYLine randomQueryLine(Object... shapes) {
    if (random().nextInt(100) == 42) {
      // we want to ensure some cross, so randomly generate lines that share vertices with the indexed point set
      int maxBound = (int)Math.floor(shapes.length * 0.1d);
      if (maxBound < 2) {
        maxBound = shapes.length;
      }
      float[] x = new float[RandomNumbers.randomIntBetween(random(), 2, maxBound)];
      float[] y = new float[x.length];
      for (int i = 0, j = 0; j < x.length && i < shapes.length; ++i, ++j) {
        Point p = (Point) (shapes[i]);
        if (random().nextBoolean() && p != null) {
          x[j] = p.x;
          y[j] = p.y;
        } else {
          x[j] = (float)ShapeTestUtil.nextDouble();
          y[j] = (float)ShapeTestUtil.nextDouble();
        }
      }
      return new XYLine(x, y);
    }
    return nextLine();
  }

  @Override
  protected Field[] createIndexableFields(String field, Object point) {
    Point p = (Point)point;
    return XYShape.createIndexableFields(field, p.x, p.y);
  }

  @Override
  protected Validator getValidator() {
    return new PointValidator(this.ENCODER);
  }

  protected static class PointValidator extends Validator {
    protected PointValidator(Encoder encoder) {
      super(encoder);
    }

    @Override
    public boolean testBBoxQuery(double minLat, double maxLat, double minLon, double maxLon, Object shape) {
      Point p = (Point)shape;
      double lat = encoder.quantizeY(p.y);
      double lon = encoder.quantizeX(p.x);
      boolean isDisjoint = lat < minLat || lat > maxLat;

      isDisjoint = isDisjoint || ((minLon > maxLon)
          ? lon < minLon && lon > maxLon
          : lon < minLon || lon > maxLon);
      if (queryRelation == QueryRelation.DISJOINT) {
        return isDisjoint;
      }
      return isDisjoint == false;
    }

    @Override
    public boolean testComponentQuery(Component2D query, Object shape) {
      Point p = (Point) shape;
      double lat = encoder.quantizeY(p.y);
      double lon = encoder.quantizeX(p.x);
      // for consistency w/ the query we test the point as a triangle
      Relation r = query.relateTriangle(lon, lat, lon, lat, lon, lat);
      if (queryRelation == QueryRelation.WITHIN) {
        return r == Relation.CELL_INSIDE_QUERY;
      } else if (queryRelation == QueryRelation.DISJOINT) {
        return r == Relation.CELL_OUTSIDE_QUERY;
      }
      return r != Relation.CELL_OUTSIDE_QUERY;
    }
  }
}
