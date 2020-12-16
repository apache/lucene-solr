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
import org.apache.lucene.geo.Point;

/** random bounding box, line, and polygon query tests for random generated {@code latitude, longitude} points */
public class TestLatLonPointShapeQueries extends BaseLatLonShapeTestCase {

  @Override
  protected ShapeType getShapeType() {
    return ShapeType.POINT;
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
        Point p = (Point) (shapes[i]);
        if (random().nextBoolean() && p != null) {
          lats[j] = p.getLat();
          lons[j] = p.getLon();
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
  protected Field[] createIndexableFields(String field, Object point) {
    Point p = (Point)point;
    return LatLonShape.createIndexableFields(field, p.getLat(), p.getLon());
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
    public boolean testComponentQuery(Component2D query, Object shape) {
      Point p = (Point) shape;
      if (queryRelation == QueryRelation.CONTAINS) {
        return testWithinQuery(query, LatLonShape.createIndexableFields("dummy", p.getLat(), p.getLon())) == Component2D.WithinRelation.CANDIDATE;
      }
      return testComponentQuery(query, LatLonShape.createIndexableFields("dummy", p.getLat(), p.getLon()));
    }
  }
}
