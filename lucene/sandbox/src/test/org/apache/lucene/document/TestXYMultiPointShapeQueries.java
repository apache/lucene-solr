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

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.document.ShapeField.QueryRelation;
import org.apache.lucene.geo.Component2D;

/** random cartesian bounding box, line, and polygon query tests for random indexed arrays of {@code x, y} points */
public class TestXYMultiPointShapeQueries extends BaseXYShapeTestCase {
  @Override
  protected ShapeType getShapeType() {
    return ShapeType.POINT;
  }

  @Override
  protected Point[] nextShape() {
    int n = random().nextInt(4) + 1;
    Point[] points = new Point[n];
    for (int i =0; i < n; i++) {
      points[i] = (Point)ShapeType.POINT.nextShape();
    }
    return points;
  }

  @Override
  protected Field[] createIndexableFields(String name, Object o) {
    Point[] points = (Point[]) o;
    List<Field> allFields = new ArrayList<>();
    for (Point point : points) {
      Field[] fields = XYShape.createIndexableFields(name, point.x, point.y);
      for (Field field : fields) {
        allFields.add(field);
      }
    }
    return allFields.toArray(new Field[allFields.size()]);
  }

  @Override
  public Validator getValidator() {
    return new MultiPointValidator(ENCODER);
  }

  protected class MultiPointValidator extends Validator {
    TestXYPointShapeQueries.PointValidator POINTVALIDATOR;
    MultiPointValidator(Encoder encoder) {
      super(encoder);
      POINTVALIDATOR = new TestXYPointShapeQueries.PointValidator(encoder);
    }

    @Override
    public Validator setRelation(QueryRelation relation) {
      super.setRelation(relation);
      POINTVALIDATOR.queryRelation = relation;
      return this;
    }

    @Override
    public boolean testBBoxQuery(double minLat, double maxLat, double minLon, double maxLon, Object shape) {
      Point[] points = (Point[]) shape;
      for (Point p : points) {
        boolean b = POINTVALIDATOR.testBBoxQuery(minLat, maxLat, minLon, maxLon, p);
        if (b == true && queryRelation == QueryRelation.INTERSECTS) {
          return true;
        } else if (b == false && queryRelation == QueryRelation.DISJOINT) {
          return false;
        } else if (b == false && queryRelation == QueryRelation.WITHIN) {
          return false;
        }
      }
      return queryRelation != QueryRelation.INTERSECTS;
    }

    @Override
    public boolean testComponentQuery(Component2D query, Object shape) {
      Point[] points = (Point[]) shape;
      for (Point p : points) {
        boolean b = POINTVALIDATOR.testComponentQuery(query, p);
        if (b == true && queryRelation == QueryRelation.INTERSECTS) {
          return true;
        } else if (b == false && queryRelation == QueryRelation.DISJOINT) {
          return false;
        } else if (b == false && queryRelation == QueryRelation.WITHIN) {
          return false;
        }
      }
      return queryRelation != QueryRelation.INTERSECTS;
    }
  }

  @Slow
  @Nightly
  @Override
  public void testRandomBig() throws Exception {
    doTestRandom(10000);
  }
}
