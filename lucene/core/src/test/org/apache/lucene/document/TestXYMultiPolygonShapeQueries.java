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
import org.apache.lucene.geo.Tessellator;
import org.apache.lucene.geo.XYPolygon;
import org.apache.lucene.geo.XYRectangle;
import org.apache.lucene.geo.XYGeometry;
import org.apache.lucene.util.LuceneTestCase;

/** random cartesian bounding box, line, and polygon query tests for random indexed arrays of cartesian {@link XYPolygon} types */
@LuceneTestCase.SuppressCodecs("SimpleText")
public class TestXYMultiPolygonShapeQueries extends BaseXYShapeTestCase {
  @Override
  protected ShapeType getShapeType() {
    return ShapeType.POLYGON;
  }

  @Override
  protected XYPolygon[] nextShape() {
    int n = random().nextInt(4) + 1;
    XYPolygon[] polygons = new XYPolygon[n];
    for (int i =0; i < n; i++) {
      int  repetitions =0;
      while (true) {
        // if we can't tessellate; then random polygon generator created a malformed shape
        XYPolygon p = (XYPolygon) getShapeType().nextShape();
        try {
          Tessellator.tessellate(p);
          //polygons are disjoint so CONTAINS works. Note that if we intersect
          //any shape then contains return false.
          if (isDisjoint(polygons, p, i)) {
            polygons[i] = p;
            break;
          }
          repetitions++;
          if (repetitions > 2) {
            //try again
            return nextShape();
          }
        } catch (IllegalArgumentException e) {
          continue;
        }
      }
    }
    return polygons;
  }

  private boolean isDisjoint(XYPolygon[] polygons, XYPolygon check, int totalPolygons) {
    // we use bounding boxes so we do not get polygons with shared points.
    for (XYPolygon polygon : polygons) {
      if (polygon != null) {
        if (getEncoder().quantizeY(polygon.minY) > getEncoder().quantizeY(check.maxY)
            || getEncoder().quantizeY(polygon.maxY) < getEncoder().quantizeY(check.minY)
            || getEncoder().quantizeX(polygon.minX) > getEncoder().quantizeX(check.maxX)
            || getEncoder().quantizeX(polygon.maxX) < getEncoder().quantizeX(check.minX)) {
          continue;
        }
        return false;
      }
    }
    return true;
  }

  @Override
  protected Field[] createIndexableFields(String name, Object o) {
    XYPolygon[] polygons = (XYPolygon[]) o;
    List<Field> allFields = new ArrayList<>();
    for (XYPolygon polygon : polygons) {
      Field[] fields = XYShape.createIndexableFields(name, polygon);
      for (Field field : fields) {
        allFields.add(field);
      }
    }
    return allFields.toArray(new Field[allFields.size()]);
  }

  @Override
  protected Validator getValidator() {
    return new MultiPolygonValidator(ENCODER);
  }

  protected class MultiPolygonValidator extends Validator {
    TestXYPolygonShapeQueries.PolygonValidator POLYGONVALIDATOR;
    MultiPolygonValidator(Encoder encoder) {
      super(encoder);
      POLYGONVALIDATOR = new TestXYPolygonShapeQueries.PolygonValidator(encoder);
    }

    @Override
    public Validator setRelation(QueryRelation relation) {
      super.setRelation(relation);
      POLYGONVALIDATOR.queryRelation = relation;
      return this;
    }

    @Override
    public boolean testBBoxQuery(double minY, double maxY, double minX, double maxX, Object shape) {
      Component2D rectangle2D = XYGeometry.create(new XYRectangle((float) minX, (float) maxX, (float) minY, (float) maxY));
      return testComponentQuery(rectangle2D, shape);
    }

    @Override
    public boolean testComponentQuery(Component2D query, Object shape) {
      XYPolygon[] polygons = (XYPolygon[])shape;
      for (XYPolygon p : polygons) {
        boolean b = POLYGONVALIDATOR.testComponentQuery(query, p);
        if (b == true && queryRelation == QueryRelation.INTERSECTS) {
          return true;
        } else if (b == true && queryRelation == QueryRelation.CONTAINS) {
          return true;
        } else if (b == false && queryRelation == QueryRelation.DISJOINT) {
          return false;
        } else if (b == false && queryRelation == QueryRelation.WITHIN) {
          return false;
        }
      }
      return queryRelation != QueryRelation.INTERSECTS && queryRelation != QueryRelation.CONTAINS;
    }
  }

  @Slow
  @Nightly
  @Override
  public void testRandomBig() throws Exception {
    doTestRandom(10000);
  }
}
