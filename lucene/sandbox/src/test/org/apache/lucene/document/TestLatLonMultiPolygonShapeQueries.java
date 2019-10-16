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
import org.apache.lucene.geo.Polygon;
import org.apache.lucene.geo.Tessellator;

/** random bounding box, line, and polygon query tests for random indexed arrays of {@link Polygon} types */
public class TestLatLonMultiPolygonShapeQueries extends BaseLatLonShapeTestCase {

  @Override
  protected ShapeType getShapeType() {
    return ShapeType.POLYGON;
  }

  @Override
  protected Polygon[] nextShape() {

    int n = random().nextInt(4) + 1;
    Polygon[] polygons = new Polygon[n];
    for (int i =0; i < n; i++) {
      while (true) {
        // if we can't tessellate; then random polygon generator created a malformed shape
        Polygon p = (Polygon) getShapeType().nextShape();
        try {
          Tessellator.tessellate(p);
          polygons[i] = p;
          break;
        } catch (IllegalArgumentException e) {
          continue;
        }
      }
    }
    return polygons;
  }

  @Override
  protected Field[] createIndexableFields(String name, Object o) {
    Polygon[] polygons = (Polygon[]) o;
    List<Field> allFields = new ArrayList<>();
    for (Polygon polygon : polygons) {
      Field[] fields = LatLonShape.createIndexableFields(name, polygon);
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
    TestLatLonPolygonShapeQueries.PolygonValidator POLYGONVALIDATOR;
    MultiPolygonValidator(Encoder encoder) {
      super(encoder);
      POLYGONVALIDATOR = new TestLatLonPolygonShapeQueries.PolygonValidator(encoder);
    }

    @Override
    public Validator setRelation(QueryRelation relation) {
      super.setRelation(relation);
      POLYGONVALIDATOR.queryRelation = relation;
      return this;
    }

    @Override
    public boolean testBBoxQuery(double minLat, double maxLat, double minLon, double maxLon, Object shape) {
      Polygon[] polygons = (Polygon[])shape;
      for (Polygon p : polygons) {
        boolean b = POLYGONVALIDATOR.testBBoxQuery(minLat, maxLat, minLon, maxLon, p);
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
      Polygon[] polygons = (Polygon[])shape;
      for (Polygon p : polygons) {
        boolean b = POLYGONVALIDATOR.testComponentQuery(query, p);
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
