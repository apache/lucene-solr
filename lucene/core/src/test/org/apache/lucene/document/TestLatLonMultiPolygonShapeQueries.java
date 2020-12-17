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
      int repetitions = 0;
      while (true) {
        Polygon p = (Polygon) getShapeType().nextShape();
        // polygons are disjoint so CONTAINS works. Note that if we intersect
        // any shape then contains return false.
        if (isDisjoint(polygons, p)) {
          polygons[i] = p;
          break;
        }
        repetitions++;
        if (repetitions > 50) {
          // try again
          return nextShape();
        }
      }
    }
    return polygons;
  }

  private boolean isDisjoint(Polygon[] polygons, Polygon check) {
    // we use bounding boxes so we do not get intersecting polygons.
    for (Polygon polygon : polygons) {
      if (polygon != null) {
        if (getEncoder().quantizeY(polygon.minLat) > getEncoder().quantizeY(check.maxLat)
                || getEncoder().quantizeY(polygon.maxLat) < getEncoder().quantizeY(check.minLat)
                || getEncoder().quantizeX(polygon.minLon) > getEncoder().quantizeX(check.maxLon)
                || getEncoder().quantizeX(polygon.maxLon) < getEncoder().quantizeX(check.minLon)) {
          continue;
        }
        return false;
      }
    }
    return true;
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
    public boolean testComponentQuery(Component2D query, Object shape) {
      Polygon[] polygons = (Polygon[]) shape;
      if (queryRelation == QueryRelation.CONTAINS) {
        return testWithinPolygon(query, polygons);
      }
      for (Polygon p : polygons) {
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

    private boolean testWithinPolygon(Component2D query, Polygon[] polygons) {
      Component2D.WithinRelation answer = Component2D.WithinRelation.DISJOINT;
      for (Polygon p : polygons) {
        Component2D.WithinRelation relation = POLYGONVALIDATOR.testWithinPolygon(query, p);
        if (relation == Component2D.WithinRelation.NOTWITHIN) {
          return false;
        } else if (relation == Component2D.WithinRelation.CANDIDATE) {
          answer = relation;
        }
      }
      return answer == Component2D.WithinRelation.CANDIDATE;
    }
  }

  @Slow
  @Nightly
  @Override
  public void testRandomBig() throws Exception {
    doTestRandom(10000);
  }
}
