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

import org.apache.lucene.document.LatLonShape.QueryRelation;
import org.apache.lucene.geo.Line2D;
import org.apache.lucene.geo.Polygon;
import org.apache.lucene.geo.Polygon2D;
import org.apache.lucene.geo.Tessellator;
import org.apache.lucene.index.PointValues;

/** random bounding box and polygon query tests for random indexed arrays of {@link Polygon} types */
public class TestLatLonMultiPolygonShapeQueries extends BaseLatLonShapeTestCase {

  protected final MultiPolygonValidator VALIDATOR = new MultiPolygonValidator();
  protected final TestLatLonPolygonShapeQueries.PolygonValidator POLYGONVALIDATOR = new TestLatLonPolygonShapeQueries.PolygonValidator();

  @Override
  protected ShapeType getShapeType() {
    return ShapeType.POLYGON;
  }

  @Override
  protected Polygon[] nextShape() {

    int n = random().nextInt(4) + 1;
    Polygon[] polygons = new Polygon[n];
    for (int i =0; i < n; i++) {
      int  repetitions =0;
      while (true) {
        // if we can't tessellate; then random polygon generator created a malformed shape
        Polygon p = (Polygon) getShapeType().nextShape();
        try {
          //polygons are disjoint so CONTAINS works. Note that if we intersect
          //any shape then contains return false.
          if (isDisjoint(polygons, Tessellator.tessellate(p), i)) {
            polygons[i] = p;
            break;
          }
          repetitions++;
          if (repetitions > 1000) {
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

  private boolean isDisjoint(Polygon[] polygons, List<Tessellator.Triangle> triangles, int totalPolygons) {
    if (totalPolygons == 0) {
      return true;
    }
    Polygon[] currentPolygons = new Polygon[totalPolygons];
    for (int i =0; i < totalPolygons; i++) {
      currentPolygons[i] = polygons[i];
    }
    Polygon2D impl = Polygon2D.create(currentPolygons);
    for (Tessellator.Triangle triangle : triangles) {
      if (impl.relateTriangle(triangle.getLon(0), triangle.getLat(0),
          triangle.getLon(1), triangle.getLat(1),
          triangle.getLon(2), triangle.getLat(2)) != PointValues.Relation.CELL_OUTSIDE_QUERY) {
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
  protected Validator getValidator(QueryRelation relation) {
    VALIDATOR.setRelation(relation);
    POLYGONVALIDATOR.setRelation(relation);
    return VALIDATOR;
  }

  protected class MultiPolygonValidator extends Validator {
    @Override
    public boolean testBBoxQuery(double minLat, double maxLat, double minLon, double maxLon, Object shape) {
      Polygon[] polygons = (Polygon[])shape;
      for (Polygon p : polygons) {
        boolean b = POLYGONVALIDATOR.testBBoxQuery(minLat, maxLat, minLon, maxLon, p);
        if (b == true && queryRelation == QueryRelation.INTERSECTS) {
          return true;
        } else if (b == true && queryRelation == QueryRelation.CONTAINS) {
          return true;
        } else if (b == false && queryRelation != QueryRelation.INTERSECTS && queryRelation != QueryRelation.CONTAINS) {
          return false;
        }
      }
      return (queryRelation != QueryRelation.INTERSECTS && queryRelation != QueryRelation.CONTAINS);
    }

    @Override
    public boolean testLineQuery(Line2D query, Object shape) {
      Polygon[] polygons = (Polygon[])shape;
      for (Polygon p : polygons) {
        boolean b = POLYGONVALIDATOR.testLineQuery(query, p);
        if (b == true && queryRelation == QueryRelation.INTERSECTS) {
          return true;
        } else if (b == true && queryRelation == QueryRelation.CONTAINS) {
          return true;
        } else if (b == false && queryRelation != QueryRelation.INTERSECTS && queryRelation != QueryRelation.CONTAINS) {
          return false;
        }
      }
      return (queryRelation != QueryRelation.INTERSECTS && queryRelation != QueryRelation.CONTAINS);
    }

    @Override
    public boolean testPolygonQuery(Polygon2D query, Object shape) {
      Polygon[] polygons = (Polygon[])shape;
      for (Polygon p : polygons) {
        boolean b = POLYGONVALIDATOR.testPolygonQuery(query, p);
        if (b == true && queryRelation == QueryRelation.INTERSECTS) {
          return true;
        } else if (b == true && queryRelation == QueryRelation.CONTAINS) {
          return true;
        } else if (b == false && queryRelation != QueryRelation.INTERSECTS && queryRelation != QueryRelation.CONTAINS) {
          return false;
        }
      }
      return (queryRelation != QueryRelation.INTERSECTS && queryRelation != QueryRelation.CONTAINS);
    }
  }

  @Slow
  @Nightly
  @Override
  public void testRandomBig() throws Exception {
    doTestRandom(10000);
  }
}
