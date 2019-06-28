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

import java.util.List;

import org.apache.lucene.document.LatLonShape.QueryRelation;
import org.apache.lucene.geo.EdgeTree;
import org.apache.lucene.geo.Line2D;
import org.apache.lucene.geo.Polygon;
import org.apache.lucene.geo.Polygon2D;
import org.apache.lucene.geo.Rectangle;
import org.apache.lucene.geo.Rectangle2D;
import org.apache.lucene.geo.Tessellator;
import org.apache.lucene.index.PointValues.Relation;

/** random bounding box and polygon query tests for random indexed {@link Polygon} types */
public class TestLatLonPolygonShapeQueries extends BaseLatLonShapeTestCase {

  protected final PolygonValidator VALIDATOR = new PolygonValidator();

  @Override
  protected ShapeType getShapeType() {
    return ShapeType.POLYGON;
  }

  @Override
  protected Polygon nextShape() {
    Polygon p;
    while (true) {
      // if we can't tessellate; then random polygon generator created a malformed shape
      p = (Polygon)getShapeType().nextShape();
      try {
        Tessellator.tessellate(p);
        return p;
      } catch (IllegalArgumentException e) {
        continue;
      }
    }
  }

  @Override
  protected Field[] createIndexableFields(String field, Object polygon) {
    return LatLonShape.createIndexableFields(field, (Polygon)polygon);
  }

  @Override
  protected Validator getValidator(QueryRelation relation) {
    VALIDATOR.setRelation(relation);
    return VALIDATOR;
  }

  protected static class PolygonValidator extends Validator {
    @Override
    public boolean testBBoxQuery(double minLat, double maxLat, double minLon, double maxLon, Object shape) {
      Polygon p = (Polygon)shape;
      Rectangle2D rectangle2D = Rectangle2D.create(new Rectangle(minLat, maxLat, minLon, maxLon));
      List<Tessellator.Triangle> tessellation = Tessellator.tessellate(p);
      for (Tessellator.Triangle t : tessellation) {
        int[] decoded = encodeDecodeTriangle(t.getLon(0), t.getLat(0), t.getLon(1), t.getLat(1), t.getLon(2), t.getLat(2));
        if (queryRelation == QueryRelation.WITHIN) {
          if (rectangle2D.containsTriangle(decoded[1], decoded[0], decoded[3], decoded[2], decoded[5], decoded[4]) == false) {
            return false;
          }
        } else {
          if (rectangle2D.intersectsTriangle(decoded[1], decoded[0], decoded[3], decoded[2], decoded[5], decoded[4]) == true) {
            return queryRelation == QueryRelation.INTERSECTS;
          }
        }
      }
      return queryRelation != QueryRelation.INTERSECTS;
    }

    @Override
    public boolean testLineQuery(Line2D query, Object shape) {
      return testPolygon(query, (Polygon) shape);
    }

    @Override
    public boolean testPolygonQuery(Polygon2D query, Object shape) {
      return testPolygon(query, (Polygon) shape);
    }

    private boolean testPolygon(EdgeTree tree, Polygon shape) {
      List<Tessellator.Triangle> tessellation = Tessellator.tessellate(shape);
      for (Tessellator.Triangle t : tessellation) {
        double[] qTriangle = quantizeTriangle(t.getLon(0), t.getLat(0), t.getLon(1), t.getLat(1), t.getLon(2), t.getLat(2));
        Relation r = tree.relateTriangle(qTriangle[1], qTriangle[0], qTriangle[3], qTriangle[2], qTriangle[5], qTriangle[4]);
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

  @Nightly
  @Override
  public void testRandomBig() throws Exception {
    doTestRandom(25000);
  }
}
