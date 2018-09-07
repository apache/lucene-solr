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
import org.apache.lucene.geo.Polygon;
import org.apache.lucene.geo.Polygon2D;
import org.apache.lucene.geo.Tessellator;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.util.LuceneTestCase;

/** random bounding box and polygon query tests for random indexed {@link Polygon} types */
@LuceneTestCase.BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 6-Sep-2018
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

  protected class PolygonValidator extends Validator {
    @Override
    public boolean testBBoxQuery(double minLat, double maxLat, double minLon, double maxLon, Object shape) {
      Polygon p = (Polygon)shape;
      if (queryRelation == QueryRelation.WITHIN) {
        // within: bounding box of shape should be within query box
        return minLat <= quantizeLat(p.minLat) && maxLat >= quantizeLat(p.maxLat)
            && minLon <= quantizeLon(p.minLon) && maxLon >= quantizeLon(p.maxLon);
      }

      Polygon2D poly = Polygon2D.create(quantizePolygon(p));
      Relation r = poly.relate(minLat, maxLat, minLon, maxLon);
      if (queryRelation == QueryRelation.DISJOINT) {
        return r == Relation.CELL_OUTSIDE_QUERY;
      }
      return r != Relation.CELL_OUTSIDE_QUERY;
    }

    @Override
    public boolean testPolygonQuery(Polygon2D query, Object shape) {
      List<Tessellator.Triangle> tessellation = Tessellator.tessellate((Polygon) shape);
      for (Tessellator.Triangle t : tessellation) {
        // we quantize the triangle for consistency with the index
        Relation r = query.relateTriangle(quantizeLon(t.getLon(0)), quantizeLat(t.getLat(0)),
            quantizeLon(t.getLon(1)), quantizeLat(t.getLat(1)),
            quantizeLon(t.getLon(2)), quantizeLat(t.getLat(2)));
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
