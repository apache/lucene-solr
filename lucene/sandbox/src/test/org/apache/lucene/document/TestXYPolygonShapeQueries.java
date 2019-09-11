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

import org.apache.lucene.document.ShapeField.QueryRelation;
import org.apache.lucene.geo.EdgeTree;
import org.apache.lucene.geo.Line2D;
import org.apache.lucene.geo.Tessellator;
import org.apache.lucene.geo.XYPolygon;
import org.apache.lucene.geo.XYPolygon2D;
import org.apache.lucene.geo.XYRectangle;
import org.apache.lucene.geo.XYRectangle2D;
import org.apache.lucene.index.PointValues.Relation;

/** random cartesian bounding box, line, and polygon query tests for random indexed {@link XYPolygon} types */
public class TestXYPolygonShapeQueries extends BaseXYShapeTestCase {

  @Override
  protected ShapeType getShapeType() {
    return ShapeType.POLYGON;
  }

  @Override
  protected XYPolygon nextShape() {
    XYPolygon p;
    while (true) {
      // if we can't tessellate; then random polygon generator created a malformed shape
      p = (XYPolygon)getShapeType().nextShape();
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
    return XYShape.createIndexableFields(field, (XYPolygon)polygon);
  }

  @Override
  protected Validator getValidator() {
    return new PolygonValidator(this.ENCODER);
  }

  protected static class PolygonValidator extends Validator {
    protected PolygonValidator(Encoder encoder) {
      super(encoder);
    }

    @Override
    public boolean testBBoxQuery(double minY, double maxY, double minX, double maxX, Object shape) {
      XYPolygon p = (XYPolygon)shape;
      XYRectangle2D rectangle2D = XYRectangle2D.create(new XYRectangle(minX, maxX, minY, maxY));
      List<Tessellator.Triangle> tessellation = Tessellator.tessellate(p);
      EdgeTree.WithinRelation withinRelation = EdgeTree.WithinRelation.DISJOINT;
      for (Tessellator.Triangle t : tessellation) {
        double[] qTriangle = encoder.quantizeTriangle(t.getX(0), t.getY(0), t.isEdgefromPolygon(0),
            t.getX(1), t.getY(1), t.isEdgefromPolygon(1),
            t.getX(2), t.getY(2), t.isEdgefromPolygon(2));
        if (queryRelation == QueryRelation.CONTAINS) {
          ShapeField.DecodedTriangle decoded = encoder.encodeDecodeTriangle(t.getX(0), t.getY(0), t.isEdgefromPolygon(0),
              t.getX(1), t.getY(1), t.isEdgefromPolygon(1),
              t.getX(2), t.getY(2), t.isEdgefromPolygon(2));
          EdgeTree.WithinRelation relation = rectangle2D.withinTriangle((float) qTriangle[1], (float) qTriangle[0], decoded.ab,
                                                                        (float) qTriangle[3], (float) qTriangle[2], decoded.bc,
                                                                        (float) qTriangle[5], (float) qTriangle[4], decoded.ca);
          if (relation == EdgeTree.WithinRelation.NOTWITHIN) {
            return false;
          } else if (relation == EdgeTree.WithinRelation.CANDIDATE) {
            withinRelation = EdgeTree.WithinRelation.CANDIDATE;
          }
        } else {
          Relation r = rectangle2D.relateTriangle((float) qTriangle[1], (float) qTriangle[0], (float) qTriangle[3], (float) qTriangle[2], (float) qTriangle[5], (float) qTriangle[4]);
          if (queryRelation == QueryRelation.DISJOINT) {
            if (r != Relation.CELL_OUTSIDE_QUERY) return false;
          } else if (queryRelation == QueryRelation.WITHIN) {
            if (r != Relation.CELL_INSIDE_QUERY) return false;
          } else {
            if (r != Relation.CELL_OUTSIDE_QUERY) return true;
          }
        }
      }
      if (queryRelation == QueryRelation.CONTAINS) {
        return withinRelation == EdgeTree.WithinRelation.CANDIDATE;
      }
      return queryRelation != QueryRelation.INTERSECTS;
    }

    @Override
    public boolean testLineQuery(Line2D query, Object shape) {
      if (queryRelation == QueryRelation.CONTAINS) {
        return testWithinPolygon(query, (XYPolygon) shape);
      }
      return testPolygon(query, (XYPolygon) shape);
    }

    @Override
    public boolean testPolygonQuery(Object query, Object shape) {
      if (queryRelation == QueryRelation.CONTAINS) {
        return testWithinPolygon((XYPolygon2D) query, (XYPolygon) shape);
      }
      return testPolygon((XYPolygon2D)query, (XYPolygon) shape);
    }

    private boolean testPolygon(EdgeTree tree, XYPolygon shape) {
      List<Tessellator.Triangle> tessellation = Tessellator.tessellate(shape);
      for (Tessellator.Triangle t : tessellation) {
        double[] qTriangle = encoder.quantizeTriangle(t.getX(0), t.getY(0), t.isEdgefromPolygon(0),
                                                      t.getX(1), t.getY(1), t.isEdgefromPolygon(1),
                                                      t.getX(2), t.getY(2), t.isEdgefromPolygon(2));
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

    private boolean testWithinPolygon(EdgeTree tree, XYPolygon shape) {
      List<Tessellator.Triangle> tessellation = Tessellator.tessellate(shape);
      EdgeTree.WithinRelation answer = EdgeTree.WithinRelation.DISJOINT;
      for (Tessellator.Triangle t : tessellation) {
        ShapeField.DecodedTriangle qTriangle = encoder.encodeDecodeTriangle(t.getX(0), t.getY(0), t.isEdgefromPolygon(0),
            t.getX(1), t.getY(1), t.isEdgefromPolygon(1),
            t.getX(2), t.getY(2), t.isEdgefromPolygon(2));
        EdgeTree.WithinRelation relation = tree.withinTriangle(encoder.decodeX(qTriangle.aX), encoder.decodeY(qTriangle.aY), qTriangle.ab,
            encoder.decodeX(qTriangle.bX), encoder.decodeY(qTriangle.bY), qTriangle.bc,
            encoder.decodeX(qTriangle.cX), encoder.decodeY(qTriangle.cY), qTriangle.ca);
        if (relation == EdgeTree.WithinRelation.NOTWITHIN) {
          return false;
        } else if (relation == EdgeTree.WithinRelation.CANDIDATE) {
          answer = EdgeTree.WithinRelation.CANDIDATE;
        }
      }
      return answer == EdgeTree.WithinRelation.CANDIDATE;
    }
  }

  @Nightly
  @Override
  public void testRandomBig() throws Exception {
    doTestRandom(25000);
  }

}
