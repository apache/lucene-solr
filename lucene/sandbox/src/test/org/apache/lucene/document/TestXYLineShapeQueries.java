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
import org.apache.lucene.geo.EdgeTree;
import org.apache.lucene.geo.Line2D;
import org.apache.lucene.geo.ShapeTestUtil;
import org.apache.lucene.geo.XYLine;
import org.apache.lucene.geo.XYPolygon2D;
import org.apache.lucene.geo.XYRectangle;
import org.apache.lucene.geo.XYRectangle2D;
import org.apache.lucene.index.PointValues.Relation;

/** random cartesian bounding box, line, and polygon query tests for random generated cartesian {@link XYLine} types */
public class TestXYLineShapeQueries extends BaseXYShapeTestCase {

  @Override
  protected ShapeType getShapeType() {
    return ShapeType.LINE;
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
        XYLine l = (XYLine) (shapes[i]);
        if (random().nextBoolean() && l != null) {
          int v = random().nextInt(l.numPoints() - 1);
          x[j] = (float)l.getX(v);
          y[j] = (float)l.getY(v);
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
  protected Field[] createIndexableFields(String field, Object line) {
    return XYShape.createIndexableFields(field, (XYLine)line);
  }

  @Override
  protected Validator getValidator() {
    return new LineValidator(this.ENCODER);
  }

  protected static class LineValidator extends Validator {
    protected LineValidator(Encoder encoder) {
      super(encoder);
    }

    @Override
    public boolean testBBoxQuery(double minY, double maxY, double minX, double maxX, Object shape) {
      XYLine line = (XYLine)shape;
      XYRectangle2D rectangle2D = XYRectangle2D.create(new XYRectangle(minX, maxX, minY, maxY));
      EdgeTree.WithinRelation withinRelation = EdgeTree.WithinRelation.DISJOINT;
      for (int i = 0, j = 1; j < line.numPoints(); ++i, ++j) {
        double[] qTriangle = encoder.quantizeTriangle(line.getX(i), line.getY(i), true, line.getX(j), line.getY(j), true, line.getX(i), line.getY(i), true);
        if (queryRelation == QueryRelation.CONTAINS) {
          EdgeTree.WithinRelation relation = rectangle2D.withinTriangle((float) qTriangle[1], (float) qTriangle[0], true,
                                                                        (float) qTriangle[3], (float) qTriangle[2], true,
                                                                        (float) qTriangle[5], (float) qTriangle[4], true);
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
    public boolean testLineQuery(Line2D line2d, Object shape) {
      if (queryRelation == QueryRelation.CONTAINS) {
        return testWithinLine(line2d, (XYLine) shape);
      }
      return testLine(line2d, (XYLine) shape);
    }

    @Override
    public boolean testPolygonQuery(Object poly2d, Object shape) {
      if (queryRelation == QueryRelation.CONTAINS) {
        return testWithinLine((XYPolygon2D) poly2d, (XYLine) shape);
      }
      return testLine((XYPolygon2D)poly2d, (XYLine) shape);
    }

    private boolean testLine(EdgeTree queryPoly, XYLine line) {

      for (int i = 0, j = 1; j < line.numPoints(); ++i, ++j) {
        double[] qTriangle = encoder.quantizeTriangle(line.getX(i), line.getY(i), true, line.getX(j), line.getY(j), true, line.getX(i), line.getY(i), true);
        Relation r = queryPoly.relateTriangle(qTriangle[1], qTriangle[0], qTriangle[3], qTriangle[2], qTriangle[5], qTriangle[4]);
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

    private boolean testWithinLine(EdgeTree tree, XYLine line) {
      EdgeTree.WithinRelation answer = EdgeTree.WithinRelation.DISJOINT;
      for (int i = 0, j = 1; j < line.numPoints(); ++i, ++j) {
        double[] qTriangle = encoder.quantizeTriangle(line.getX(i), line.getY(i), true, line.getX(j), line.getY(j), true, line.getX(i), line.getY(i), true);
        EdgeTree.WithinRelation relation = tree.withinTriangle(qTriangle[1], qTriangle[0], true, qTriangle[3], qTriangle[2], true, qTriangle[5], qTriangle[4], true);
        if (relation == EdgeTree.WithinRelation.NOTWITHIN) {
          return false;
        } else if (relation == EdgeTree.WithinRelation.CANDIDATE) {
          answer = EdgeTree.WithinRelation.CANDIDATE;
        }
      }
      return answer == EdgeTree.WithinRelation.CANDIDATE;
    }
  }
}
