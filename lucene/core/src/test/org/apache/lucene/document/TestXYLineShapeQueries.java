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

import java.util.Random;

import org.apache.lucene.document.ShapeField.QueryRelation;
import org.apache.lucene.geo.Component2D;
import org.apache.lucene.geo.ShapeTestUtil;
import org.apache.lucene.geo.XYGeometry;
import org.apache.lucene.geo.XYLine;
import org.apache.lucene.geo.XYRectangle;
import org.apache.lucene.index.PointValues.Relation;

/** random cartesian bounding box, line, and polygon query tests for random generated cartesian {@link XYLine} types */
public class TestXYLineShapeQueries extends BaseXYShapeTestCase {

  @Override
  protected ShapeType getShapeType() {
    return ShapeType.LINE;
  }

  @Override
  protected XYLine randomQueryLine(Object... shapes) {
    Random random = random();
    if (random.nextInt(100) == 42) {
      // we want to ensure some cross, so randomly generate lines that share vertices with the indexed point set
      int maxBound = (int)Math.floor(shapes.length * 0.1d);
      if (maxBound < 2) {
        maxBound = shapes.length;
      }
      float[] x = new float[RandomNumbers.randomIntBetween(random, 2, maxBound)];
      float[] y = new float[x.length];
      for (int i = 0, j = 0; j < x.length && i < shapes.length; ++i, ++j) {
        XYLine l = (XYLine) (shapes[i]);
        if (random.nextBoolean() && l != null) {
          int v = random.nextInt(l.numPoints() - 1);
          x[j] = l.getX(v);
          y[j] = l.getY(v);
        } else {
          x[j] = ShapeTestUtil.nextFloat(random);
          y[j] = ShapeTestUtil.nextFloat(random);
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
      Component2D rectangle2D = XYGeometry.create(new XYRectangle((float) minX, (float) maxX, (float) minY, (float) maxY));
      return testComponentQuery(rectangle2D, shape);
    }

    @Override
    public boolean testComponentQuery(Component2D query, Object shape) {
      XYLine line = (XYLine) shape;
      if (queryRelation == QueryRelation.CONTAINS) {
        return testWithinLine(query, (XYLine) shape);
      }
      for (int i = 0, j = 1; j < line.numPoints(); ++i, ++j) {
        double[] qTriangle = encoder.quantizeTriangle(line.getX(i), line.getY(i), true, line.getX(j), line.getY(j), true, line.getX(i), line.getY(i), true);
        Relation r = query.relateTriangle(qTriangle[1], qTriangle[0], qTriangle[3], qTriangle[2], qTriangle[5], qTriangle[4]);
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

    private boolean testWithinLine(Component2D tree, XYLine line) {
      Component2D.WithinRelation answer = Component2D.WithinRelation.DISJOINT;
      for (int i = 0, j = 1; j < line.numPoints(); ++i, ++j) {
        double[] qTriangle = encoder.quantizeTriangle(line.getX(i), line.getY(i), true, line.getX(j), line.getY(j), true, line.getX(i), line.getY(i), true);
        Component2D.WithinRelation relation = tree.withinTriangle(qTriangle[1], qTriangle[0], true, qTriangle[3], qTriangle[2], true, qTriangle[5], qTriangle[4], true);
        if (relation == Component2D.WithinRelation.NOTWITHIN) {
          return false;
        } else if (relation == Component2D.WithinRelation.CANDIDATE) {
          answer = Component2D.WithinRelation.CANDIDATE;
        }
      }
      return answer == Component2D.WithinRelation.CANDIDATE;
    }
  }
}
