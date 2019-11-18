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
import org.apache.lucene.geo.Component2D;
import org.apache.lucene.geo.ShapeTestUtil;
import org.apache.lucene.geo.XYLine;
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
      for (int i = 0, j = 1; j < line.numPoints(); ++i, ++j) {
        ShapeField.DecodedTriangle decoded = encoder.encodeDecodeTriangle(line.getX(i), line.getY(i), true, line.getX(j), line.getY(j), true, line.getX(i), line.getY(i), true);
        if (queryRelation == QueryRelation.WITHIN) {
          if (rectangle2D.containsTriangle(decoded.aX, decoded.aY, decoded.bX, decoded.bY, decoded.cX, decoded.cY) == false) {
            return false;
          }
        } else {
          if (rectangle2D.intersectsTriangle(decoded.aX, decoded.aY, decoded.bX, decoded.bY, decoded.cX, decoded.cY) == true) {
            return queryRelation == QueryRelation.INTERSECTS;
          }
        }
      }
      return queryRelation != QueryRelation.INTERSECTS;
    }

    @Override
    public boolean testComponentQuery(Component2D query, Object shape) {
      XYLine line = (XYLine) shape;
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
  }
}
