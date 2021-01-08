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

import static org.apache.lucene.geo.XYEncodingUtils.decode;
import static org.apache.lucene.geo.XYEncodingUtils.encode;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import java.util.Arrays;
import java.util.Random;
import org.apache.lucene.document.ShapeField.QueryRelation;
import org.apache.lucene.geo.Component2D;
import org.apache.lucene.geo.ShapeTestUtil;
import org.apache.lucene.geo.Tessellator;
import org.apache.lucene.geo.XYCircle;
import org.apache.lucene.geo.XYGeometry;
import org.apache.lucene.geo.XYLine;
import org.apache.lucene.geo.XYPoint;
import org.apache.lucene.geo.XYPolygon;
import org.apache.lucene.geo.XYRectangle;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.TestUtil;

/** Base test case for testing indexing and search functionality of cartesian geometry * */
public abstract class BaseXYShapeTestCase extends BaseSpatialTestCase {
  protected abstract ShapeType getShapeType();

  protected Object nextShape() {
    return getShapeType().nextShape();
  }

  /** factory method to create a new bounding box query */
  @Override
  protected Query newRectQuery(
          String field,
          QueryRelation queryRelation,
          double minX,
          double maxX,
          double minY,
          double maxY) {
    return XYShape.newBoxQuery(
            field, queryRelation, (float) minX, (float) maxX, (float) minY, (float) maxY);
  }

  /** factory method to create a new line query */
  @Override
  protected Query newLineQuery(String field, QueryRelation queryRelation, Object... lines) {
    return XYShape.newLineQuery(field, queryRelation, Arrays.stream(lines).toArray(XYLine[]::new));
  }

  /** factory method to create a new polygon query */
  @Override
  protected Query newPolygonQuery(String field, QueryRelation queryRelation, Object... polygons) {
    return XYShape.newPolygonQuery(
            field, queryRelation, Arrays.stream(polygons).toArray(XYPolygon[]::new));
  }

  @Override
  protected Query newPointsQuery(String field, QueryRelation queryRelation, Object... points) {
    return XYShape.newPointQuery(
            field, queryRelation, Arrays.stream(points).toArray(float[][]::new));
  }

  @Override
  protected Query newDistanceQuery(String field, QueryRelation queryRelation, Object circle) {
    return XYShape.newDistanceQuery(field, queryRelation, (XYCircle) circle);
  }

  @Override
  protected Component2D toPoint2D(Object... points) {
    float[][] p = Arrays.stream(points).toArray(float[][]::new);
    XYPoint[] pointArray = new XYPoint[points.length];
    for (int i = 0; i < points.length; i++) {
      pointArray[i] = new XYPoint(p[i][0], p[i][1]);
    }
    return XYGeometry.create(pointArray);
  }

  @Override
  protected Component2D toLine2D(Object... lines) {
    return XYGeometry.create(Arrays.stream(lines).toArray(XYLine[]::new));
  }

  @Override
  protected Component2D toPolygon2D(Object... polygons) {
    return XYGeometry.create(Arrays.stream(polygons).toArray(XYPolygon[]::new));
  }

  @Override
  protected Component2D toRectangle2D(double minX, double maxX, double minY, double maxY) {
    return XYGeometry.create(
            new XYRectangle((float) minX, (float) maxX, (float) minY, (float) maxY));
  }

  @Override
  protected Component2D toCircle2D(Object circle) {
    return XYGeometry.create((XYCircle) circle);
  }

  @Override
  public XYRectangle randomQueryBox() {
    return ShapeTestUtil.nextBox(random());
  }

  @Override
  protected double rectMinX(Object rect) {
    return ((XYRectangle) rect).minX;
  }

  @Override
  protected double rectMaxX(Object rect) {
    return ((XYRectangle) rect).maxX;
  }

  @Override
  protected double rectMinY(Object rect) {
    return ((XYRectangle) rect).minY;
  }

  @Override
  protected double rectMaxY(Object rect) {
    return ((XYRectangle) rect).maxY;
  }

  @Override
  protected boolean rectCrossesDateline(Object rect) {
    return false;
  }

  /** use {@link ShapeTestUtil#nextPolygon()} to create a random line */
  @Override
  public XYLine nextLine() {
    return ShapeTestUtil.nextLine();
  }

  @Override
  protected XYPolygon nextPolygon() {
    return ShapeTestUtil.nextPolygon();
  }

  @Override
  protected Object[] nextPoints() {
    Random random = random();
    int numPoints = TestUtil.nextInt(random, 1, 20);
    float[][] points = new float[numPoints][2];
    for (int i = 0; i < numPoints; i++) {
      points[i][0] = ShapeTestUtil.nextFloat(random);
      points[i][1] = ShapeTestUtil.nextFloat(random);
    }
    return points;
  }

  @Override
  protected Object nextCircle() {
    return ShapeTestUtil.nextCircle();
  }

  @Override
  protected Encoder getEncoder() {
    return new Encoder() {
      @Override
      double decodeX(int encoded) {
        return decode(encoded);
      }

      @Override
      double decodeY(int encoded) {
        return decode(encoded);
      }

      @Override
      double quantizeX(double raw) {
        return decode(encode((float) raw));
      }

      @Override
      double quantizeXCeil(double raw) {
        return decode(encode((float) raw));
      }

      @Override
      double quantizeY(double raw) {
        return decode(encode((float) raw));
      }

      @Override
      double quantizeYCeil(double raw) {
        return decode(encode((float) raw));
      }
    };
  }

  /** internal shape type for testing different shape types */
  protected enum ShapeType {
    POINT() {
      public XYPoint nextShape() {
        return ShapeTestUtil.nextPoint();
      }
    },
    LINE() {
      public XYLine nextShape() {
        return ShapeTestUtil.nextLine();
      }
    },
    POLYGON() {
      public XYPolygon nextShape() {
        while (true) {
          XYPolygon p = ShapeTestUtil.nextPolygon();
          try {
            Tessellator.tessellate(p);
            return p;
          } catch (IllegalArgumentException e) {
            // if we can't tessellate; then random polygon generator created a malformed shape
          }
        }
      }
    },
    MIXED() {
      public Object nextShape() {
        return RandomPicks.randomFrom(random(), subList).nextShape();
      }
    };

    static ShapeType[] subList;

    static {
      subList = new ShapeType[] {POINT, LINE, POLYGON};
    }

    public abstract Object nextShape();
  }
}
