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

import static org.apache.lucene.geo.GeoEncodingUtils.decodeLatitude;
import static org.apache.lucene.geo.GeoEncodingUtils.decodeLongitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLatitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLatitudeCeil;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLongitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLongitudeCeil;
import static org.apache.lucene.geo.GeoTestUtil.nextLatitude;
import static org.apache.lucene.geo.GeoTestUtil.nextLongitude;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import java.util.Arrays;
import org.apache.lucene.document.ShapeField.QueryRelation;
import org.apache.lucene.geo.Circle;
import org.apache.lucene.geo.Component2D;
import org.apache.lucene.geo.GeoTestUtil;
import org.apache.lucene.geo.GeoUtils;
import org.apache.lucene.geo.LatLonGeometry;
import org.apache.lucene.geo.Line;
import org.apache.lucene.geo.Point;
import org.apache.lucene.geo.Polygon;
import org.apache.lucene.geo.Rectangle;
import org.apache.lucene.geo.Tessellator;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.TestUtil;

/** Base test case for testing geospatial indexing and search functionality * */
public abstract class BaseLatLonSpatialTestCase extends BaseSpatialTestCase {

  protected abstract ShapeType getShapeType();

  protected Object nextShape() {
    return getShapeType().nextShape();
  }

  @Override
  protected Component2D toLine2D(Object... lines) {
    return LatLonGeometry.create(Arrays.stream(lines).toArray(Line[]::new));
  }

  @Override
  protected Component2D toPolygon2D(Object... polygons) {
    return LatLonGeometry.create(Arrays.stream(polygons).toArray(Polygon[]::new));
  }

  @Override
  protected Component2D toRectangle2D(double minX, double maxX, double minY, double maxY) {
    return LatLonGeometry.create(new Rectangle(minY, maxY, minX, maxX));
  }

  @Override
  protected Component2D toPoint2D(Object... points) {
    double[][] p = Arrays.stream(points).toArray(double[][]::new);
    Point[] pointArray = new Point[points.length];
    for (int i = 0; i < points.length; i++) {
      pointArray[i] = new Point(p[i][0], p[i][1]);
    }
    return LatLonGeometry.create(pointArray);
  }

  @Override
  protected Component2D toCircle2D(Object circle) {
    return LatLonGeometry.create((Circle) circle);
  }

  @Override
  protected Circle nextCircle() {
    final double radiusMeters =
        random().nextDouble() * GeoUtils.EARTH_MEAN_RADIUS_METERS * Math.PI / 2.0 + 1.0;
    return new Circle(nextLatitude(), nextLongitude(), radiusMeters);
  }

  @Override
  public Rectangle randomQueryBox() {
    return GeoTestUtil.nextBox();
  }

  @Override
  protected Object[] nextPoints() {
    int numPoints = TestUtil.nextInt(random(), 1, 20);
    double[][] points = new double[numPoints][2];
    for (int i = 0; i < numPoints; i++) {
      points[i][0] = nextLatitude();
      points[i][1] = nextLongitude();
    }
    return points;
  }

  @Override
  protected double rectMinX(Object rect) {
    return ((Rectangle) rect).minLon;
  }

  @Override
  protected double rectMaxX(Object rect) {
    return ((Rectangle) rect).maxLon;
  }

  @Override
  protected double rectMinY(Object rect) {
    return ((Rectangle) rect).minLat;
  }

  /** factory method to create a new polygon query */
  protected Query newPolygonQuery(String field, QueryRelation queryRelation, Polygon... polygons) {
    return LatLonShape.newPolygonQuery(field, queryRelation, polygons);
  }

  @Override
  protected double rectMaxY(Object rect) {
    return ((Rectangle) rect).maxLat;
  }

  @Override
  protected boolean rectCrossesDateline(Object rect) {
    return ((Rectangle) rect).crossesDateline();
  }

  @Override
  public Line nextLine() {
    return GeoTestUtil.nextLine();
  }

  @Override
  protected Polygon nextPolygon() {
    return GeoTestUtil.nextPolygon();
  }

  @Override
  protected Encoder getEncoder() {
    return new Encoder() {
      @Override
      double decodeX(int encoded) {
        return decodeLongitude(encoded);
      }

      @Override
      double decodeY(int encoded) {
        return decodeLatitude(encoded);
      }

      @Override
      double quantizeX(double raw) {
        return decodeLongitude(encodeLongitude(raw));
      }

      @Override
      double quantizeXCeil(double raw) {
        return decodeLongitude(encodeLongitudeCeil(raw));
      }

      @Override
      double quantizeY(double raw) {
        return decodeLatitude(encodeLatitude(raw));
      }

      @Override
      double quantizeYCeil(double raw) {
        return decodeLatitude(encodeLatitudeCeil(raw));
      }
    };
  }

  /** internal shape type for testing different shape types */
  protected enum ShapeType {
    POINT() {
      public Point nextShape() {
        return GeoTestUtil.nextPoint();
      }
    },
    LINE() {
      public Line nextShape() {
        return GeoTestUtil.nextLine();
      }
    },
    POLYGON() {
      public Polygon nextShape() {
        while (true) {
          Polygon p = GeoTestUtil.nextPolygon();
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
