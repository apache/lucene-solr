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

import java.util.Arrays;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import org.apache.lucene.document.ShapeField.QueryRelation;
import org.apache.lucene.geo.Component2D;
import org.apache.lucene.geo.GeoTestUtil;
import org.apache.lucene.geo.LatLonGeometry;
import org.apache.lucene.geo.Line;
import org.apache.lucene.geo.Polygon;
import org.apache.lucene.geo.Rectangle;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryUtils;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.geo.Circle;

import static org.apache.lucene.geo.GeoEncodingUtils.decodeLatitude;
import static org.apache.lucene.geo.GeoEncodingUtils.decodeLongitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLatitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLatitudeCeil;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLongitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLongitudeCeil;
import static org.apache.lucene.geo.GeoTestUtil.nextLatitude;
import static org.apache.lucene.geo.GeoTestUtil.nextLongitude;

/** Base test case for testing geospatial indexing and search functionality **/
public abstract class BaseLatLonShapeTestCase extends BaseShapeTestCase {

  protected abstract ShapeType getShapeType();

  protected Object nextShape() {
    return getShapeType().nextShape();
  }

  /** factory method to create a new bounding box query */
  @Override
  protected Query newRectQuery(String field, QueryRelation queryRelation, double minLon, double maxLon, double minLat, double maxLat) {
    return LatLonShape.newBoxQuery(field, queryRelation, minLat, maxLat, minLon, maxLon);
  }

  /** factory method to create a new line query */
  @Override
  protected Query newLineQuery(String field, QueryRelation queryRelation, Object... lines) {
    return LatLonShape.newLineQuery(field, queryRelation, Arrays.stream(lines).toArray(Line[]::new));
  }

  /** factory method to create a new polygon query */
  @Override
  protected Query newPolygonQuery(String field, QueryRelation queryRelation, Object... polygons) {
    return LatLonShape.newPolygonQuery(field, queryRelation, Arrays.stream(polygons).toArray(Polygon[]::new));
  }

  @Override
  protected Query newPointsQuery(String field, QueryRelation queryRelation, Object... points) {
    return LatLonShape.newPointQuery(field, queryRelation, Arrays.stream(points).toArray(double[][]::new));
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
  protected Component2D toPoint2D(Object... points) {
    double[][] p = Arrays.stream(points).toArray(double[][]::new);
    org.apache.lucene.geo.Point[] pointArray = new org.apache.lucene.geo.Point[points.length];
    for (int i =0; i < points.length; i++) {
      pointArray[i] = new org.apache.lucene.geo.Point(p[i][0], p[i][1]);
    }
    return LatLonGeometry.create(pointArray);
  }

  @Override
  protected Query newDistanceQuery(String field, QueryRelation queryRelation, Object circle) {
    return LatLonShape.newDistanceQuery(field, queryRelation, (Circle) circle);
  }

  @Override
  protected Component2D toCircle2D(Object circle) {
    return LatLonGeometry.create((Circle) circle);
  }

  @Override
  protected Circle nextCircle() {
    return new Circle(nextLatitude(), nextLongitude(), random().nextDouble() * Circle.MAX_RADIUS);
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
    return ((Rectangle)rect).minLon;
  }

  @Override
  protected double rectMaxX(Object rect) {
    return ((Rectangle)rect).maxLon;
  }

  @Override
  protected double rectMinY(Object rect) {
    return ((Rectangle)rect).minLat;
  }

  public void testBoxQueryEqualsAndHashcode() {
    Rectangle rectangle = GeoTestUtil.nextBox();
    QueryRelation queryRelation = RandomPicks.randomFrom(random(), QueryRelation.values());
    String fieldName = "foo";
    Query q1 = newRectQuery(fieldName, queryRelation, rectangle.minLon, rectangle.maxLon, rectangle.minLat, rectangle.maxLat);
    Query q2 = newRectQuery(fieldName, queryRelation, rectangle.minLon, rectangle.maxLon, rectangle.minLat, rectangle.maxLat);
    QueryUtils.checkEqual(q1, q2);
    //different field name
    Query q3 = newRectQuery("bar", queryRelation, rectangle.minLon, rectangle.maxLon, rectangle.minLat, rectangle.maxLat);
    QueryUtils.checkUnequal(q1, q3);
    //different query relation
    QueryRelation newQueryRelation = RandomPicks.randomFrom(random(), QueryRelation.values());
    Query q4 = newRectQuery(fieldName, newQueryRelation, rectangle.minLon, rectangle.maxLon, rectangle.minLat, rectangle.maxLat);
    if (queryRelation == newQueryRelation) {
      QueryUtils.checkEqual(q1, q4);
    } else {
      QueryUtils.checkUnequal(q1, q4);
    }
    //different shape
    Rectangle newRectangle = GeoTestUtil.nextBox();
    Query q5 = newRectQuery(fieldName, queryRelation, newRectangle.minLon, newRectangle.maxLon, newRectangle.minLat, newRectangle.maxLat);
    if (rectangle.equals(newRectangle)) {
      QueryUtils.checkEqual(q1, q5);
    } else {
      QueryUtils.checkUnequal(q1, q5);
    }
  }

  /** factory method to create a new line query */
  protected Query newLineQuery(String field, QueryRelation queryRelation, Line... lines) {
    return LatLonShape.newLineQuery(field, queryRelation, lines);
  }

  public void testLineQueryEqualsAndHashcode() {
    Line line = nextLine();
    QueryRelation queryRelation = RandomPicks.randomFrom(random(), POINT_LINE_RELATIONS);
    String fieldName = "foo";
    Query q1 = newLineQuery(fieldName, queryRelation, line);
    Query q2 = newLineQuery(fieldName, queryRelation, line);
    QueryUtils.checkEqual(q1, q2);
    //different field name
    Query q3 = newLineQuery("bar", queryRelation, line);
    QueryUtils.checkUnequal(q1, q3);
    //different query relation
    QueryRelation newQueryRelation = RandomPicks.randomFrom(random(), POINT_LINE_RELATIONS);
    Query q4 = newLineQuery(fieldName, newQueryRelation, line);
    if (queryRelation == newQueryRelation) {
      QueryUtils.checkEqual(q1, q4);
    } else {
      QueryUtils.checkUnequal(q1, q4);
    }
    //different shape
    Line newLine = nextLine();
    Query q5 = newLineQuery(fieldName, queryRelation, newLine);
    if (line.equals(newLine)) {
      QueryUtils.checkEqual(q1, q5);
    } else {
      QueryUtils.checkUnequal(q1, q5);
    }
  }

  public void testBoundingBoxQueriesEquivalence() throws Exception {
    int numShapes = atLeast(20);

    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);

    for (int  i =0; i < numShapes; i++) {
      indexRandomShapes(w.w, nextShape());
    }
    if (random().nextBoolean()) {
      w.forceMerge(1);
    }

    ///// search //////
    IndexReader reader = w.getReader();
    w.close();
    IndexSearcher searcher = newSearcher(reader);

    Rectangle box = GeoTestUtil.nextBox();

    Query q1 = LatLonShape.newBoxQuery(FIELD_NAME, QueryRelation.INTERSECTS, box.minLat, box.maxLat, box.minLon, box.maxLon);
    Query q2 = new LatLonShapeQuery(FIELD_NAME, QueryRelation.INTERSECTS, box);
    assertEquals(searcher.count(q1), searcher.count(q2));
    q1 = LatLonShape.newBoxQuery(FIELD_NAME, QueryRelation.WITHIN, box.minLat, box.maxLat, box.minLon, box.maxLon);
    q2 = new LatLonShapeQuery(FIELD_NAME, QueryRelation.WITHIN, box);
    assertEquals(searcher.count(q1), searcher.count(q2));
    q1 = LatLonShape.newBoxQuery(FIELD_NAME, QueryRelation.CONTAINS, box.minLat, box.maxLat, box.minLon, box.maxLon);
    if (box.crossesDateline()) {
      q2 = LatLonShape.newGeometryQuery(FIELD_NAME, QueryRelation.CONTAINS, box);
    } else {
      q2 = new LatLonShapeQuery(FIELD_NAME, QueryRelation.CONTAINS, box);
    }
    assertEquals(searcher.count(q1), searcher.count(q2));
    q1 = LatLonShape.newBoxQuery(FIELD_NAME, QueryRelation.DISJOINT, box.minLat, box.maxLat, box.minLon, box.maxLon);
    q2 = new LatLonShapeQuery(FIELD_NAME, QueryRelation.DISJOINT, box);
    assertEquals(searcher.count(q1), searcher.count(q2));

    IOUtils.close(w, reader, dir);
  }

  /** factory method to create a new polygon query */
  protected Query newPolygonQuery(String field, QueryRelation queryRelation, Polygon... polygons) {
    return LatLonShape.newPolygonQuery(field, queryRelation, polygons);
  }

  /** factory method to create a new point query */
  protected Query newPointQuery(String field, QueryRelation queryRelation, double[]... points) {
    return LatLonShape.newPointQuery(field, queryRelation, points);
  }

  public void testPolygonQueryEqualsAndHashcode() {
    Polygon polygon = GeoTestUtil.nextPolygon();
    QueryRelation queryRelation = RandomPicks.randomFrom(random(), QueryRelation.values());
    String fieldName = "foo";
    Query q1 = newPolygonQuery(fieldName, queryRelation, polygon);
    Query q2 = newPolygonQuery(fieldName, queryRelation, polygon);
    QueryUtils.checkEqual(q1, q2);
    //different field name
    Query q3 = newPolygonQuery("bar", queryRelation, polygon);
    QueryUtils.checkUnequal(q1, q3);
    //different query relation
    QueryRelation newQueryRelation = RandomPicks.randomFrom(random(), QueryRelation.values());
    Query q4 = newPolygonQuery(fieldName, newQueryRelation, polygon);
    if (queryRelation == newQueryRelation) {
      QueryUtils.checkEqual(q1, q4);
    } else {
      QueryUtils.checkUnequal(q1, q4);
    }
    //different shape
    Polygon newPolygon = GeoTestUtil.nextPolygon();;
    Query q5 = newPolygonQuery(fieldName, queryRelation, newPolygon);
    if (polygon.equals(newPolygon)) {
      QueryUtils.checkEqual(q1, q5);
    } else {
      QueryUtils.checkUnequal(q1, q5);
    }
  }

  @Override
  protected double rectMaxY(Object rect) {
    return ((Rectangle)rect).maxLat;
  }

  @Override
  protected boolean rectCrossesDateline(Object rect) {
    return ((Rectangle)rect).crossesDateline();
  }

  /** use {@link GeoTestUtil#nextPolygon()} to create a random line; TODO: move to GeoTestUtil */
  @Override
  public Line nextLine() {
    return getNextLine();
  }

  public static Line getNextLine() {
    Polygon poly = GeoTestUtil.nextPolygon();
    double[] lats = new double[poly.numPoints() - 1];
    double[] lons = new double[lats.length];
    System.arraycopy(poly.getPolyLats(), 0, lats, 0, lats.length);
    System.arraycopy(poly.getPolyLons(), 0, lons, 0, lons.length);

    return new Line(lats, lons);
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

      /** quantizes a latitude value to be consistent with index encoding */
      protected double quantizeLat(double rawLat) {
        return quantizeY(rawLat);
      }

      /** quantizes a provided latitude value rounded up to the nearest encoded integer */
      protected double quantizeLatCeil(double rawLat) {
        return quantizeYCeil(rawLat);
      }

      /** quantizes a longitude value to be consistent with index encoding */
      protected double quantizeLon(double rawLon) {
        return quantizeX(rawLon);
      }

      /** quantizes a provided longitude value rounded up to the nearest encoded integer */
      protected double quantizeLonCeil(double rawLon) {
        return quantizeXCeil(rawLon);
      }

      @Override
      double[] quantizeTriangle(double ax, double ay, boolean ab, double bx, double by, boolean bc, double cx, double cy, boolean ca) {
        ShapeField.DecodedTriangle decoded = encodeDecodeTriangle(ax, ay, ab, bx, by, bc, cx, cy, ca);
        return new double[]{decodeLatitude(decoded.aY), decodeLongitude(decoded.aX), decodeLatitude(decoded.bY), decodeLongitude(decoded.bX), decodeLatitude(decoded.cY), decodeLongitude(decoded.cX)};
      }

      @Override
      ShapeField.DecodedTriangle encodeDecodeTriangle(double ax, double ay, boolean ab, double bx, double by, boolean bc, double cx, double cy, boolean ca) {
        byte[] encoded = new byte[7 * ShapeField.BYTES];
        ShapeField.encodeTriangle(encoded, encodeLatitude(ay), encodeLongitude(ax), ab, encodeLatitude(by), encodeLongitude(bx), bc, encodeLatitude(cy), encodeLongitude(cx), ca);
        ShapeField.DecodedTriangle triangle  = new ShapeField.DecodedTriangle();
        ShapeField.decodeTriangle(encoded, triangle);
        return triangle;
      }
    };
  }

  /** internal shape type for testing different shape types */
  protected enum ShapeType {
    POINT() {
      public Point nextShape() {
        return new Point(nextLongitude(), nextLatitude());
      }
    },
    LINE() {
      public Line nextShape() {
        Polygon p = GeoTestUtil.nextPolygon();
        double[] lats = new double[p.numPoints() - 1];
        double[] lons = new double[lats.length];
        for (int i = 0; i < lats.length; ++i) {
          lats[i] = p.getPolyLat(i);
          lons[i] = p.getPolyLon(i);
        }
        return new Line(lats, lons);
      }
    },
    POLYGON() {
      public Polygon nextShape() {
        return GeoTestUtil.nextPolygon();
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

    static ShapeType fromObject(Object shape) {
      if (shape instanceof Point) {
        return POINT;
      } else if (shape instanceof Line) {
        return LINE;
      } else if (shape instanceof Polygon) {
        return POLYGON;
      }
      throw new IllegalArgumentException("invalid shape type from " + shape.toString());
    }
  }

  /** internal lat lon point class for testing point shapes */
  protected static class Point {
    double lon;
    double lat;

    public Point(double lon, double lat) {
      this.lon = lon;
      this.lat = lat;
    }

    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("POINT(");
      sb.append(lon);
      sb.append(',');
      sb.append(lat);
      sb.append(')');
      return sb.toString();
    }
  }
}
