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
package org.apache.lucene.geo;

import org.apache.lucene.geo.SimpleWKTShapeParser.ShapeType;
import org.apache.lucene.util.LuceneTestCase;

/** simple WKT parsing tests */
public class TestSimpleWKTShapeParsing extends LuceneTestCase {

  /** test simple Point */
  public void testPoint() throws Exception {
    StringBuilder b = new StringBuilder();
    b.append(ShapeType.POINT + "(101.0 10.0)");
    Object shape = SimpleWKTShapeParser.parse(b.toString());

    assertTrue(shape instanceof double[]);
    double[] point = (double[])shape;
    assertEquals(101d, point[0], 0d);  // lon
    assertEquals(10d, point[1], 1d);   // lat
  }

  /** test POINT EMPTY returns null */
  public void testEmptyPoint() throws Exception {
    StringBuilder b = new StringBuilder();
    b.append(ShapeType.POINT + SimpleWKTShapeParser.SPACE + SimpleWKTShapeParser.EMPTY);
    Object shape = SimpleWKTShapeParser.parse(b.toString());
    assertNull(shape);
  }

  /** test simple MULTIPOINT */
  public void testMultiPoint() throws Exception {
    StringBuilder b = new StringBuilder();
    b.append(ShapeType.MULTIPOINT + "(101.0 10.0, 180.0 90.0, -180.0 -90.0)");
    Object shape = SimpleWKTShapeParser.parse(b.toString());

    assertTrue(shape instanceof double[][]);
    double[][] pts = (double[][])shape;
    assertEquals(3, pts.length,0);
    assertEquals(101d, pts[0][0], 0);
    assertEquals(10d, pts[0][1], 0);
    assertEquals(180d, pts[1][0], 0);
    assertEquals(90d, pts[1][1], 0);
    assertEquals(-180d, pts[2][0], 0);
    assertEquals(-90d, pts[2][1], 0);
  }

  /** test MULTIPOINT EMPTY returns null */
  public void testEmptyMultiPoint() throws Exception {
    StringBuilder b = new StringBuilder();
    b.append(ShapeType.MULTIPOINT + SimpleWKTShapeParser.SPACE + SimpleWKTShapeParser.EMPTY);
    Object shape = SimpleWKTShapeParser.parse(b.toString());
    assertNull(shape);
  }

  /** test simple LINESTRING */
  public void testLine() throws Exception {
    StringBuilder b = new StringBuilder();
    b.append(ShapeType.LINESTRING + "(101.0 10.0, 180.0 90.0, -180.0 -90.0)");
    Object shape = SimpleWKTShapeParser.parse(b.toString());

    assertTrue(shape instanceof Line);
    Line line = (Line)shape;
    assertEquals(3, line.numPoints(),0);
    assertEquals(101d, line.getLon(0), 0);
    assertEquals(10d, line.getLat(0), 0);
    assertEquals(180d, line.getLon(1), 0);
    assertEquals(90d, line.getLat(1), 0);
    assertEquals(-180d, line.getLon(2), 0);
    assertEquals(-90d, line.getLat(2), 0);
  }

  /** test empty LINESTRING */
  public void testEmptyLine() throws Exception {
    StringBuilder b = new StringBuilder();
    b.append(ShapeType.LINESTRING + SimpleWKTShapeParser.SPACE + SimpleWKTShapeParser.EMPTY);
    Object shape = SimpleWKTShapeParser.parse(b.toString());
    assertNull(shape);
  }

  /** test simple MULTILINESTRING */
  public void testMultiLine() throws Exception {
    StringBuilder b = new StringBuilder();
    b.append(ShapeType.MULTILINESTRING + "((100.0 0.0, 101.0 0.0, 101.0 1.0, 100.0 1.0, 100.0 0.0),");
    b.append("(10.0 2.0, 11.0 2.0, 11.0 3.0, 10.0 3.0, 10.0 2.0))");
    Object shape = SimpleWKTShapeParser.parse(b.toString());

    assertTrue(shape instanceof Line[]);
    Line[] lines = (Line[])shape;
    assertEquals(2, lines.length, 0);
  }

  /** test empty MULTILINESTRING */
  public void testEmptyMultiLine() throws Exception {
    StringBuilder b = new StringBuilder();
    b.append(ShapeType.MULTILINESTRING + SimpleWKTShapeParser.SPACE + SimpleWKTShapeParser.EMPTY);
    Object shape = SimpleWKTShapeParser.parse(b.toString());
    assertNull(shape);
  }

  /** test simple polygon: POLYGON((100.0 0.0, 101.0 0.0, 101.0 1.0, 100.0 1.0, 100.0 0.0)) */
  public void testPolygon() throws Exception {
    StringBuilder b = new StringBuilder();
    b.append(ShapeType.POLYGON + "((100.0 0.0, 101.0 0.0, 101.0 1.0, 100.0 1.0, 100.0 0.0))\n");
    Object shape = SimpleWKTShapeParser.parse(b.toString());

    assertTrue(shape instanceof Polygon);
    Polygon polygon = (Polygon)shape;
    assertEquals(new Polygon(new double[] {0.0, 0.0, 1.0, 1.0, 0.0},
        new double[] {100.0, 101.0, 101.0, 100.0, 100.0}), polygon);
  }

  /** test polygon with hole */
  public void testPolygonWithHole() throws Exception {
    StringBuilder b = new StringBuilder();
    b.append(ShapeType.POLYGON + "((100.0 0.0, 101.0 0.0, 101.0 1.0, 100.0 1.0, 100.0 0.0), ");
    b.append("(100.5 0.5, 100.5 0.75, 100.75 0.75, 100.75 0.5, 100.5 0.5))");
    Object shape = SimpleWKTShapeParser.parse(b.toString());

    assertTrue(shape instanceof Polygon);
    Polygon hole = new Polygon(new double[] {0.5, 0.75, 0.75, 0.5, 0.5},
        new double[] {100.5, 100.5, 100.75, 100.75, 100.5});
    Polygon expected = new Polygon(new double[] {0.0, 0.0, 1.0, 1.0, 0.0},
        new double[] {100.0, 101.0, 101.0, 100.0, 100.0}, hole);
    Polygon polygon = (Polygon)shape;

    assertEquals(expected, polygon);
  }

  /** test MultiPolygon returns Polygon array */
  public void testMultiPolygon() throws Exception {
    StringBuilder b = new StringBuilder();
    b.append(ShapeType.MULTIPOLYGON + "(((100.0 0.0, 101.0 0.0, 101.0 1.0, 100.0 1.0, 100.0 0.0)),");
    b.append("((10.0 2.0, 11.0 2.0, 11.0 3.0, 10.0 3.0, 10.0 2.0)))");
    Object shape = SimpleWKTShapeParser.parse(b.toString());

    assertTrue(shape instanceof Polygon[]);
    Polygon[] polygons = (Polygon[])shape;
    assertEquals(2, polygons.length);
    assertEquals(new Polygon(new double[] {0.0, 0.0, 1.0, 1.0, 0.0},
        new double[] {100.0, 101.0, 101.0, 100.0, 100.0}), polygons[0]);
    assertEquals(new Polygon(new double[] {2.0, 2.0, 3.0, 3.0, 2.0},
        new double[] {10.0, 11.0, 11.0, 10.0, 10.0}), polygons[1]);
  }

  /** polygon must be closed */
  public void testPolygonNotClosed() {
    StringBuilder b = new StringBuilder();
    b.append(ShapeType.POLYGON + "((100.0 0.0, 101.0 0.0, 101.0 1.0, 100.0 1.0))\n");

    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      SimpleWKTShapeParser.parse(b.toString());
    });
    assertTrue(expected.getMessage(),
        expected.getMessage().contains("first and last points of the polygon must be the same (it must close itself)"));
  }

  /** test simple ENVELOPE (minLon, maxLon, maxLat, minLat) */
  public void testEnvelope() throws Exception {
    StringBuilder b = new StringBuilder();
    b.append(ShapeType.ENVELOPE + "(-180.0, 180.0, 90.0, -90.0)");
    Object shape = SimpleWKTShapeParser.parse(b.toString());

    assertTrue(shape instanceof Rectangle);
    Rectangle bbox = (Rectangle)shape;
    assertEquals(-180d, bbox.minLon, 0);
    assertEquals(180d, bbox.maxLon, 0);
    assertEquals(-90d, bbox.minLat, 0);
    assertEquals(90d, bbox.maxLat, 0);
  }

  /** test simple geometry collection */
  public void testGeometryCollection() throws Exception {
    StringBuilder b = new StringBuilder();
    b.append(ShapeType.GEOMETRYCOLLECTION + "(");
    b.append(ShapeType.MULTIPOLYGON + "(((100.0 0.0, 101.0 0.0, 101.0 1.0, 100.0 1.0, 100.0 0.0)),");
    b.append("((10.0 2.0, 11.0 2.0, 11.0 3.0, 10.0 3.0, 10.0 2.0))),");
    b.append(ShapeType.POINT + "(101.0 10.0),");
    b.append(ShapeType.LINESTRING + "(101.0 10.0, 180.0 90.0, -180.0 -90.0),");
    b.append(ShapeType.ENVELOPE + "(-180.0, 180.0, 90.0, -90.0)");
    b.append(")");
    Object shape = SimpleWKTShapeParser.parse(b.toString());

    assertTrue(shape instanceof Object[]);
    Object[] shapes = (Object[]) shape;
    assertEquals(4, shapes.length);
    assertTrue(shapes[0] instanceof Polygon[]);
    assertTrue(shapes[1] instanceof double[]);
    assertTrue(shapes[2] instanceof Line);
    assertTrue(shapes[3] instanceof Rectangle);
  }
}
