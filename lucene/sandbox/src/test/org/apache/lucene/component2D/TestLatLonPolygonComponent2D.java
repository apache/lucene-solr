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
package org.apache.lucene.component2D;

import com.carrotsearch.randomizedtesting.generators.RandomNumbers;
import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.geo.GeoTestUtil;
import org.apache.lucene.geo.Polygon;
import org.apache.lucene.geo.Rectangle;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.util.TestUtil;

import static org.apache.lucene.geo.GeoTestUtil.createRegularPolygon;
import static org.apache.lucene.geo.GeoTestUtil.nextLatitude;
import static org.apache.lucene.geo.GeoTestUtil.nextLongitude;
import static org.apache.lucene.geo.GeoTestUtil.nextPointNear;
import static org.apache.lucene.geo.GeoTestUtil.nextPolygon;

/** Test Polygon2D impl */
public class TestLatLonPolygonComponent2D extends TestBaseLatLonComponent2D {

  @Override
  protected Object nextShape() {
    return GeoTestUtil.nextPolygon();
  }

  @Override
  protected Component2D getComponent(Object shape) {
    if (random().nextBoolean()) {
      return LatLonComponent2DFactory.create(shape);
    } else {
      return LatLonComponent2DFactory.create((Polygon) shape);
    }
  }

  /** Three boxes, an island inside a hole inside a shape */
  public void testMultiPolygon() {
    Polygon hole = new Polygon(new double[] { -10, -10, 10, 10, -10 }, new double[] { -10, 10, 10, -10, -10 });
    Polygon outer = new Polygon(new double[] { -50, -50, 50, 50, -50 }, new double[] { -50, 50, 50, -50, -50 }, hole);
    Polygon island = new Polygon(new double[] { -5, -5, 5, 5, -5 }, new double[] { -5, 5, 5, -5, -5 } );
    Component2D polygon = LatLonComponent2DFactory.create(outer, island);

    // contains(point)
    assertTrue(polygon.contains(GeoEncodingUtils.encodeLongitude(2), GeoEncodingUtils.encodeLatitude(-2))); // on the island
    assertFalse(polygon.contains(GeoEncodingUtils.encodeLongitude(6), GeoEncodingUtils.encodeLatitude(-6))); // in the hole
    assertTrue(polygon.contains(GeoEncodingUtils.encodeLongitude(25), GeoEncodingUtils.encodeLatitude(-25))); // on the mainland
    assertFalse(polygon.contains(GeoEncodingUtils.encodeLongitude(51), GeoEncodingUtils.encodeLatitude(-51))); // in the ocean

    // relate(box): this can conservatively return CELL_CROSSES_QUERY
    assertEquals(Relation.CELL_INSIDE_QUERY, polygon.relate(GeoEncodingUtils.encodeLongitude(-2), GeoEncodingUtils.encodeLongitude(2), GeoEncodingUtils.encodeLatitude(-2), GeoEncodingUtils.encodeLatitude(2))); // on the island
    assertEquals(Relation.CELL_OUTSIDE_QUERY, polygon.relate(GeoEncodingUtils.encodeLongitude(6), GeoEncodingUtils.encodeLongitude(7), GeoEncodingUtils.encodeLatitude(6), GeoEncodingUtils.encodeLatitude(7))); // in the hole
    assertEquals(Relation.CELL_INSIDE_QUERY, polygon.relate(GeoEncodingUtils.encodeLongitude(24), GeoEncodingUtils.encodeLongitude(25), GeoEncodingUtils.encodeLatitude(24), GeoEncodingUtils.encodeLatitude(25))); // on the mainland
    assertEquals(Relation.CELL_OUTSIDE_QUERY, polygon.relate(GeoEncodingUtils.encodeLongitude(51), GeoEncodingUtils.encodeLongitude(52), GeoEncodingUtils.encodeLatitude(51), GeoEncodingUtils.encodeLatitude(52))); // in the ocean
    assertEquals(Relation.CELL_CROSSES_QUERY, polygon.relate(GeoEncodingUtils.encodeLongitude(-60), GeoEncodingUtils.encodeLongitude(60), GeoEncodingUtils.encodeLatitude(-60), GeoEncodingUtils.encodeLatitude(60))); // enclosing us completely
    assertEquals(Relation.CELL_CROSSES_QUERY, polygon.relate(GeoEncodingUtils.encodeLongitude(49), GeoEncodingUtils.encodeLongitude(51), GeoEncodingUtils.encodeLatitude(49), GeoEncodingUtils.encodeLatitude(51))); // overlapping the mainland
    assertEquals(Relation.CELL_CROSSES_QUERY, polygon.relate(GeoEncodingUtils.encodeLongitude(9), GeoEncodingUtils.encodeLongitude(11), GeoEncodingUtils.encodeLatitude(9), GeoEncodingUtils.encodeLatitude(11))); // overlapping the hole
    assertEquals(Relation.CELL_CROSSES_QUERY, polygon.relate(GeoEncodingUtils.encodeLongitude(5), GeoEncodingUtils.encodeLongitude(6), GeoEncodingUtils.encodeLatitude(5), GeoEncodingUtils.encodeLatitude(6))); // overlapping the island
  }

  public void testRandomMultiPolygon() {
    int length = random().nextInt(14) + 1;
    Polygon[] polygons = new Polygon[length];
    Component2D[] components = new Component2D[length];
    for (int i =0; i < length; i++) {
      polygons[i] = GeoTestUtil.nextPolygon();
      components[i] = LatLonComponent2DFactory.create(polygons[i]);
    }
    Component2D component = LatLonComponent2DFactory.create(polygons);
    for (int j = 0; j < 1000; j++) {
      int latitude = GeoEncodingUtils.encodeLatitude(GeoTestUtil.nextLatitude());
      int longitude = GeoEncodingUtils.encodeLongitude(GeoTestUtil.nextLongitude());
      boolean c1 = component.contains(latitude, longitude);
      boolean c2 = false;
      for (Component2D c : components) {
        if (c.contains(latitude, longitude)) {
          c2 = true;
          break;
        }
      }
      assertEquals(c1, c2);
    }
  }

  public void testPacMan() throws Exception {
    // pacman
    double[] px = {0, 10, 10, 0, -8, -10, -8, 0, 10, 10, 0};
    double[] py = {0, 5, 9, 10, 9, 0, -9, -10, -9, -5, 0};

    // candidate crosses cell
    int xMin = GeoEncodingUtils.encodeLongitude(2);//-5;
    int xMax = GeoEncodingUtils.encodeLongitude(11);//0.000001;
    int yMin = GeoEncodingUtils.encodeLatitude(-1);//0;
    int yMax = GeoEncodingUtils.encodeLatitude(1);//5;

    // test cell crossing poly
    Component2D polygon = LatLonComponent2DFactory.create(new Polygon(py, px));
    assertEquals(Relation.CELL_CROSSES_QUERY, polygon.relate(xMin, xMax, yMin, yMax));
  }

  public void testBoundingBox() throws Exception {
    for (int i = 0; i < 100; i++) {
      Component2D impl = LatLonComponent2DFactory.create(nextPolygon());
      for (int j = 0; j < 100; j++) {
        int x = GeoEncodingUtils.encodeLongitude(nextLongitude());
        int y = GeoEncodingUtils.encodeLatitude(nextLatitude());
        // if the point is within poly, then it should be in our bounding box
        if (impl.contains(x, y)) {
          assertTrue(x >= impl.getBoundingBox().minX && x <= impl.getBoundingBox().maxX);
          assertTrue(y >= impl.getBoundingBox().minY && y <= impl.getBoundingBox().maxY);
        }
      }
    }
  }

  // targets the bounding box directly
  public void testBoundingBoxEdgeCases() throws Exception {
    for (int i = 0; i < 100; i++) {
      Polygon polygon = nextPolygon();
      Component2D impl = LatLonComponent2DFactory.create(polygon);

      for (int j = 0; j < 100; j++) {
        double point[] = GeoTestUtil.nextPointNear(polygon);
        int x = GeoEncodingUtils.encodeLatitude(point[0]);
        int y = GeoEncodingUtils.encodeLongitude(point[1]);
        // if the point is within poly, then it should be in our bounding box
        if (impl.contains(x, y)) {
          assertTrue(x >= impl.getBoundingBox().minX && x <= impl.getBoundingBox().maxX);
          assertTrue(y >= impl.getBoundingBox().minY && y <= impl.getBoundingBox().maxY);
        }
      }
    }
  }

  /** If polygon.contains(box) returns true, then any point in that box should return true as well */
  public void testContainsRandom() throws Exception {
    int iters = atLeast(50);
    for (int i = 0; i < iters; i++) {
      Polygon polygon = nextPolygon();
      Component2D impl = LatLonComponent2DFactory.create(polygon);
      for (int j = 0; j < 100; j++) {
        Rectangle rectangle = GeoTestUtil.nextBoxNear(polygon);
        // allowed to conservatively return false
        if (impl.relate(GeoEncodingUtils.encodeLongitude(rectangle.minLon), GeoEncodingUtils.encodeLongitude(rectangle.maxLon),
            GeoEncodingUtils.encodeLatitude(rectangle.minLat), GeoEncodingUtils.encodeLatitude(rectangle.maxLat)) == Relation.CELL_INSIDE_QUERY) {
          for (int k = 0; k < 500; k++) {
            // this tests in our range but sometimes outside! so we have to double-check its really in other box
            double point[] = GeoTestUtil.nextPointNear(rectangle);
            int x = GeoEncodingUtils.encodeLongitude(point[1]);
            int y = GeoEncodingUtils.encodeLatitude(point[0]);

            // check for sure its in our box
            if (y >= GeoEncodingUtils.encodeLatitude(rectangle.minLat) && y <= GeoEncodingUtils.encodeLatitude(rectangle.maxLat) &&
                x >= GeoEncodingUtils.encodeLongitude(rectangle.minLon) && x <= GeoEncodingUtils.encodeLongitude(rectangle.maxLon)) {
              assertTrue(impl.contains(x, y));
              assertEquals(Relation.CELL_INSIDE_QUERY, impl.relateTriangle(x, y, x, y, x, y));
            }
          }
          for (int k = 0; k < 100; k++) {
            // this tests in our range but sometimes outside! so we have to double-check its really in other box
            double point[] = GeoTestUtil.nextPointNear(polygon);
            int x = GeoEncodingUtils.encodeLongitude(point[1]);
            int y = GeoEncodingUtils.encodeLatitude(point[0]);
            // check for sure its in our box
            if (y >= GeoEncodingUtils.encodeLatitude(rectangle.minLat) && y <= GeoEncodingUtils.encodeLatitude(rectangle.maxLat) &&
                x >= GeoEncodingUtils.encodeLongitude(rectangle.minLon) && x <= GeoEncodingUtils.encodeLongitude(rectangle.maxLon)) {
              assertTrue(impl.contains(x, y));
              assertEquals(Relation.CELL_INSIDE_QUERY, impl.relateTriangle(x, y, x, y, x, y));
            }
          }
        }
      }
    }
  }

  /** If polygon.contains(box) returns true, then any point in that box should return true as well */
  // different from testContainsRandom in that its not a purely random test. we iterate the vertices of the polygon
  // and generate boxes near each one of those to try to be more efficient.
  public void testContainsEdgeCases() throws Exception {
    for (int i = 0; i < 1000; i++) {
      Polygon polygon = nextPolygon();
      Component2D impl = LatLonComponent2DFactory.create(polygon);
      for (int j = 0; j < 10; j++) {
        Rectangle rectangle = GeoTestUtil.nextBoxNear(polygon);
        // allowed to conservatively return false
        if (impl.relate(GeoEncodingUtils.encodeLongitude(rectangle.minLon), GeoEncodingUtils.encodeLongitude(rectangle.maxLon),
            GeoEncodingUtils.encodeLatitude(rectangle.minLat), GeoEncodingUtils.encodeLatitude(rectangle.maxLat)) == Relation.CELL_INSIDE_QUERY) {
          for (int k = 0; k < 100; k++) {
            // this tests in our range but sometimes outside! so we have to double-check its really in other box
            double point[] = GeoTestUtil.nextPointNear(rectangle);
            int x = GeoEncodingUtils.encodeLongitude(point[1]);
            int y = GeoEncodingUtils.encodeLatitude(point[0]);
            // check for sure its in our box
            if (y >= GeoEncodingUtils.encodeLatitude(rectangle.minLat) && y <= GeoEncodingUtils.encodeLatitude(rectangle.maxLat) &&
                x >= GeoEncodingUtils.encodeLongitude(rectangle.minLon) && x <= GeoEncodingUtils.encodeLongitude(rectangle.maxLon)) {
              assertTrue(impl.contains(x, y));
              assertEquals(Relation.CELL_INSIDE_QUERY, impl.relateTriangle(x, y, x, y, x, y));
            }
          }
          for (int k = 0; k < 20; k++) {
            // this tests in our range but sometimes outside! so we have to double-check its really in other box
            double point[] = GeoTestUtil.nextPointNear(polygon);
            int x = GeoEncodingUtils.encodeLongitude(point[1]);
            int y = GeoEncodingUtils.encodeLatitude(point[0]);
            // check for sure its in our box
            if (y >= GeoEncodingUtils.encodeLatitude(rectangle.minLat) && y <= GeoEncodingUtils.encodeLatitude(rectangle.maxLat) &&
                x >= GeoEncodingUtils.encodeLongitude(rectangle.minLon) && x <= GeoEncodingUtils.encodeLongitude(rectangle.maxLon)) {
              assertTrue(impl.contains(x, y));
              assertEquals(Relation.CELL_INSIDE_QUERY, impl.relateTriangle(x, y, x, y, x, y));
            }
          }
        }
      }
    }
  }

  /** If polygon.intersects(box) returns false, then any point in that box should return false as well */
  public void testIntersectRandom() {
    int iters = atLeast(10);
    for (int i = 0; i < iters; i++) {
      Polygon polygon = nextPolygon();
      Component2D impl = LatLonComponent2DFactory.create(polygon);
      for (int j = 0; j < 100; j++) {
        Rectangle rectangle = GeoTestUtil.nextBoxNear(polygon);
        if (impl.relate(GeoEncodingUtils.encodeLongitude(rectangle.minLon), GeoEncodingUtils.encodeLongitude(rectangle.maxLon),
            GeoEncodingUtils.encodeLatitude(rectangle.minLat), GeoEncodingUtils.encodeLatitude(rectangle.maxLat)) == Relation.CELL_OUTSIDE_QUERY) {
          for (int k = 0; k < 1000; k++) {
            double point[] = GeoTestUtil.nextPointNear(rectangle);
            // this tests in our range but sometimes outside! so we have to double-check its really in other box
            int x = GeoEncodingUtils.encodeLongitude(point[1]);
            int y = GeoEncodingUtils.encodeLatitude(point[0]);
            // check for sure its in our box
            if (y >= GeoEncodingUtils.encodeLatitude(rectangle.minLat) && y <= GeoEncodingUtils.encodeLatitude(rectangle.maxLat) &&
                x >= GeoEncodingUtils.encodeLongitude(rectangle.minLon) && x <= GeoEncodingUtils.encodeLongitude(rectangle.maxLon)) {
              assertFalse(impl.contains(x, y));
              assertEquals(Relation.CELL_OUTSIDE_QUERY, impl.relateTriangle(x, y, x, y, x, y));
            }
          }
          for (int k = 0; k < 100; k++) {
            double point[] = GeoTestUtil.nextPointNear(polygon);
            // this tests in our range but sometimes outside! so we have to double-check its really in other box
            int x = GeoEncodingUtils.encodeLongitude(point[1]);
            int y = GeoEncodingUtils.encodeLatitude(point[0]);
            // check for sure its in our box
            if (y >= GeoEncodingUtils.encodeLatitude(rectangle.minLat) && y <= GeoEncodingUtils.encodeLatitude(rectangle.maxLat) &&
                x >= GeoEncodingUtils.encodeLongitude(rectangle.minLon) && x <= GeoEncodingUtils.encodeLongitude(rectangle.maxLon)) {
              assertFalse(impl.contains(x, y));
              assertEquals(Relation.CELL_OUTSIDE_QUERY, impl.relateTriangle(x, y, x, y, x, y));
            }
          }
        }
      }
    }
  }

  /** If polygon.intersects(box) returns false, then any point in that box should return false as well */
  // different from testIntersectsRandom in that its not a purely random test. we iterate the vertices of the polygon
  // and generate boxes near each one of those to try to be more efficient.
  public void testIntersectEdgeCases() {
    for (int i = 0; i < 100; i++) {
      Polygon polygon = nextPolygon();
      Component2D impl = LatLonComponent2DFactory.create(polygon);

      for (int j = 0; j < 10; j++) {
        Rectangle rectangle = GeoTestUtil.nextBoxNear(polygon);
        // allowed to conservatively return false.
        if (impl.relate(GeoEncodingUtils.encodeLongitude(rectangle.minLon), GeoEncodingUtils.encodeLongitude(rectangle.maxLon),
            GeoEncodingUtils.encodeLatitude(rectangle.minLat), GeoEncodingUtils.encodeLatitude(rectangle.maxLat)) == Relation.CELL_OUTSIDE_QUERY) {
          for (int k = 0; k < 100; k++) {
            // this tests in our range but sometimes outside! so we have to double-check its really in other box
            double point[] = GeoTestUtil.nextPointNear(rectangle);
            int x = GeoEncodingUtils.encodeLongitude(point[1]);
            int y = GeoEncodingUtils.encodeLatitude(point[0]);
            // check for sure its in our box
            if (y >= GeoEncodingUtils.encodeLatitude(rectangle.minLat) && y <= GeoEncodingUtils.encodeLatitude(rectangle.maxLat) &&
                x >= GeoEncodingUtils.encodeLongitude(rectangle.minLon) && x <= GeoEncodingUtils.encodeLongitude(rectangle.maxLon)) {
              assertFalse(impl.contains(x, y));
              assertEquals(Relation.CELL_OUTSIDE_QUERY, impl.relateTriangle(x, y, x, y, x, y));
            }
          }
          for (int k = 0; k < 50; k++) {
            // this tests in our range but sometimes outside! so we have to double-check its really in other box
            double point[] = GeoTestUtil.nextPointNear(polygon);
            int x = GeoEncodingUtils.encodeLongitude(point[1]);
            int y = GeoEncodingUtils.encodeLatitude(point[0]);
            // check for sure its in our box
            if (y >= GeoEncodingUtils.encodeLatitude(rectangle.minLat) && y <= GeoEncodingUtils.encodeLatitude(rectangle.maxLat) &&
                x >= GeoEncodingUtils.encodeLongitude(rectangle.minLon) && x <= GeoEncodingUtils.encodeLongitude(rectangle.maxLon)) {
              assertFalse(impl.contains(x, y));
              assertEquals(Relation.CELL_OUTSIDE_QUERY, impl.relateTriangle(x, y, x, y, x, y));
            }
          }
        }
      }
    }
  }

  /** Tests edge case behavior with respect to insideness */
  public void testEdgeInsideness() {
    Component2D poly = LatLonComponent2DFactory.create(new Polygon(new double[] { -2, -2, 2, 2, -2 }, new double[] { -2, 2, 2, -2, -2 }));
    assertTrue(poly.contains(GeoEncodingUtils.encodeLongitude(-2), GeoEncodingUtils.encodeLatitude(-2))); // bottom left corner: true
    assertTrue(poly.contains(GeoEncodingUtils.encodeLongitude(2), GeoEncodingUtils.encodeLatitude(-2)));  // bottom right corner: true
    assertTrue(poly.contains(GeoEncodingUtils.encodeLongitude(-2), GeoEncodingUtils.encodeLatitude(2)));  // top left corner: true
    assertTrue(poly.contains(GeoEncodingUtils.encodeLongitude(2), GeoEncodingUtils.encodeLatitude(2)));  // top right corner: true
    assertTrue(poly.contains(GeoEncodingUtils.encodeLongitude(-1), GeoEncodingUtils.encodeLatitude(-2))); // bottom side: true
    assertTrue(poly.contains(GeoEncodingUtils.encodeLongitude(0), GeoEncodingUtils.encodeLatitude(-2)));  // bottom side: true
    assertTrue(poly.contains(GeoEncodingUtils.encodeLongitude(1), GeoEncodingUtils.encodeLatitude(-2)));  // bottom side: true
    assertTrue(poly.contains(GeoEncodingUtils.encodeLongitude(-1), GeoEncodingUtils.encodeLatitude(2)));  // top side: true
    assertTrue(poly.contains(GeoEncodingUtils.encodeLongitude(0), GeoEncodingUtils.encodeLatitude(2)));   // top side: true
    assertTrue(poly.contains(GeoEncodingUtils.encodeLongitude(1), GeoEncodingUtils.encodeLatitude(2)));   // top side: true
    assertTrue(poly.contains(GeoEncodingUtils.encodeLongitude(2), GeoEncodingUtils.encodeLatitude(-1)));  // right side: true
    assertTrue(poly.contains(GeoEncodingUtils.encodeLongitude(2), GeoEncodingUtils.encodeLatitude(0)));   // right side: true
    assertTrue(poly.contains(GeoEncodingUtils.encodeLongitude(2), GeoEncodingUtils.encodeLatitude(1)));   // right side: true
    assertTrue(poly.contains(GeoEncodingUtils.encodeLongitude(-2), GeoEncodingUtils.encodeLatitude(-1))); // left side: true
    assertTrue(poly.contains(GeoEncodingUtils.encodeLongitude(-2), GeoEncodingUtils.encodeLatitude(0)));  // left side: true
    assertTrue(poly.contains(GeoEncodingUtils.encodeLongitude(-2), GeoEncodingUtils.encodeLatitude(1)));  // left side: true
  }

  // targets the polygon directly
  public void testRelateTriangle() {
    for (int i = 0; i < 100; ++i) {
      Polygon polygon = nextPolygon();
      Component2D impl = LatLonComponent2DFactory.create(polygon);

      for (int j = 0; j < 100; j++) {
        double[] a = nextPointNear(polygon);
        double[] b = nextPointNear(polygon);
        double[] c = nextPointNear(polygon);

        int[] aEnc = new int[] {GeoEncodingUtils.encodeLatitude(a[0]), GeoEncodingUtils.encodeLongitude(a[1])};
        int[] bEnc = new int[] {GeoEncodingUtils.encodeLatitude(b[0]), GeoEncodingUtils.encodeLongitude(b[1])};
        int[] cEnc = new int[] {GeoEncodingUtils.encodeLatitude(c[0]), GeoEncodingUtils.encodeLongitude(c[1])};

        // if the point is within poly, then triangle should not intersect
        if (impl.contains(aEnc[1], aEnc[0]) || impl.contains(bEnc[1], bEnc[0]) || impl.contains(cEnc[1], cEnc[0])) {
          assertTrue(impl.relateTriangle(aEnc[1], aEnc[0], bEnc[1], bEnc[0], cEnc[1], cEnc[0]) != Relation.CELL_OUTSIDE_QUERY);
        }
      }
    }
  }

  public void testRelateTriangleContainsPolygon() {
    Polygon polygon = new Polygon(new double[]{0, 0, 1, 1, 0}, new double[]{0, 1, 1, 0, 0});
    Component2D impl = LatLonComponent2DFactory.create(polygon);
    assertEquals(Relation.CELL_CROSSES_QUERY, impl.relateTriangle(GeoEncodingUtils.encodeLongitude(-10) , GeoEncodingUtils.encodeLatitude(-1),
        GeoEncodingUtils.encodeLongitude(2), GeoEncodingUtils.encodeLatitude(-1),
            GeoEncodingUtils.encodeLongitude(10), GeoEncodingUtils.encodeLatitude(10)));
  }

  // test
  public void testRelateTriangleEdgeCases() {
    for (int i = 0; i < 100; ++i) {
      // random radius between 1Km and 100Km
      int randomRadius = RandomNumbers.randomIntBetween(random(), 1000, 100000);
      // random number of vertices
      int numVertices = RandomNumbers.randomIntBetween(random(), 100, 1000);
      Polygon polygon = createRegularPolygon(0, 0, randomRadius, numVertices);
      Component2D impl = LatLonComponent2DFactory.create(polygon);

      // create and test a simple tessellation
      for (int j = 1; j < numVertices; ++j) {
        int[] a = new int[] {0, 0};  // center of poly
        int[] b = new int[] {GeoEncodingUtils.encodeLatitude(polygon.getPolyLat(j - 1)),
                             GeoEncodingUtils.encodeLongitude(polygon.getPolyLon(j - 1))};
        // occassionally test pancake triangles
        int[] c = random().nextBoolean() ? new int[] {GeoEncodingUtils.encodeLatitude(polygon.getPolyLat(j)), GeoEncodingUtils.encodeLongitude(polygon.getPolyLon(j))} : new int[] {a[0], a[1]};
        assertTrue(impl.relateTriangle(a[0], a[1], b[0], b[1], c[0], c[1]) != Relation.CELL_OUTSIDE_QUERY);
      }
    }
  }

  public void testLineCrossingPolygonPoints() {
    Polygon p = new Polygon(new double[] {0, -1, 0, 1, 0}, new double[] {-1, 0, 1, 0, -1});
    Component2D component = LatLonComponent2DFactory.create(p);
    Relation rel = component.relateTriangle(GeoEncodingUtils.encodeLongitude(-1.5),
        GeoEncodingUtils.encodeLatitude(0),
        GeoEncodingUtils.encodeLongitude(1.5),
        GeoEncodingUtils.encodeLatitude(0),
        GeoEncodingUtils.encodeLongitude(-1.5),
        GeoEncodingUtils.encodeLatitude(0));
    assertEquals(Relation.CELL_CROSSES_QUERY, rel);
  }

  public void testRandomLineCrossingPolygon() {
    Polygon p = GeoTestUtil.createRegularPolygon(0, 0, 1000, TestUtil.nextInt(random(), 100, 10000));
    Component2D polygon2D = LatLonComponent2DFactory.create(p);
    for (int i=0; i < 1000; i ++) {
      double longitude = GeoTestUtil.nextLongitude();
      double latitude = GeoTestUtil.nextLatitude();
      Relation rel = polygon2D.relateTriangle(
          GeoEncodingUtils.encodeLongitude(-longitude),
          GeoEncodingUtils.encodeLatitude(-latitude),
          GeoEncodingUtils.encodeLongitude(longitude),
          GeoEncodingUtils.encodeLatitude(latitude),
          GeoEncodingUtils.encodeLongitude(-longitude),
          GeoEncodingUtils.encodeLatitude(-latitude));
      assertNotEquals(Relation.CELL_OUTSIDE_QUERY, rel);
    }
  }

  public void testLUCENE8679() {
    double alat = 1.401298464324817E-45;
    double alon = 24.76789767911785;
    double blat = 34.26468306870807;
    double blon = -52.67048754768767;
    Polygon polygon = new Polygon(new double[] {-14.448264200949083, 0, 0, -14.448264200949083, -14.448264200949083},
        new double[] {0.9999999403953552, 0.9999999403953552, 124.50086371762484, 124.50086371762484, 0.9999999403953552});
    Component2D component = LatLonComponent2DFactory.create(polygon);
    Relation rel = component.relateTriangle(
        GeoEncodingUtils.encodeLongitude(alon), GeoEncodingUtils.encodeLatitude(blat),
        GeoEncodingUtils.encodeLongitude(blon), GeoEncodingUtils.encodeLatitude(blat),
        GeoEncodingUtils.encodeLongitude(alon), GeoEncodingUtils.encodeLatitude(alat));

    assertEquals(Relation.CELL_CROSSES_QUERY, rel);

    rel = component.relateTriangle(
        GeoEncodingUtils.encodeLongitude(alon), GeoEncodingUtils.encodeLatitude(blat),
        GeoEncodingUtils.encodeLongitude(alon), GeoEncodingUtils.encodeLatitude(alat),
        GeoEncodingUtils.encodeLongitude(blon), GeoEncodingUtils.encodeLatitude(blat));

    assertEquals(Relation.CELL_CROSSES_QUERY, rel);
  }

  public void testTriangleTouchingEdges() {
    Polygon p = new Polygon(new double[] {0, 0, 1, 1, 0}, new double[] {0, 1, 1, 0, 0});
    Component2D component = LatLonComponent2DFactory.create(p);
    //3 shared points
    Relation rel = component.relateTriangle(
        GeoEncodingUtils.encodeLongitude(0.5), GeoEncodingUtils.encodeLatitude(0),
        GeoEncodingUtils.encodeLongitude(1), GeoEncodingUtils.encodeLatitude(0.5),
        GeoEncodingUtils.encodeLongitude(0.5), GeoEncodingUtils.encodeLatitude(1));
    assertEquals(Relation.CELL_INSIDE_QUERY, rel);
    //2 shared points
    rel = component.relateTriangle(
        GeoEncodingUtils.encodeLongitude(0.5), GeoEncodingUtils.encodeLatitude(0),
        GeoEncodingUtils.encodeLongitude(1), GeoEncodingUtils.encodeLatitude(0.5),
        GeoEncodingUtils.encodeLongitude(0.5), GeoEncodingUtils.encodeLatitude(0.75));
    assertEquals(Relation.CELL_INSIDE_QUERY, rel);
    //1 shared point
    rel = component.relateTriangle(
        GeoEncodingUtils.encodeLongitude(0.5), GeoEncodingUtils.encodeLatitude(0.5),
        GeoEncodingUtils.encodeLongitude(0.5), GeoEncodingUtils.encodeLatitude(0),
        GeoEncodingUtils.encodeLongitude(0.75), GeoEncodingUtils.encodeLatitude(0.75));
    assertEquals(Relation.CELL_INSIDE_QUERY, rel);
    // 1 shared point but out
    rel = component.relateTriangle(
        GeoEncodingUtils.encodeLongitude(1), GeoEncodingUtils.encodeLatitude(0.5),
        GeoEncodingUtils.encodeLongitude(2), GeoEncodingUtils.encodeLatitude(0),
        GeoEncodingUtils.encodeLongitude(2), GeoEncodingUtils.encodeLatitude(2));
    assertEquals(Relation.CELL_CROSSES_QUERY, rel);
    // 1 shared point but crossing
    rel = component.relateTriangle(
        GeoEncodingUtils.encodeLongitude(0.5), GeoEncodingUtils.encodeLatitude(0),
        GeoEncodingUtils.encodeLongitude(2), GeoEncodingUtils.encodeLatitude(0.5),
        GeoEncodingUtils.encodeLongitude(0.5), GeoEncodingUtils.encodeLatitude(1));
    assertEquals(Relation.CELL_CROSSES_QUERY, rel);
    //share one edge
    rel = component.relateTriangle(
        GeoEncodingUtils.encodeLongitude(0), GeoEncodingUtils.encodeLatitude(0),
        GeoEncodingUtils.encodeLongitude(0), GeoEncodingUtils.encodeLatitude(1),
        GeoEncodingUtils.encodeLongitude(0.5), GeoEncodingUtils.encodeLatitude(0.5));
    assertEquals(Relation.CELL_INSIDE_QUERY, rel);
    //share one edge outside
    rel = component.relateTriangle(
        GeoEncodingUtils.encodeLongitude(0), GeoEncodingUtils.encodeLatitude(1),
        GeoEncodingUtils.encodeLongitude(1.5), GeoEncodingUtils.encodeLatitude(1.5),
        GeoEncodingUtils.encodeLongitude(1), GeoEncodingUtils.encodeLatitude(1));
    assertEquals(Relation.CELL_CROSSES_QUERY, rel);
  }

  public void testTriangleCrossingPolygonVertices() {
    Polygon p = new Polygon(new double[] {0, 0, -5, -10, -5, 0}, new double[] {-1, 1, 5, 0, -5, -1});
    Component2D component = LatLonComponent2DFactory.create(p);
    Relation rel = component.relateTriangle(
        GeoEncodingUtils.encodeLongitude(-5), GeoEncodingUtils.encodeLatitude(0),
        GeoEncodingUtils.encodeLongitude(10), GeoEncodingUtils.encodeLatitude(0),
        GeoEncodingUtils.encodeLongitude(-5), GeoEncodingUtils.encodeLatitude(-15));
    assertEquals(Relation.CELL_CROSSES_QUERY, rel);
  }

  public void testLineCrossingPolygonVertices() {
    Polygon p = new Polygon(new double[] {0, -1, 0, 1, 0}, new double[] {-1, 0, 1, 0, -1});
    Component2D component = LatLonComponent2DFactory.create(p);
    Relation rel = component.relateTriangle(
        GeoEncodingUtils.encodeLongitude(-1.5), GeoEncodingUtils.encodeLatitude(0),
        GeoEncodingUtils.encodeLongitude(1.5), GeoEncodingUtils.encodeLatitude(0),
        GeoEncodingUtils.encodeLongitude(-1.5), GeoEncodingUtils.encodeLatitude(0));
    assertEquals(Relation.CELL_CROSSES_QUERY, rel);
  }
}
