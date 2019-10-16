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

import org.apache.lucene.geo.Component2D;
import org.apache.lucene.geo.GeoUtils;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.util.LuceneTestCase;

/** base shape encoding class for testing encoding of tessellated {@link org.apache.lucene.document.XYShape} and
 * {@link LatLonShape}
  */
public abstract class BaseShapeEncodingTestCase extends LuceneTestCase {

  protected abstract int encodeX(double x);
  protected abstract double decodeX(int x);
  protected abstract int encodeY(double y);
  protected abstract double decodeY(int y);

  protected abstract double nextX();
  protected abstract double nextY();

  protected abstract Object nextPolygon();
  protected abstract Component2D createPolygon2D(Object polygon);

  //One shared point with MBR -> MinY, MinX
  public void testPolygonEncodingMinLatMinLon() {
    double ay = 0.0;
    double ax = 0.0;
    double by = 1.0;
    double blon = 2.0;
    double cy = 2.0;
    double cx = 1.0;
    int ayEnc = encodeY(ay);
    int axEnc = encodeX(ax);
    int byEnc = encodeY(by);
    int bxEnc = encodeX(blon);
    int cyEnc = encodeY(cy);
    int cxEnc = encodeX(cx);
    verifyEncodingPermutations(ayEnc, axEnc, byEnc, bxEnc, cyEnc, cxEnc);
    byte[] b = new byte[7 * ShapeField.BYTES];
    ShapeField.encodeTriangle(b, ayEnc, axEnc, true, byEnc, bxEnc, true, cyEnc, cxEnc, true);
    ShapeField.DecodedTriangle encoded = new ShapeField.DecodedTriangle();
    ShapeField.decodeTriangle(b, encoded);
    assertEquals(encoded.aY, ayEnc);
    assertEquals(encoded.aX, axEnc);
    assertEquals(encoded.bY, byEnc);
    assertEquals(encoded.bX, bxEnc);
    assertEquals(encoded.cY, cyEnc);
    assertEquals(encoded.cX, cxEnc);
  }

  //One shared point with MBR -> MinLat, MaxLon
  public void testPolygonEncodingMinLatMaxLon() {
    double ay = 1.0;
    double ax = 0.0;
    double by = 0.0;
    double blon = 2.0;
    double cy = 2.0;
    double cx = 1.0;
    int ayEnc = encodeY(ay);
    int axEnc = encodeX(ax);
    int byEnc = encodeY(by);
    int bxEnc = encodeX(blon);
    int cyEnc = encodeY(cy);
    int cxEnc = encodeX(cx);
    verifyEncodingPermutations(ayEnc, axEnc, byEnc, bxEnc, cyEnc, cxEnc);
    byte[] b = new byte[7 * ShapeField.BYTES];
    ShapeField.encodeTriangle(b, ayEnc, axEnc, true, byEnc, bxEnc, true, cyEnc, cxEnc, true);
    ShapeField.DecodedTriangle encoded = new ShapeField.DecodedTriangle();
    ShapeField.decodeTriangle(b, encoded);
    assertEquals(encoded.aY, ayEnc);
    assertEquals(encoded.aX, axEnc);
    assertEquals(encoded.bY, byEnc);
    assertEquals(encoded.bX, bxEnc);
    assertEquals(encoded.cY, cyEnc);
    assertEquals(encoded.cX, cxEnc);
  }

  //One shared point with MBR -> MaxLat, MaxLon
  public void testPolygonEncodingMaxLatMaxLon() {
    double ay = 1.0;
    double ax = 0.0;
    double by = 2.0;
    double blon = 2.0;
    double cy = 0.0;
    double cx = 1.0;
    int ayEnc = encodeY(ay);
    int axEnc = encodeX(ax);
    int byEnc = encodeY(cy);
    int bxEnc = encodeX(cx);
    int cyEnc = encodeY(by);
    int cxEnc = encodeX(blon);
    verifyEncodingPermutations(ayEnc, axEnc, byEnc, bxEnc, cyEnc, cxEnc);
    byte[] b = new byte[7 * ShapeField.BYTES];
    ShapeField.encodeTriangle(b, ayEnc, axEnc, true, byEnc, bxEnc, true, cyEnc, cxEnc, true);
    ShapeField.DecodedTriangle encoded = new ShapeField.DecodedTriangle();
    ShapeField.decodeTriangle(b, encoded);
    assertEquals(encoded.aY, ayEnc);
    assertEquals(encoded.aX, axEnc);
    assertEquals(encoded.bY, byEnc);
    assertEquals(encoded.bX, bxEnc);
    assertEquals(encoded.cY, cyEnc);
    assertEquals(encoded.cX, cxEnc);
  }

  //One shared point with MBR -> MaxLat, MinLon
  public void testPolygonEncodingMaxLatMinLon() {
    double ay = 2.0;
    double ax = 0.0;
    double by = 1.0;
    double blon = 2.0;
    double cy = 0.0;
    double cx = 1.0;
    int ayEnc = encodeY(ay);
    int axEnc = encodeX(ax);
    int byEnc = encodeY(cy);
    int bxEnc = encodeX(cx);
    int cyEnc = encodeY(by);
    int cxEnc = encodeX(blon);
    verifyEncodingPermutations(ayEnc, axEnc, byEnc, bxEnc, cyEnc, cxEnc);
    byte[] b = new byte[7 * ShapeField.BYTES];
    ShapeField.encodeTriangle(b, ayEnc, axEnc, true, byEnc, bxEnc, true, cyEnc, cxEnc, true);
    ShapeField.DecodedTriangle encoded = new ShapeField.DecodedTriangle();
    ShapeField.decodeTriangle(b, encoded);
    assertEquals(encoded.aY, ayEnc);
    assertEquals(encoded.aX, axEnc);
    assertEquals(encoded.bY, byEnc);
    assertEquals(encoded.bX, bxEnc);
    assertEquals(encoded.cY, cyEnc);
    assertEquals(encoded.cX, cxEnc);
  }

  //Two shared point with MBR -> [MinLat, MinLon], [MaxLat, MaxLon], third point below
  public void testPolygonEncodingMinLatMinLonMaxLatMaxLonBelow() {
    double ay = 0.0;
    double ax = 0.0;
    double by = 0.25;
    double blon = 0.75;
    double cy = 2.0;
    double cx = 2.0;
    int ayEnc = encodeY(ay);
    int axEnc = encodeX(ax);
    int byEnc = encodeY(by);
    int bxEnc = encodeX(blon);
    int cyEnc = encodeY(cy);
    int cxEnc = encodeX(cx);
    verifyEncodingPermutations(ayEnc, axEnc, byEnc, bxEnc, cyEnc, cxEnc);
    byte[] b = new byte[7 * ShapeField.BYTES];
    ShapeField.encodeTriangle(b, ayEnc, axEnc, true, byEnc, bxEnc, true, cyEnc, cxEnc, true);
    ShapeField.DecodedTriangle encoded = new ShapeField.DecodedTriangle();
    ShapeField.decodeTriangle(b, encoded);
    assertEquals(encoded.aY, ayEnc);
    assertEquals(encoded.aX, axEnc);
    assertEquals(encoded.bY, byEnc);
    assertEquals(encoded.bX, bxEnc);
    assertEquals(encoded.cY, cyEnc);
    assertEquals(encoded.cX, cxEnc);
  }

  //Two shared point with MBR -> [MinLat, MinLon], [MaxLat, MaxLon], third point above
  public void testPolygonEncodingMinLatMinLonMaxLatMaxLonAbove() {
    double ay = 0.0;
    double ax = 0.0;
    double by = 2.0;
    double bx = 2.0;
    double cy = 1.75;
    double cx = 1.25;
    int ayEnc = encodeY(ay);
    int axEnc = encodeX(ax);
    int byEnc = encodeY(by);
    int bxEnc = encodeX(bx);
    int cyEnc = encodeY(cy);
    int cxEnc = encodeX(cx);
    verifyEncodingPermutations(ayEnc, axEnc, byEnc, bxEnc, cyEnc, cxEnc);
    byte[] b = new byte[7 * ShapeField.BYTES];
    ShapeField.encodeTriangle(b, ayEnc, axEnc, true, byEnc, bxEnc, true, cyEnc, cxEnc, true);
    ShapeField.DecodedTriangle encoded = new ShapeField.DecodedTriangle();
    ShapeField.decodeTriangle(b, encoded);
    assertEquals(encoded.aY, ayEnc);
    assertEquals(encoded.aX, axEnc);
    assertEquals(encoded.bY, byEnc);
    assertEquals(encoded.bX, bxEnc);
    assertEquals(encoded.cY, cyEnc);
    assertEquals(encoded.cX, cxEnc);
  }

  //Two shared point with MBR -> [MinLat, MaxLon], [MaxLat, MinLon], third point below
  public void testPolygonEncodingMinLatMaxLonMaxLatMinLonBelow() {
    double ay = 8.0;
    double ax = 6.0;
    double by = 6.25;
    double bx = 6.75;
    double cy = 6.0;
    double cx = 8.0;
    int ayEnc = encodeY(ay);
    int axEnc = encodeX(ax);
    int byEnc = encodeY(by);
    int bxEnc = encodeX(bx);
    int cyEnc = encodeY(cy);
    int cxEnc = encodeX(cx);
    verifyEncodingPermutations(ayEnc, axEnc, byEnc, bxEnc, cyEnc, cxEnc);
    byte[] b = new byte[7 * ShapeField.BYTES];
    ShapeField.encodeTriangle(b, ayEnc, axEnc, true, byEnc, bxEnc, true, cyEnc, cxEnc, true);
    ShapeField.DecodedTriangle encoded = new ShapeField.DecodedTriangle();
    ShapeField.decodeTriangle(b, encoded);
    assertEquals(encoded.aY, ayEnc);
    assertEquals(encoded.aX, axEnc);
    assertEquals(encoded.bY, byEnc);
    assertEquals(encoded.bX, bxEnc);
    assertEquals(encoded.cY, cyEnc);
    assertEquals(encoded.cX, cxEnc);
  }

  //Two shared point with MBR -> [MinLat, MaxLon], [MaxLat, MinLon], third point above
  public void testPolygonEncodingMinLatMaxLonMaxLatMinLonAbove() {
    double ay = 2.0;
    double ax = 0.0;
    double by = 0.0;
    double bx = 2.0;
    double cy = 1.75;
    double cx = 1.25;
    int ayEnc = encodeY(ay);
    int axEnc = encodeX(ax);
    int byEnc = encodeY(by);
    int bxEnc = encodeX(bx);
    int cyEnc = encodeY(cy);
    int cxEnc = encodeX(cx);
    verifyEncodingPermutations(ayEnc, axEnc, byEnc, bxEnc, cyEnc, cxEnc);
    byte[] b = new byte[7 * ShapeField.BYTES];
    ShapeField.encodeTriangle(b, ayEnc, axEnc, true, byEnc, bxEnc, true, cyEnc, cxEnc, true);
    ShapeField.DecodedTriangle encoded = new ShapeField.DecodedTriangle();
    ShapeField.decodeTriangle(b, encoded);
    assertEquals(encoded.aY, ayEnc);
    assertEquals(encoded.aX, axEnc);
    assertEquals(encoded.bY, byEnc);
    assertEquals(encoded.bX, bxEnc);
    assertEquals(encoded.cY, cyEnc);
    assertEquals(encoded.cX, cxEnc);
  }

  //all points shared with MBR
  public void testPolygonEncodingAllSharedAbove() {
    double ay = 0.0;
    double ax = 0.0;
    double by = 0.0;
    double bx = 2.0;
    double cy = 2.0;
    double cx = 2.0;
    int ayEnc = encodeY(ay);
    int axEnc = encodeX(ax);
    int byEnc = encodeY(by);
    int bxEnc = encodeX(bx);
    int cyEnc = encodeY(cy);
    int cxEnc = encodeX(cx);
    verifyEncodingPermutations(ayEnc, axEnc, byEnc, bxEnc, cyEnc, cxEnc);
    byte[] b = new byte[7 * ShapeField.BYTES];
    ShapeField.encodeTriangle(b, ayEnc, axEnc, true, byEnc, bxEnc, true, cyEnc, cxEnc, true);
    ShapeField.DecodedTriangle encoded = new ShapeField.DecodedTriangle();
    ShapeField.decodeTriangle(b, encoded);
    assertEquals(encoded.aY, ayEnc);
    assertEquals(encoded.aX, axEnc);
    assertEquals(encoded.bY, byEnc);
    assertEquals(encoded.bX, bxEnc);
    assertEquals(encoded.cY, cyEnc);
    assertEquals(encoded.cX, cxEnc);
  }

  //all points shared with MBR
  public void testPolygonEncodingAllSharedBelow() {
    double ay = 2.0;
    double ax = 0.0;
    double by = 0.0;
    double bx = 0.0;
    double cy = 2.0;
    double cx = 2.0;
    int ayEnc = encodeY(ay);
    int axEnc = encodeX(ax);
    int byEnc = encodeY(by);
    int bxEnc = encodeX(bx);
    int cyEnc = encodeY(cy);
    int cxEnc = encodeX(cx);
    byte[] b = new byte[7 * ShapeField.BYTES];
    ShapeField.encodeTriangle(b, ayEnc, axEnc, true, byEnc, bxEnc, true, cyEnc, cxEnc, true);
    ShapeField.DecodedTriangle encoded = new ShapeField.DecodedTriangle();
    ShapeField.decodeTriangle(b, encoded);
    assertEquals(encoded.aY, ayEnc);
    assertEquals(encoded.aX, axEnc);
    assertEquals(encoded.bY, byEnc);
    assertEquals(encoded.bX, bxEnc);
    assertEquals(encoded.cY, cyEnc);
    assertEquals(encoded.cX, cxEnc);
  }

  //[a,b,c] == [c,a,b] == [b,c,a] == [c,b,a] == [b,a,c] == [a,c,b]
  public void verifyEncodingPermutations(int ayEnc, int axEnc, int byEnc, int bxEnc, int cyEnc, int cxEnc) {
    //this is only valid when points are not co-planar
    assertTrue(GeoUtils.orient(ayEnc, axEnc, byEnc, bxEnc, cyEnc, cxEnc) != 0);
    byte[] b = new byte[7 * ShapeField.BYTES];
    //[a,b,c]
    ShapeField.encodeTriangle(b, ayEnc, axEnc, true, byEnc, bxEnc, true, cyEnc, cxEnc, false);
    ShapeField.DecodedTriangle encodedABC = new ShapeField.DecodedTriangle();
    ShapeField.decodeTriangle(b, encodedABC);
    //[c,a,b]
    ShapeField.encodeTriangle(b, cyEnc, cxEnc, false, ayEnc, axEnc, true, byEnc, bxEnc, true);
    ShapeField.DecodedTriangle encodedCAB = new ShapeField.DecodedTriangle();
    ShapeField.decodeTriangle(b, encodedCAB);
    assertEquals(encodedABC, encodedCAB);
    //[b,c,a]
    ShapeField.encodeTriangle(b, byEnc, bxEnc, true, cyEnc, cxEnc, false, ayEnc, axEnc, true);
    ShapeField.DecodedTriangle encodedBCA = new ShapeField.DecodedTriangle();
    ShapeField.decodeTriangle(b, encodedBCA);
    assertEquals(encodedABC, encodedBCA);
    //[c,b,a]
    ShapeField.encodeTriangle(b, cyEnc, cxEnc, true, byEnc, bxEnc, true, ayEnc, axEnc, false);
    ShapeField.DecodedTriangle encodedCBA= new ShapeField.DecodedTriangle();
    ShapeField.decodeTriangle(b, encodedCBA);
    assertEquals(encodedABC, encodedCBA);
    //[b,a,c]
    ShapeField.encodeTriangle(b, byEnc, bxEnc, true, ayEnc, axEnc, false, cyEnc, cxEnc, true);
    ShapeField.DecodedTriangle encodedBAC= new ShapeField.DecodedTriangle();
    ShapeField.decodeTriangle(b, encodedBAC);
    assertEquals(encodedABC, encodedBAC);
    //[a,c,b]
    ShapeField.encodeTriangle(b, ayEnc, axEnc, false, cyEnc, cxEnc, true, byEnc, bxEnc, true);
    ShapeField.DecodedTriangle encodedACB= new ShapeField.DecodedTriangle();
    ShapeField.decodeTriangle(b, encodedACB);
    assertEquals(encodedABC, encodedACB);
  }

  public void testPointEncoding() {
    double lat = 45.0;
    double lon = 45.0;
    int latEnc = encodeY(lat);
    int lonEnc = encodeX(lon);
    byte[] b = new byte[7 * ShapeField.BYTES];
    ShapeField.encodeTriangle(b, latEnc, lonEnc, true, latEnc, lonEnc, true, latEnc, lonEnc, true);
    ShapeField.DecodedTriangle encoded = new ShapeField.DecodedTriangle();
    ShapeField.decodeTriangle(b, encoded);
    assertEquals(encoded.aY, latEnc);
    assertEquals(encoded.aX, lonEnc);
    assertEquals(encoded.bY, latEnc);
    assertEquals(encoded.bX, lonEnc);
    assertEquals(encoded.cY, latEnc);
    assertEquals(encoded.cX, lonEnc);
  }

  public void testLineEncodingSameLat() {
    double lat = 2.0;
    double ax = 0.0;
    double bx = 2.0;
    int latEnc = encodeY(lat);
    int axEnc = encodeX(ax);
    int bxEnc = encodeX(bx);
    byte[] b = new byte[7 * ShapeField.BYTES];
    ShapeField.encodeTriangle(b, latEnc, axEnc, true, latEnc, bxEnc, true, latEnc, axEnc, true);
    ShapeField.DecodedTriangle encoded = new ShapeField.DecodedTriangle();
    ShapeField.decodeTriangle(b, encoded);
    assertEquals(encoded.aY, latEnc);
    assertEquals(encoded.aX, axEnc);
    assertEquals(encoded.bY, latEnc);
    assertEquals(encoded.bX, bxEnc);
    assertEquals(encoded.cY, latEnc);
    assertEquals(encoded.cX, axEnc);
    ShapeField.encodeTriangle(b, latEnc, axEnc, true, latEnc, axEnc, true, latEnc, bxEnc, true);
    ShapeField.decodeTriangle(b, encoded);
    assertEquals(encoded.aY, latEnc);
    assertEquals(encoded.aX, axEnc);
    assertEquals(encoded.bY, latEnc);
    assertEquals(encoded.bX, axEnc);
    assertEquals(encoded.cY, latEnc);
    assertEquals(encoded.cX, bxEnc);
    ShapeField.encodeTriangle(b, latEnc, bxEnc, true, latEnc, axEnc, true, latEnc, axEnc, true);
    ShapeField.decodeTriangle(b, encoded);
    assertEquals(encoded.aY, latEnc);
    assertEquals(encoded.aX, axEnc);
    assertEquals(encoded.bY, latEnc);
    assertEquals(encoded.bX, bxEnc);
    assertEquals(encoded.cY, latEnc);
    assertEquals(encoded.cX, axEnc);
  }

  public void testLineEncodingSameLon() {
    double ay = 0.0;
    double by = 2.0;
    double lon = 2.0;
    int ayEnc = encodeY(ay);
    int byEnc = encodeY(by);
    int lonEnc = encodeX(lon);
    byte[] b = new byte[7 * ShapeField.BYTES];
    ShapeField.encodeTriangle(b, ayEnc, lonEnc, true, byEnc, lonEnc, true, ayEnc, lonEnc, true);
    ShapeField.DecodedTriangle encoded = new ShapeField.DecodedTriangle();
    ShapeField.decodeTriangle(b, encoded);
    assertEquals(encoded.aY, ayEnc);
    assertEquals(encoded.aX, lonEnc);
    assertEquals(encoded.bY, byEnc);
    assertEquals(encoded.bX, lonEnc);
    assertEquals(encoded.cY, ayEnc);
    assertEquals(encoded.cX, lonEnc);
    ShapeField.encodeTriangle(b, ayEnc, lonEnc, true, ayEnc, lonEnc, true, byEnc, lonEnc, true);
    ShapeField.decodeTriangle(b, encoded);
    assertEquals(encoded.aY, ayEnc);
    assertEquals(encoded.aX, lonEnc);
    assertEquals(encoded.bY, ayEnc);
    assertEquals(encoded.bX, lonEnc);
    assertEquals(encoded.cY, byEnc);
    assertEquals(encoded.cX, lonEnc);
    ShapeField.encodeTriangle(b, byEnc, lonEnc, true, ayEnc, lonEnc, true, ayEnc, lonEnc, true);
    ShapeField.decodeTriangle(b, encoded);
    assertEquals(encoded.aY, ayEnc);
    assertEquals(encoded.aX, lonEnc);
    assertEquals(encoded.bY, byEnc);
    assertEquals(encoded.bX, lonEnc);
    assertEquals(encoded.cY, ayEnc);
    assertEquals(encoded.cX, lonEnc);
  }

  public void testLineEncoding() {
    double ay = 0.0;
    double by = 2.0;
    double ax = 0.0;
    double bx = 2.0;
    int ayEnc = encodeY(ay);
    int byEnc = encodeY(by);
    int axEnc = encodeX(ax);
    int bxEnc = encodeX(bx);
    byte[] b = new byte[7 * ShapeField.BYTES];
    ShapeField.encodeTriangle(b, ayEnc, axEnc, true, byEnc, bxEnc, true, ayEnc, axEnc, true);
    ShapeField.DecodedTriangle encoded = new ShapeField.DecodedTriangle();
    ShapeField.decodeTriangle(b, encoded);
    assertEquals(encoded.aY, ayEnc);
    assertEquals(encoded.aX, axEnc);
    assertEquals(encoded.bY, byEnc);
    assertEquals(encoded.bX, bxEnc);
    assertEquals(encoded.cY, ayEnc);
    assertEquals(encoded.cX, axEnc);
    ShapeField.encodeTriangle(b, ayEnc, axEnc, true, ayEnc, axEnc, true, byEnc, bxEnc, true);
    ShapeField.decodeTriangle(b, encoded);
    assertEquals(encoded.aY, ayEnc);
    assertEquals(encoded.aX, axEnc);
    assertEquals(encoded.bY, ayEnc);
    assertEquals(encoded.bX, axEnc);
    assertEquals(encoded.cY, byEnc);
    assertEquals(encoded.cX, bxEnc);
    ShapeField.encodeTriangle(b, byEnc, bxEnc, true, ayEnc, axEnc, true, ayEnc, axEnc, true);
    ShapeField.decodeTriangle(b, encoded);
    assertEquals(encoded.aY, ayEnc);
    assertEquals(encoded.aX, axEnc);
    assertEquals(encoded.bY, byEnc);
    assertEquals(encoded.bX, bxEnc);
    assertEquals(encoded.cY, ayEnc);
    assertEquals(encoded.cX, axEnc);
  }

  public void testRandomPointEncoding() {
    double ay = nextY();
    double ax = nextX();
    verifyEncoding(ay, ax, ay, ax, ay, ax);
  }

  public void testRandomLineEncoding() {
    double ay = nextY();
    double ax = nextX();
    double by = nextY();
    double bx = nextX();
    verifyEncoding(ay, ax, by, bx, ay, ax);
  }

  public void testRandomPolygonEncoding() {
    double ay = nextY();
    double ax = nextX();
    double by = nextY();
    double bx = nextX();
    double cy = nextY();
    double cx = nextX();
    verifyEncoding(ay, ax, by, bx, cy, cx);
  }

  private void verifyEncoding(double ay, double ax, double by, double bx, double cy, double cx) {
    int[] original = new int[]{
        encodeY(ay),
        encodeX(ax),
        encodeY(by),
        encodeX(bx),
        encodeY(cy),
        encodeX(cx)};

    //quantize the triangle
    byte[] b = new byte[7 * ShapeField.BYTES];
    ShapeField.encodeTriangle(b, original[0], original[1], true, original[2], original[3], true, original[4], original[5], true);
    ShapeField.DecodedTriangle encoded = new ShapeField.DecodedTriangle();
    ShapeField.decodeTriangle(b, encoded);
    double[] encodedQuantize = new double[] {
        decodeY(encoded.aY),
        decodeX(encoded.aX),
        decodeY(encoded.bY),
        decodeX(encoded.bX),
        decodeY(encoded.cY),
        decodeX(encoded.cX)};

    int orientation = GeoUtils.orient(original[1], original[0], original[3], original[2], original[5], original[4]);
    //quantize original
    double[] originalQuantize;
    //we need to change the orientation if CW
    if (orientation == -1) {
      originalQuantize = new double[] {
          decodeY(original[4]),
          decodeX(original[5]),
          decodeY(original[2]),
          decodeX(original[3]),
          decodeY(original[0]),
          decodeX(original[1])};
    } else {
      originalQuantize = new double[] {
          decodeY(original[0]),
          decodeX(original[1]),
          decodeY(original[2]),
          decodeX(original[3]),
          decodeY(original[4]),
          decodeX(original[5])};
    }

    for (int i =0; i < 100; i ++) {
      Component2D polygon2D = createPolygon2D(nextPolygon());
      PointValues.Relation originalRelation = polygon2D.relateTriangle(originalQuantize[1], originalQuantize[0], originalQuantize[3], originalQuantize[2], originalQuantize[5], originalQuantize[4]);
      PointValues.Relation encodedRelation = polygon2D.relateTriangle(encodedQuantize[1], encodedQuantize[0], encodedQuantize[3], encodedQuantize[2], encodedQuantize[5], encodedQuantize[4]);
      assertTrue(originalRelation == encodedRelation);
    }
  }

  public void testDegeneratedTriangle() {
    double ay = 1e-26d;
    double ax = 0.0d;
    double by = -1.0d;
    double bx = 0.0d;
    double cy = 1.0d;
    double cx = 0.0d;
    int ayEnc = encodeY(ay);
    int axEnc = encodeX(ax);
    int byEnc = encodeY(by);
    int bxEnc = encodeX(bx);
    int cyEnc = encodeY(cy);
    int cxEnc = encodeX(cx);
    byte[] b = new byte[7 * ShapeField.BYTES];
    ShapeField.encodeTriangle(b, ayEnc, axEnc, true, byEnc, bxEnc, true, cyEnc, cxEnc, true);
    ShapeField.DecodedTriangle encoded = new ShapeField.DecodedTriangle();
    ShapeField.decodeTriangle(b, encoded);
    assertTrue(encoded.aY == byEnc);
    assertTrue(encoded.aX == bxEnc);
    assertTrue(encoded.bY == cyEnc);
    assertTrue(encoded.bX == cxEnc);
    assertTrue(encoded.cY == ayEnc);
    assertTrue(encoded.cX == axEnc);
  }
}
