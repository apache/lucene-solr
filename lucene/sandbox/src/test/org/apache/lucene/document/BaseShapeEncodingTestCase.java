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

import org.apache.lucene.geo.GeoUtils;
import org.apache.lucene.geo.Polygon2D;
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
  protected abstract Polygon2D createPolygon2D(Object polygon);

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
    ShapeField.encodeTriangle(b, ayEnc, axEnc, byEnc, bxEnc, cyEnc, cxEnc);
    int[] encoded = new int[6];
    ShapeField.decodeTriangle(b, encoded);
    assertTrue(encoded[0] == ayEnc);
    assertTrue(encoded[1] == axEnc);
    assertTrue(encoded[2] == byEnc);
    assertTrue(encoded[3] == bxEnc);
    assertTrue(encoded[4] == cyEnc);
    assertTrue(encoded[5] == cxEnc);
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
    ShapeField.encodeTriangle(b, ayEnc, axEnc, byEnc, bxEnc, cyEnc, cxEnc);
    int[] encoded = new int[6];
    ShapeField.decodeTriangle(b, encoded);
    assertTrue(encoded[0] == ayEnc);
    assertTrue(encoded[1] == axEnc);
    assertTrue(encoded[2] == byEnc);
    assertTrue(encoded[3] == bxEnc);
    assertTrue(encoded[4] == cyEnc);
    assertTrue(encoded[5] == cxEnc);
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
    ShapeField.encodeTriangle(b, ayEnc, axEnc, byEnc, bxEnc, cyEnc, cxEnc);
    int[] encoded = new int[6];
    ShapeField.decodeTriangle(b, encoded);
    assertTrue(encoded[0] == ayEnc);
    assertTrue(encoded[1] == axEnc);
    assertTrue(encoded[2] == byEnc);
    assertTrue(encoded[3] == bxEnc);
    assertTrue(encoded[4] == cyEnc);
    assertTrue(encoded[5] == cxEnc);
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
    ShapeField.encodeTriangle(b, ayEnc, axEnc, byEnc, bxEnc, cyEnc, cxEnc);
    int[] encoded = new int[6];
    ShapeField.decodeTriangle(b, encoded);
    assertTrue(encoded[0] == ayEnc);
    assertTrue(encoded[1] == axEnc);
    assertTrue(encoded[2] == byEnc);
    assertTrue(encoded[3] == bxEnc);
    assertTrue(encoded[4] == cyEnc);
    assertTrue(encoded[5] == cxEnc);
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
    ShapeField.encodeTriangle(b, ayEnc, axEnc, byEnc, bxEnc, cyEnc, cxEnc);
    int[] encoded = new int[6];
    ShapeField.decodeTriangle(b, encoded);
    assertTrue(encoded[0] == ayEnc);
    assertTrue(encoded[1] == axEnc);
    assertTrue(encoded[2] == byEnc);
    assertTrue(encoded[3] == bxEnc);
    assertTrue(encoded[4] == cyEnc);
    assertTrue(encoded[5] == cxEnc);
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
    ShapeField.encodeTriangle(b, ayEnc, axEnc, byEnc, bxEnc, cyEnc, cxEnc);
    int[] encoded = new int[6];
    ShapeField.decodeTriangle(b, encoded);
    assertTrue(encoded[0] == ayEnc);
    assertTrue(encoded[1] == axEnc);
    assertTrue(encoded[2] == byEnc);
    assertTrue(encoded[3] == bxEnc);
    assertTrue(encoded[4] == cyEnc);
    assertTrue(encoded[5] == cxEnc);
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
    ShapeField.encodeTriangle(b, ayEnc, axEnc, byEnc, bxEnc, cyEnc, cxEnc);
    int[] encoded = new int[6];
    ShapeField.decodeTriangle(b, encoded);
    assertTrue(encoded[0] == ayEnc);
    assertTrue(encoded[1] == axEnc);
    assertTrue(encoded[2] == byEnc);
    assertTrue(encoded[3] == bxEnc);
    assertTrue(encoded[4] == cyEnc);
    assertTrue(encoded[5] == cxEnc);
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
    ShapeField.encodeTriangle(b, ayEnc, axEnc, byEnc, bxEnc, cyEnc, cxEnc);
    int[] encoded = new int[6];
    ShapeField.decodeTriangle(b, encoded);
    assertTrue(encoded[0] == ayEnc);
    assertTrue(encoded[1] == axEnc);
    assertTrue(encoded[2] == byEnc);
    assertTrue(encoded[3] == bxEnc);
    assertTrue(encoded[4] == cyEnc);
    assertTrue(encoded[5] == cxEnc);
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
    ShapeField.encodeTriangle(b, ayEnc, axEnc, byEnc, bxEnc, cyEnc, cxEnc);
    int[] encoded = new int[6];
    ShapeField.decodeTriangle(b, encoded);
    assertTrue(encoded[0] == ayEnc);
    assertTrue(encoded[1] == axEnc);
    assertTrue(encoded[2] == byEnc);
    assertTrue(encoded[3] == bxEnc);
    assertTrue(encoded[4] == cyEnc);
    assertTrue(encoded[5] == cxEnc);
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
    ShapeField.encodeTriangle(b, ayEnc, axEnc, byEnc, bxEnc, cyEnc, cxEnc);
    int[] encoded = new int[6];
    ShapeField.decodeTriangle(b, encoded);
    assertTrue(encoded[0] == ayEnc);
    assertTrue(encoded[1] == axEnc);
    assertTrue(encoded[2] == byEnc);
    assertTrue(encoded[3] == bxEnc);
    assertTrue(encoded[4] == cyEnc);
    assertTrue(encoded[5] == cxEnc);
  }

  //[a,b,c] == [c,a,b] == [b,c,a] == [c,b,a] == [b,a,c] == [a,c,b]
  public void verifyEncodingPermutations(int ayEnc, int axEnc, int byEnc, int bxEnc, int cyEnc, int cxEnc) {
    //this is only valid when points are not co-planar
    assertTrue(GeoUtils.orient(ayEnc, axEnc, byEnc, bxEnc, cyEnc, cxEnc) != 0);
    byte[] b = new byte[7 * ShapeField.BYTES];
    //[a,b,c]
    ShapeField.encodeTriangle(b, ayEnc, axEnc, byEnc, bxEnc, cyEnc, cxEnc);
    int[] encodedABC = new int[6];
    ShapeField.decodeTriangle(b, encodedABC);
    //[c,a,b]
    ShapeField.encodeTriangle(b, cyEnc, cxEnc, ayEnc, axEnc, byEnc, bxEnc);
    int[] encodedCAB = new int[6];
    ShapeField.decodeTriangle(b, encodedCAB);
    assertTrue(Arrays.equals(encodedABC, encodedCAB));
    //[b,c,a]
    ShapeField.encodeTriangle(b, byEnc, bxEnc, cyEnc, cxEnc, ayEnc, axEnc);
    int[] encodedBCA = new int[6];
    ShapeField.decodeTriangle(b, encodedBCA);
    assertTrue(Arrays.equals(encodedABC, encodedBCA));
    //[c,b,a]
    ShapeField.encodeTriangle(b, cyEnc, cxEnc, byEnc, bxEnc, ayEnc, axEnc);
    int[] encodedCBA= new int[6];
    ShapeField.decodeTriangle(b, encodedCBA);
    assertTrue(Arrays.equals(encodedABC, encodedCBA));
    //[b,a,c]
    ShapeField.encodeTriangle(b, byEnc, bxEnc, ayEnc, axEnc, cyEnc, cxEnc);
    int[] encodedBAC= new int[6];
    ShapeField.decodeTriangle(b, encodedBAC);
    assertTrue(Arrays.equals(encodedABC, encodedBAC));
    //[a,c,b]
    ShapeField.encodeTriangle(b, ayEnc, axEnc, cyEnc, cxEnc, byEnc, bxEnc);
    int[] encodedACB= new int[6];
    ShapeField.decodeTriangle(b, encodedACB);
    assertTrue(Arrays.equals(encodedABC, encodedACB));
  }

  public void testPointEncoding() {
    double lat = 45.0;
    double lon = 45.0;
    int latEnc = encodeY(lat);
    int lonEnc = encodeX(lon);
    byte[] b = new byte[7 * ShapeField.BYTES];
    ShapeField.encodeTriangle(b, latEnc, lonEnc, latEnc, lonEnc, latEnc, lonEnc);
    int[] encoded = new int[6];
    ShapeField.decodeTriangle(b, encoded);
    assertTrue(encoded[0] == latEnc && encoded[2] == latEnc && encoded[4] == latEnc);
    assertTrue(encoded[1] == lonEnc && encoded[3] == lonEnc && encoded[5] == lonEnc);
  }

  public void testLineEncodingSameLat() {
    double lat = 2.0;
    double ax = 0.0;
    double bx = 2.0;
    int latEnc = encodeY(lat);
    int axEnc = encodeX(ax);
    int bxEnc = encodeX(bx);
    byte[] b = new byte[7 * ShapeField.BYTES];
    ShapeField.encodeTriangle(b, latEnc, axEnc, latEnc, bxEnc, latEnc, axEnc);
    int[] encoded = new int[6];
    ShapeField.decodeTriangle(b, encoded);
    assertTrue(encoded[0] == latEnc);
    assertTrue(encoded[1] == axEnc);
    assertTrue(encoded[2] == latEnc);
    assertTrue(encoded[3] == bxEnc);
    assertTrue(encoded[4] == latEnc);
    assertTrue(encoded[5] == axEnc);
    ShapeField.encodeTriangle(b, latEnc, axEnc, latEnc, axEnc, latEnc, bxEnc);
    encoded = new int[6];
    ShapeField.decodeTriangle(b, encoded);
    assertTrue(encoded[0] == latEnc);
    assertTrue(encoded[1] == axEnc);
    assertTrue(encoded[2] == latEnc);
    assertTrue(encoded[3] == axEnc);
    assertTrue(encoded[4] == latEnc);
    assertTrue(encoded[5] == bxEnc);
    ShapeField.encodeTriangle(b, latEnc, bxEnc, latEnc, axEnc, latEnc, axEnc);
    encoded = new int[6];
    ShapeField.decodeTriangle(b, encoded);
    assertTrue(encoded[0] == latEnc);
    assertTrue(encoded[1] == axEnc);
    assertTrue(encoded[2] == latEnc);
    assertTrue(encoded[3] == bxEnc);
    assertTrue(encoded[4] == latEnc);
    assertTrue(encoded[5] == axEnc);
  }

  public void testLineEncodingSameLon() {
    double ay = 0.0;
    double by = 2.0;
    double lon = 2.0;
    int ayEnc = encodeY(ay);
    int byEnc = encodeY(by);
    int lonEnc = encodeX(lon);
    byte[] b = new byte[7 * ShapeField.BYTES];
    ShapeField.encodeTriangle(b, ayEnc, lonEnc, byEnc, lonEnc, ayEnc, lonEnc);
    int[] encoded = new int[6];
    ShapeField.decodeTriangle(b, encoded);
    assertTrue(encoded[0] == ayEnc);
    assertTrue(encoded[1] == lonEnc);
    assertTrue(encoded[2] == byEnc);
    assertTrue(encoded[3] == lonEnc);
    assertTrue(encoded[4] == ayEnc);
    assertTrue(encoded[5] == lonEnc);
    ShapeField.encodeTriangle(b, ayEnc, lonEnc, ayEnc, lonEnc, byEnc, lonEnc);
    encoded = new int[6];
    ShapeField.decodeTriangle(b, encoded);
    assertTrue(encoded[0] == ayEnc);
    assertTrue(encoded[1] == lonEnc);
    assertTrue(encoded[2] == ayEnc);
    assertTrue(encoded[3] == lonEnc);
    assertTrue(encoded[4] == byEnc);
    assertTrue(encoded[5] == lonEnc);
    ShapeField.encodeTriangle(b, byEnc, lonEnc, ayEnc, lonEnc, ayEnc, lonEnc);
    encoded = new int[6];
    ShapeField.decodeTriangle(b, encoded);
    assertTrue(encoded[0] == ayEnc);
    assertTrue(encoded[1] == lonEnc);
    assertTrue(encoded[2] == byEnc);
    assertTrue(encoded[3] == lonEnc);
    assertTrue(encoded[4] == ayEnc);
    assertTrue(encoded[5] == lonEnc);
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
    ShapeField.encodeTriangle(b, ayEnc, axEnc, byEnc, bxEnc, ayEnc, axEnc);
    int[] encoded = new int[6];
    ShapeField.decodeTriangle(b, encoded);
    assertTrue(encoded[0] == ayEnc);
    assertTrue(encoded[1] == axEnc);
    assertTrue(encoded[2] == byEnc);
    assertTrue(encoded[3] == bxEnc);
    assertTrue(encoded[4] == ayEnc);
    assertTrue(encoded[5] == axEnc);
    ShapeField.encodeTriangle(b, ayEnc, axEnc, ayEnc, axEnc, byEnc, bxEnc);
    encoded = new int[6];
    ShapeField.decodeTriangle(b, encoded);
    assertTrue(encoded[0] == ayEnc);
    assertTrue(encoded[1] == axEnc);
    assertTrue(encoded[2] == ayEnc);
    assertTrue(encoded[3] == axEnc);
    assertTrue(encoded[4] == byEnc);
    assertTrue(encoded[5] == bxEnc);
    ShapeField.encodeTriangle(b, byEnc, bxEnc, ayEnc, axEnc, ayEnc, axEnc);
    encoded = new int[6];
    ShapeField.decodeTriangle(b, encoded);
    assertTrue(encoded[0] == ayEnc);
    assertTrue(encoded[1] == axEnc);
    assertTrue(encoded[2] == byEnc);
    assertTrue(encoded[3] == bxEnc);
    assertTrue(encoded[4] == ayEnc);
    assertTrue(encoded[5] == axEnc);
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
    ShapeField.encodeTriangle(b, original[0], original[1], original[2], original[3], original[4], original[5]);
    int[] encoded = new int[6];
    ShapeField.decodeTriangle(b, encoded);
    double[] encodedQuantize = new double[] {
        decodeY(encoded[0]),
        decodeX(encoded[1]),
        decodeY(encoded[2]),
        decodeX(encoded[3]),
        decodeY(encoded[4]),
        decodeX(encoded[5])};

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
      Polygon2D polygon2D = createPolygon2D(nextPolygon());
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
    ShapeField.encodeTriangle(b, ayEnc, axEnc, byEnc, bxEnc, cyEnc, cxEnc);
    int[] encoded = new int[6];
    ShapeField.decodeTriangle(b, encoded);
    assertTrue(encoded[0] == byEnc);
    assertTrue(encoded[1] == bxEnc);
    assertTrue(encoded[2] == cyEnc);
    assertTrue(encoded[3] == cxEnc);
    assertTrue(encoded[4] == ayEnc);
    assertTrue(encoded[5] == axEnc);
  }
}
