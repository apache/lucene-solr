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


import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.geo.GeoTestUtil;
import org.apache.lucene.geo.GeoUtils;
import org.apache.lucene.geo.Polygon;
import org.apache.lucene.geo.Polygon2D;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.util.LuceneTestCase;

/** Test case for LatLonShape encoding */
public class TestLatLonShapeEncoding extends LuceneTestCase {

  //One shared point with MBR -> MinLat, MinLon
  public void testPolygonEncodingMinLatMinLon() {
    double alat = 0.0;
    double alon = 0.0;
    double blat = 1.0;
    double blon = 2.0;
    double clat = 2.0;
    double clon = 1.0;
    int alatEnc = GeoEncodingUtils.encodeLatitude(alat);
    int alonEnc = GeoEncodingUtils.encodeLongitude(alon);
    int blatEnc = GeoEncodingUtils.encodeLatitude(blat);
    int blonEnc = GeoEncodingUtils.encodeLongitude(blon);
    int clatEnc = GeoEncodingUtils.encodeLatitude(clat);
    int clonEnc = GeoEncodingUtils.encodeLongitude(clon);
    verifyEncodingPermutations(alatEnc, alonEnc, blatEnc, blonEnc, clatEnc, clonEnc);
    byte[] b = new byte[7 * LatLonShape.BYTES];
    LatLonShape.encodeTriangle(b, alatEnc, alonEnc, blatEnc, blonEnc, clatEnc, clonEnc);
    int[] encoded = new int[6];
    LatLonShape.decodeTriangle(b, encoded);
    assertTrue(encoded[0] == alatEnc);
    assertTrue(encoded[1] == alonEnc);
    assertTrue(encoded[2] == blatEnc);
    assertTrue(encoded[3] == blonEnc);
    assertTrue(encoded[4] == clatEnc);
    assertTrue(encoded[5] == clonEnc);
  }

  //One shared point with MBR -> MinLat, MaxLon
  public void testPolygonEncodingMinLatMaxLon() {
    double alat = 1.0;
    double alon = 0.0;
    double blat = 0.0;
    double blon = 2.0;
    double clat = 2.0;
    double clon = 1.0;
    int alatEnc = GeoEncodingUtils.encodeLatitude(alat);
    int alonEnc = GeoEncodingUtils.encodeLongitude(alon);
    int blatEnc = GeoEncodingUtils.encodeLatitude(blat);
    int blonEnc = GeoEncodingUtils.encodeLongitude(blon);
    int clatEnc = GeoEncodingUtils.encodeLatitude(clat);
    int clonEnc = GeoEncodingUtils.encodeLongitude(clon);
    verifyEncodingPermutations(alatEnc, alonEnc, blatEnc, blonEnc, clatEnc, clonEnc);
    byte[] b = new byte[7 * LatLonShape.BYTES];
    LatLonShape.encodeTriangle(b, alatEnc, alonEnc, blatEnc, blonEnc, clatEnc, clonEnc);
    int[] encoded = new int[6];
    LatLonShape.decodeTriangle(b, encoded);
    assertTrue(encoded[0] == alatEnc);
    assertTrue(encoded[1] == alonEnc);
    assertTrue(encoded[2] == blatEnc);
    assertTrue(encoded[3] == blonEnc);
    assertTrue(encoded[4] == clatEnc);
    assertTrue(encoded[5] == clonEnc);
  }

  //One shared point with MBR -> MaxLat, MaxLon
  public void testPolygonEncodingMaxLatMaxLon() {
    double alat = 1.0;
    double alon = 0.0;
    double blat = 2.0;
    double blon = 2.0;
    double clat = 0.0;
    double clon = 1.0;
    int alatEnc = GeoEncodingUtils.encodeLatitude(alat);
    int alonEnc = GeoEncodingUtils.encodeLongitude(alon);
    int blatEnc = GeoEncodingUtils.encodeLatitude(clat);
    int blonEnc = GeoEncodingUtils.encodeLongitude(clon);
    int clatEnc = GeoEncodingUtils.encodeLatitude(blat);
    int clonEnc = GeoEncodingUtils.encodeLongitude(blon);
    verifyEncodingPermutations(alatEnc, alonEnc, blatEnc, blonEnc, clatEnc, clonEnc);
    byte[] b = new byte[7 * LatLonShape.BYTES];
    LatLonShape.encodeTriangle(b, alatEnc, alonEnc, blatEnc, blonEnc, clatEnc, clonEnc);
    int[] encoded = new int[6];
    LatLonShape.decodeTriangle(b, encoded);
    assertTrue(encoded[0] == alatEnc);
    assertTrue(encoded[1] == alonEnc);
    assertTrue(encoded[2] == blatEnc);
    assertTrue(encoded[3] == blonEnc);
    assertTrue(encoded[4] == clatEnc);
    assertTrue(encoded[5] == clonEnc);
  }

  //One shared point with MBR -> MaxLat, MinLon
  public void testPolygonEncodingMaxLatMinLon() {
    double alat = 2.0;
    double alon = 0.0;
    double blat = 1.0;
    double blon = 2.0;
    double clat = 0.0;
    double clon = 1.0;
    int alatEnc = GeoEncodingUtils.encodeLatitude(alat);
    int alonEnc = GeoEncodingUtils.encodeLongitude(alon);
    int blatEnc = GeoEncodingUtils.encodeLatitude(clat);
    int blonEnc = GeoEncodingUtils.encodeLongitude(clon);
    int clatEnc = GeoEncodingUtils.encodeLatitude(blat);
    int clonEnc = GeoEncodingUtils.encodeLongitude(blon);
    verifyEncodingPermutations(alatEnc, alonEnc, blatEnc, blonEnc, clatEnc, clonEnc);
    byte[] b = new byte[7 * LatLonShape.BYTES];
    LatLonShape.encodeTriangle(b, alatEnc, alonEnc, blatEnc, blonEnc, clatEnc, clonEnc);
    int[] encoded = new int[6];
    LatLonShape.decodeTriangle(b, encoded);
    assertTrue(encoded[0] == alatEnc);
    assertTrue(encoded[1] == alonEnc);
    assertTrue(encoded[2] == blatEnc);
    assertTrue(encoded[3] == blonEnc);
    assertTrue(encoded[4] == clatEnc);
    assertTrue(encoded[5] == clonEnc);
  }

  //Two shared point with MBR -> [MinLat, MinLon], [MaxLat, MaxLon], third point below
  public void testPolygonEncodingMinLatMinLonMaxLatMaxLonBelow() {
    double alat = 0.0;
    double alon = 0.0;
    double blat = 0.25;
    double blon = 0.75;
    double clat = 2.0;
    double clon = 2.0;
    int alatEnc = GeoEncodingUtils.encodeLatitude(alat);
    int alonEnc = GeoEncodingUtils.encodeLongitude(alon);
    int blatEnc = GeoEncodingUtils.encodeLatitude(blat);
    int blonEnc = GeoEncodingUtils.encodeLongitude(blon);
    int clatEnc = GeoEncodingUtils.encodeLatitude(clat);
    int clonEnc = GeoEncodingUtils.encodeLongitude(clon);
    verifyEncodingPermutations(alatEnc, alonEnc, blatEnc, blonEnc, clatEnc, clonEnc);
    byte[] b = new byte[7 * LatLonShape.BYTES];
    LatLonShape.encodeTriangle(b, alatEnc, alonEnc, blatEnc, blonEnc, clatEnc, clonEnc);
    int[] encoded = new int[6];
    LatLonShape.decodeTriangle(b, encoded);
    assertTrue(encoded[0] == alatEnc);
    assertTrue(encoded[1] == alonEnc);
    assertTrue(encoded[2] == blatEnc);
    assertTrue(encoded[3] == blonEnc);
    assertTrue(encoded[4] == clatEnc);
    assertTrue(encoded[5] == clonEnc);
  }

  //Two shared point with MBR -> [MinLat, MinLon], [MaxLat, MaxLon], third point above
  public void testPolygonEncodingMinLatMinLonMaxLatMaxLonAbove() {
    double alat = 0.0;
    double alon = 0.0;
    double blat = 2.0;
    double blon = 2.0;
    double clat = 1.75;
    double clon = 1.25;
    int alatEnc = GeoEncodingUtils.encodeLatitude(alat);
    int alonEnc = GeoEncodingUtils.encodeLongitude(alon);
    int blatEnc = GeoEncodingUtils.encodeLatitude(blat);
    int blonEnc = GeoEncodingUtils.encodeLongitude(blon);
    int clatEnc = GeoEncodingUtils.encodeLatitude(clat);
    int clonEnc = GeoEncodingUtils.encodeLongitude(clon);
    verifyEncodingPermutations(alatEnc, alonEnc, blatEnc, blonEnc, clatEnc, clonEnc);
    byte[] b = new byte[7 * LatLonShape.BYTES];
    LatLonShape.encodeTriangle(b, alatEnc, alonEnc, blatEnc, blonEnc, clatEnc, clonEnc);
    int[] encoded = new int[6];
    LatLonShape.decodeTriangle(b, encoded);
    assertTrue(encoded[0] == alatEnc);
    assertTrue(encoded[1] == alonEnc);
    assertTrue(encoded[2] == blatEnc);
    assertTrue(encoded[3] == blonEnc);
    assertTrue(encoded[4] == clatEnc);
    assertTrue(encoded[5] == clonEnc);
  }

  //Two shared point with MBR -> [MinLat, MaxLon], [MaxLat, MinLon], third point below
  public void testPolygonEncodingMinLatMaxLonMaxLatMinLonBelow() {
    double alat = 2.0;
    double alon = 0.0;
    double blat = 0.25;
    double blon = 0.75;
    double clat = 0.0;
    double clon = 2.0;
    int alatEnc = GeoEncodingUtils.encodeLatitude(alat);
    int alonEnc = GeoEncodingUtils.encodeLongitude(alon);
    int blatEnc = GeoEncodingUtils.encodeLatitude(blat);
    int blonEnc = GeoEncodingUtils.encodeLongitude(blon);
    int clatEnc = GeoEncodingUtils.encodeLatitude(clat);
    int clonEnc = GeoEncodingUtils.encodeLongitude(clon);
    verifyEncodingPermutations(alatEnc, alonEnc, blatEnc, blonEnc, clatEnc, clonEnc);
    byte[] b = new byte[7 * LatLonShape.BYTES];
    LatLonShape.encodeTriangle(b, alatEnc, alonEnc, blatEnc, blonEnc, clatEnc, clonEnc);
    int[] encoded = new int[6];
    LatLonShape.decodeTriangle(b, encoded);
    assertTrue(encoded[0] == alatEnc);
    assertTrue(encoded[1] == alonEnc);
    assertTrue(encoded[2] == blatEnc);
    assertTrue(encoded[3] == blonEnc);
    assertTrue(encoded[4] == clatEnc);
    assertTrue(encoded[5] == clonEnc);
  }

  //Two shared point with MBR -> [MinLat, MaxLon], [MaxLat, MinLon], third point above
  public void testPolygonEncodingMinLatMaxLonMaxLatMinLonAbove() {
    double alat = 2.0;
    double alon = 0.0;
    double blat = 0.0;
    double blon = 2.0;
    double clat = 1.75;
    double clon = 1.25;
    int alatEnc = GeoEncodingUtils.encodeLatitude(alat);
    int alonEnc = GeoEncodingUtils.encodeLongitude(alon);
    int blatEnc = GeoEncodingUtils.encodeLatitude(blat);
    int blonEnc = GeoEncodingUtils.encodeLongitude(blon);
    int clatEnc = GeoEncodingUtils.encodeLatitude(clat);
    int clonEnc = GeoEncodingUtils.encodeLongitude(clon);
    verifyEncodingPermutations(alatEnc, alonEnc, blatEnc, blonEnc, clatEnc, clonEnc);
    byte[] b = new byte[7 * LatLonShape.BYTES];
    LatLonShape.encodeTriangle(b, alatEnc, alonEnc, blatEnc, blonEnc, clatEnc, clonEnc);
    int[] encoded = new int[6];
    LatLonShape.decodeTriangle(b, encoded);
    assertTrue(encoded[0] == alatEnc);
    assertTrue(encoded[1] == alonEnc);
    assertTrue(encoded[2] == blatEnc);
    assertTrue(encoded[3] == blonEnc);
    assertTrue(encoded[4] == clatEnc);
    assertTrue(encoded[5] == clonEnc);
  }

  //all points shared with MBR
  public void testPolygonEncodingAllSharedAbove() {
    double alat = 0.0;
    double alon = 0.0;
    double blat = 0.0;
    double blon = 2.0;
    double clat = 2.0;
    double clon = 2.0;
    int alatEnc = GeoEncodingUtils.encodeLatitude(alat);
    int alonEnc = GeoEncodingUtils.encodeLongitude(alon);
    int blatEnc = GeoEncodingUtils.encodeLatitude(blat);
    int blonEnc = GeoEncodingUtils.encodeLongitude(blon);
    int clatEnc = GeoEncodingUtils.encodeLatitude(clat);
    int clonEnc = GeoEncodingUtils.encodeLongitude(clon);
    verifyEncodingPermutations(alatEnc, alonEnc, blatEnc, blonEnc, clatEnc, clonEnc);
    byte[] b = new byte[7 * LatLonShape.BYTES];
    LatLonShape.encodeTriangle(b, alatEnc, alonEnc, blatEnc, blonEnc, clatEnc, clonEnc);
    int[] encoded = new int[6];
    LatLonShape.decodeTriangle(b, encoded);
    assertTrue(encoded[0] == alatEnc);
    assertTrue(encoded[1] == alonEnc);
    assertTrue(encoded[2] == blatEnc);
    assertTrue(encoded[3] == blonEnc);
    assertTrue(encoded[4] == clatEnc);
    assertTrue(encoded[5] == clonEnc);
  }

  //all points shared with MBR
  public void testPolygonEncodingAllSharedBelow() {
    double alat = 2.0;
    double alon = 0.0;
    double blat = 0.0;
    double blon = 0.0;
    double clat = 2.0;
    double clon = 2.0;
    int alatEnc = GeoEncodingUtils.encodeLatitude(alat);
    int alonEnc = GeoEncodingUtils.encodeLongitude(alon);
    int blatEnc = GeoEncodingUtils.encodeLatitude(blat);
    int blonEnc = GeoEncodingUtils.encodeLongitude(blon);
    int clatEnc = GeoEncodingUtils.encodeLatitude(clat);
    int clonEnc = GeoEncodingUtils.encodeLongitude(clon);
    byte[] b = new byte[7 * LatLonShape.BYTES];
    LatLonShape.encodeTriangle(b, alatEnc, alonEnc, blatEnc, blonEnc, clatEnc, clonEnc);
    int[] encoded = new int[6];
    LatLonShape.decodeTriangle(b, encoded);
    assertTrue(encoded[0] == alatEnc);
    assertTrue(encoded[1] == alonEnc);
    assertTrue(encoded[2] == blatEnc);
    assertTrue(encoded[3] == blonEnc);
    assertTrue(encoded[4] == clatEnc);
    assertTrue(encoded[5] == clonEnc);
  }

  //[a,b,c] == [c,a,b] == [b,c,a] == [c,b,a] == [b,a,c] == [a,c,b]
  public void verifyEncodingPermutations(int alatEnc, int alonEnc, int blatEnc, int blonEnc, int clatEnc, int clonEnc) {
    //this is only valid when points are not co-planar
    assertTrue(GeoUtils.orient(alatEnc, alonEnc, blatEnc, blonEnc, clatEnc, clonEnc) != 0);
    byte[] b = new byte[7 * LatLonShape.BYTES];
    //[a,b,c]
    LatLonShape.encodeTriangle(b, alatEnc, alonEnc, blatEnc, blonEnc, clatEnc, clonEnc);
    int[] encodedABC = new int[6];
    LatLonShape.decodeTriangle(b, encodedABC);
    //[c,a,b]
    LatLonShape.encodeTriangle(b, clatEnc, clonEnc, alatEnc, alonEnc, blatEnc, blonEnc);
    int[] encodedCAB = new int[6];
    LatLonShape.decodeTriangle(b, encodedCAB);
    assertTrue(Arrays.equals(encodedABC, encodedCAB));
    //[b,c,a]
    LatLonShape.encodeTriangle(b, blatEnc, blonEnc, clatEnc, clonEnc, alatEnc, alonEnc);
    int[] encodedBCA = new int[6];
    LatLonShape.decodeTriangle(b, encodedBCA);
    assertTrue(Arrays.equals(encodedABC, encodedBCA));
    //[c,b,a]
    LatLonShape.encodeTriangle(b, clatEnc, clonEnc, blatEnc, blonEnc, alatEnc, alonEnc);
    int[] encodedCBA= new int[6];
    LatLonShape.decodeTriangle(b, encodedCBA);
    assertTrue(Arrays.equals(encodedABC, encodedCBA));
    //[b,a,c]
    LatLonShape.encodeTriangle(b, blatEnc, blonEnc, alatEnc, alonEnc, clatEnc, clonEnc);
    int[] encodedBAC= new int[6];
    LatLonShape.decodeTriangle(b, encodedBAC);
    assertTrue(Arrays.equals(encodedABC, encodedBAC));
    //[a,c,b]
    LatLonShape.encodeTriangle(b, alatEnc, alonEnc, clatEnc, clonEnc, blatEnc, blonEnc);
    int[] encodedACB= new int[6];
    LatLonShape.decodeTriangle(b, encodedACB);
    assertTrue(Arrays.equals(encodedABC, encodedACB));
  }

  public void testPointEncoding() {
    double lat = 45.0;
    double lon = 45.0;
    int latEnc = GeoEncodingUtils.encodeLatitude(lat);
    int lonEnc = GeoEncodingUtils.encodeLongitude(lon);
    byte[] b = new byte[7 * LatLonShape.BYTES];
    LatLonShape.encodeTriangle(b, latEnc, lonEnc, latEnc, lonEnc, latEnc, lonEnc);
    int[] encoded = new int[6];
    LatLonShape.decodeTriangle(b, encoded);
    assertTrue(encoded[0] == latEnc && encoded[2] == latEnc && encoded[4] == latEnc);
    assertTrue(encoded[1] == lonEnc && encoded[3] == lonEnc && encoded[5] == lonEnc);
  }

  public void testLineEncodingSameLat() {
    double lat = 2.0;
    double alon = 0.0;
    double blon = 2.0;
    int latEnc = GeoEncodingUtils.encodeLatitude(lat);
    int alonEnc = GeoEncodingUtils.encodeLongitude(alon);
    int blonEnc = GeoEncodingUtils.encodeLongitude(blon);
    byte[] b = new byte[7 * LatLonShape.BYTES];
    LatLonShape.encodeTriangle(b, latEnc, alonEnc, latEnc, blonEnc, latEnc, alonEnc);
    int[] encoded = new int[6];
    LatLonShape.decodeTriangle(b, encoded);
    assertTrue(encoded[0] == latEnc);
    assertTrue(encoded[1] == alonEnc);
    assertTrue(encoded[2] == latEnc);
    assertTrue(encoded[3] == blonEnc);
    assertTrue(encoded[4] == latEnc);
    assertTrue(encoded[5] == alonEnc);
    LatLonShape.encodeTriangle(b, latEnc, alonEnc, latEnc, alonEnc, latEnc, blonEnc);
    encoded = new int[6];
    LatLonShape.decodeTriangle(b, encoded);
    assertTrue(encoded[0] == latEnc);
    assertTrue(encoded[1] == alonEnc);
    assertTrue(encoded[2] == latEnc);
    assertTrue(encoded[3] == alonEnc);
    assertTrue(encoded[4] == latEnc);
    assertTrue(encoded[5] == blonEnc);
    LatLonShape.encodeTriangle(b, latEnc, blonEnc, latEnc, alonEnc, latEnc, alonEnc);
    encoded = new int[6];
    LatLonShape.decodeTriangle(b, encoded);
    assertTrue(encoded[0] == latEnc);
    assertTrue(encoded[1] == alonEnc);
    assertTrue(encoded[2] == latEnc);
    assertTrue(encoded[3] == blonEnc);
    assertTrue(encoded[4] == latEnc);
    assertTrue(encoded[5] == alonEnc);
  }

  public void testLineEncodingSameLon() {
    double alat = 0.0;
    double blat = 2.0;
    double lon = 2.0;
    int alatEnc = GeoEncodingUtils.encodeLatitude(alat);
    int blatEnc = GeoEncodingUtils.encodeLatitude(blat);
    int lonEnc = GeoEncodingUtils.encodeLongitude(lon);
    byte[] b = new byte[7 * LatLonShape.BYTES];
    LatLonShape.encodeTriangle(b, alatEnc, lonEnc, blatEnc, lonEnc, alatEnc, lonEnc);
    int[] encoded = new int[6];
    LatLonShape.decodeTriangle(b, encoded);
    assertTrue(encoded[0] == alatEnc);
    assertTrue(encoded[1] == lonEnc);
    assertTrue(encoded[2] == blatEnc);
    assertTrue(encoded[3] == lonEnc);
    assertTrue(encoded[4] == alatEnc);
    assertTrue(encoded[5] == lonEnc);
    LatLonShape.encodeTriangle(b, alatEnc, lonEnc, alatEnc, lonEnc, blatEnc, lonEnc);
    encoded = new int[6];
    LatLonShape.decodeTriangle(b, encoded);
    assertTrue(encoded[0] == alatEnc);
    assertTrue(encoded[1] == lonEnc);
    assertTrue(encoded[2] == alatEnc);
    assertTrue(encoded[3] == lonEnc);
    assertTrue(encoded[4] == blatEnc);
    assertTrue(encoded[5] == lonEnc);
    LatLonShape.encodeTriangle(b, blatEnc, lonEnc, alatEnc, lonEnc, alatEnc, lonEnc);
    encoded = new int[6];
    LatLonShape.decodeTriangle(b, encoded);
    assertTrue(encoded[0] == alatEnc);
    assertTrue(encoded[1] == lonEnc);
    assertTrue(encoded[2] == blatEnc);
    assertTrue(encoded[3] == lonEnc);
    assertTrue(encoded[4] == alatEnc);
    assertTrue(encoded[5] == lonEnc);
  }

  public void testLineEncoding() {
    double alat = 0.0;
    double blat = 2.0;
    double alon = 0.0;
    double blon = 2.0;
    int alatEnc = GeoEncodingUtils.encodeLatitude(alat);
    int blatEnc = GeoEncodingUtils.encodeLatitude(blat);
    int alonEnc = GeoEncodingUtils.encodeLongitude(alon);
    int blonEnc = GeoEncodingUtils.encodeLongitude(blon);
    byte[] b = new byte[7 * LatLonShape.BYTES];
    LatLonShape.encodeTriangle(b, alatEnc, alonEnc, blatEnc, blonEnc, alatEnc, alonEnc);
    int[] encoded = new int[6];
    LatLonShape.decodeTriangle(b, encoded);
    assertTrue(encoded[0] == alatEnc);
    assertTrue(encoded[1] == alonEnc);
    assertTrue(encoded[2] == blatEnc);
    assertTrue(encoded[3] == blonEnc);
    assertTrue(encoded[4] == alatEnc);
    assertTrue(encoded[5] == alonEnc);
    LatLonShape.encodeTriangle(b, alatEnc, alonEnc, alatEnc, alonEnc, blatEnc, blonEnc);
    encoded = new int[6];
    LatLonShape.decodeTriangle(b, encoded);
    assertTrue(encoded[0] == alatEnc);
    assertTrue(encoded[1] == alonEnc);
    assertTrue(encoded[2] == alatEnc);
    assertTrue(encoded[3] == alonEnc);
    assertTrue(encoded[4] == blatEnc);
    assertTrue(encoded[5] == blonEnc);
    LatLonShape.encodeTriangle(b, blatEnc, blonEnc, alatEnc, alonEnc, alatEnc, alonEnc);
    encoded = new int[6];
    LatLonShape.decodeTriangle(b, encoded);
    assertTrue(encoded[0] == alatEnc);
    assertTrue(encoded[1] == alonEnc);
    assertTrue(encoded[2] == blatEnc);
    assertTrue(encoded[3] == blonEnc);
    assertTrue(encoded[4] == alatEnc);
    assertTrue(encoded[5] == alonEnc);
  }

  public void testRandomPointEncoding() {
    double alat = GeoTestUtil.nextLatitude();
    double alon = GeoTestUtil.nextLongitude();
    verifyEncoding(alat, alon, alat, alon, alat, alon);
  }

  public void testRandomLineEncoding() {
    double alat = GeoTestUtil.nextLatitude();
    double alon = GeoTestUtil.nextLongitude();
    double blat = GeoTestUtil.nextLatitude();
    double blon = GeoTestUtil.nextLongitude();
    verifyEncoding(alat, alon, blat, blon, alat, alon);
  }

  public void testRandomPolygonEncoding() {
    double alat = GeoTestUtil.nextLatitude();
    double alon = GeoTestUtil.nextLongitude();
    double blat = GeoTestUtil.nextLatitude();
    double blon = GeoTestUtil.nextLongitude();
    double clat = GeoTestUtil.nextLatitude();
    double clon = GeoTestUtil.nextLongitude();
    verifyEncoding(alat, alon, blat, blon, clat, clon);
  }

  private void verifyEncoding(double alat, double alon, double blat, double blon, double clat, double clon) {
    int[] original = new int[]{GeoEncodingUtils.encodeLatitude(alat),
        GeoEncodingUtils.encodeLongitude(alon),
        GeoEncodingUtils.encodeLatitude(blat),
        GeoEncodingUtils.encodeLongitude(blon),
        GeoEncodingUtils.encodeLatitude(clat),
        GeoEncodingUtils.encodeLongitude(clon)};

    //quantize the triangle
    byte[] b = new byte[7 * LatLonShape.BYTES];
    LatLonShape.encodeTriangle(b, original[0], original[1], original[2], original[3], original[4], original[5]);
    int[] encoded = new int[6];
    LatLonShape.decodeTriangle(b, encoded);
    double[] encodedQuantize = new double[] {GeoEncodingUtils.decodeLatitude(encoded[0]),
        GeoEncodingUtils.decodeLongitude(encoded[1]),
        GeoEncodingUtils.decodeLatitude(encoded[2]),
        GeoEncodingUtils.decodeLongitude(encoded[3]),
        GeoEncodingUtils.decodeLatitude(encoded[4]),
        GeoEncodingUtils.decodeLongitude(encoded[5])};

    int orientation = GeoUtils.orient(original[1], original[0], original[3], original[2], original[5], original[4]);
    //quantize original
    double[] originalQuantize;
    //we need to change the orientation if CW
    if (orientation == -1) {
      originalQuantize = new double[] {GeoEncodingUtils.decodeLatitude(original[4]),
          GeoEncodingUtils.decodeLongitude(original[5]),
          GeoEncodingUtils.decodeLatitude(original[2]),
          GeoEncodingUtils.decodeLongitude(original[3]),
          GeoEncodingUtils.decodeLatitude(original[0]),
          GeoEncodingUtils.decodeLongitude(original[1])};
    } else {
      originalQuantize = new double[] {GeoEncodingUtils.decodeLatitude(original[0]),
          GeoEncodingUtils.decodeLongitude(original[1]),
          GeoEncodingUtils.decodeLatitude(original[2]),
          GeoEncodingUtils.decodeLongitude(original[3]),
          GeoEncodingUtils.decodeLatitude(original[4]),
          GeoEncodingUtils.decodeLongitude(original[5])};
    }

    for (int i =0; i < 100; i ++) {
      Polygon polygon = GeoTestUtil.nextPolygon();
      Polygon2D polygon2D = Polygon2D.create(polygon);
      PointValues.Relation originalRelation = polygon2D.relateTriangle(originalQuantize[1], originalQuantize[0], originalQuantize[3], originalQuantize[2], originalQuantize[5], originalQuantize[4]);
      PointValues.Relation encodedRelation = polygon2D.relateTriangle(encodedQuantize[1], encodedQuantize[0], encodedQuantize[3], encodedQuantize[2], encodedQuantize[5], encodedQuantize[4]);
      assertTrue(originalRelation == encodedRelation);
    }
  }

  public void testDegeneratedTriangle() {
    double alat = 1e-26d;
    double alon = 0.0d;
    double blat = -1.0d;
    double blon = 0.0d;
    double clat = 1.0d;
    double clon = 0.0d;
    int alatEnc = GeoEncodingUtils.encodeLatitude(alat);
    int alonEnc = GeoEncodingUtils.encodeLongitude(alon);
    int blatEnc = GeoEncodingUtils.encodeLatitude(blat);
    int blonEnc = GeoEncodingUtils.encodeLongitude(blon);
    int clatEnc = GeoEncodingUtils.encodeLatitude(clat);
    int clonEnc = GeoEncodingUtils.encodeLongitude(clon);
    byte[] b = new byte[7 * LatLonShape.BYTES];
    LatLonShape.encodeTriangle(b, alatEnc, alonEnc, blatEnc, blonEnc, clatEnc, clonEnc);
    int[] encoded = new int[6];
    LatLonShape.decodeTriangle(b, encoded);
    assertTrue(encoded[0] == blatEnc);
    assertTrue(encoded[1] == blonEnc);
    assertTrue(encoded[2] == clatEnc);
    assertTrue(encoded[3] == clonEnc);
    assertTrue(encoded[4] == alatEnc);
    assertTrue(encoded[5] == alonEnc);
  }
}
