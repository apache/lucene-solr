package org.apache.lucene.spatial.util;

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

import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.LuceneTestCase;

import static org.apache.lucene.geo.GeoTestUtil.nextLatitude;
import static org.apache.lucene.geo.GeoTestUtil.nextLongitude;

/**
 * Tests methods in {@link GeoEncodingUtils}
 */
public class TestGeoEncodingUtils extends LuceneTestCase {
  /**
   * Tests stability of {@link GeoEncodingUtils#geoCodedToPrefixCoded}
   */
  public void testGeoPrefixCoding() throws Exception {
    int numIters = atLeast(1000);
    long hash;
    long decodedHash;
    BytesRefBuilder brb = new BytesRefBuilder();
    while (numIters-- >= 0) {
      hash = GeoEncodingUtils.mortonHash(nextLatitude(), nextLongitude());
      for (int i=32; i<64; ++i) {
        GeoEncodingUtils.geoCodedToPrefixCoded(hash, i, brb);
        decodedHash = GeoEncodingUtils.prefixCodedToGeoCoded(brb.get());
        assertEquals((hash >>> i) << i, decodedHash);
      }
    }
  }

  public void testMortonEncoding() throws Exception {
    long hash = GeoEncodingUtils.mortonHash(90, 180);
    assertEquals(180.0, GeoEncodingUtils.mortonUnhashLon(hash), 0);
    assertEquals(90.0, GeoEncodingUtils.mortonUnhashLat(hash), 0);
  }

  public void testEncodeDecode() throws Exception {
    int iters = atLeast(10000);
    for(int iter=0;iter<iters;iter++) {
      double lat = nextLatitude();
      double lon = nextLongitude();

      long enc = GeoEncodingUtils.mortonHash(lat, lon);
      double latEnc = GeoEncodingUtils.mortonUnhashLat(enc);
      double lonEnc = GeoEncodingUtils.mortonUnhashLon(enc);

      assertEquals("lat=" + lat + " latEnc=" + latEnc + " diff=" + (lat - latEnc), lat, latEnc, GeoEncodingUtils.TOLERANCE);
      assertEquals("lon=" + lon + " lonEnc=" + lonEnc + " diff=" + (lon - lonEnc), lon, lonEnc, GeoEncodingUtils.TOLERANCE);
    }
  }

  /** make sure values always go down: this is important for edge case consistency */
  public void testEncodeDecodeRoundsDown() throws Exception {
    int iters = atLeast(1000);
    for(int iter=0;iter<iters;iter++) {
      double lat = -90 + 180.0 * random().nextDouble();
      double lon = -180 + 360.0 * random().nextDouble();

      long enc = GeoEncodingUtils.mortonHash(lat, lon);
      double latEnc = GeoEncodingUtils.mortonUnhashLat(enc);
      double lonEnc = GeoEncodingUtils.mortonUnhashLon(enc);
      assertTrue(latEnc <= lat);
      assertTrue(lonEnc <= lon);
    }
  }

  public void testScaleUnscaleIsStable() throws Exception {
    int iters = atLeast(1000);
    for(int iter=0;iter<iters;iter++) {
      double lat = nextLatitude();
      double lon = nextLongitude();

      long enc = GeoEncodingUtils.mortonHash(lat, lon);
      double latEnc = GeoEncodingUtils.mortonUnhashLat(enc);
      double lonEnc = GeoEncodingUtils.mortonUnhashLon(enc);

      long enc2 = GeoEncodingUtils.mortonHash(lat, lon);
      double latEnc2 = GeoEncodingUtils.mortonUnhashLat(enc2);
      double lonEnc2 = GeoEncodingUtils.mortonUnhashLon(enc2);
      assertEquals(latEnc, latEnc2, 0.0);
      assertEquals(lonEnc, lonEnc2, 0.0);
    }
  }
}
