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
package org.apache.lucene.spatial.util;

import java.util.Random;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.TestUtil;

import static org.apache.lucene.geo.GeoEncodingUtils.decodeLatitude;
import static org.apache.lucene.geo.GeoEncodingUtils.decodeLongitude;
import static org.apache.lucene.spatial.util.MortonEncoder.decodeLatitude;
import static org.apache.lucene.spatial.util.MortonEncoder.decodeLongitude;
import static org.apache.lucene.spatial.util.MortonEncoder.encode;
import static org.apache.lucene.spatial.util.MortonEncoder.encodeCeil;

import static org.apache.lucene.util.BitUtil.deinterleave;
import static org.apache.lucene.util.BitUtil.interleave;

/**
 * Tests methods in {@link MortonEncoder}
 */
public class TestMortonEncoder extends LuceneTestCase {

  public void testMortonEncoding() throws Exception {
    final long TRANSLATE = 1L << 31;
    final double LATITUDE_DECODE = 180.0D/(0x1L<<32);
    final double LONGITUDE_DECODE = 360.0D/(0x1L<<32);
    Random random = random();
    for(int i=0; i < 10000; ++i) {
      long encoded = random().nextLong();
      long encodedLat = deinterleave(encoded >>> 1);
      long encodedLon = deinterleave(encoded);
      double expectedLat = decodeLatitude((int)(encodedLat - TRANSLATE));
      double decodedLat = decodeLatitude(encoded);
      double expectedLon = decodeLongitude((int)(encodedLon - TRANSLATE));
      double decodedLon = decodeLongitude(encoded);
      assertEquals(expectedLat, decodedLat, 0.0D);
      assertEquals(expectedLon, decodedLon, 0.0D);
      // should round-trip
      assertEquals(encoded, encode(decodedLat, decodedLon));

      // test within the range
      if (encoded != 0xFFFFFFFFFFFFFFFFL) {
        // this is the next representable value
        // all double values between [min .. max) should encode to the current integer
        // all double values between (min .. max] should encodeCeil to the next integer.
        double maxLat = expectedLat + LATITUDE_DECODE;
        encodedLat += 1;
        assertEquals(maxLat, decodeLatitude((int)(encodedLat - TRANSLATE)), 0.0D);
        double maxLon = expectedLon + LONGITUDE_DECODE;
        encodedLon += 1;
        assertEquals(maxLon, decodeLongitude((int)(encodedLon - TRANSLATE)), 0.0D);
        long encodedNext = encode(maxLat, maxLon);
        assertEquals(interleave((int)encodedLon, (int)encodedLat), encodedNext);

        // first and last doubles in range that will be quantized
        double minEdgeLat = Math.nextUp(expectedLat);
        double minEdgeLon = Math.nextUp(expectedLon);
        long encodedMinEdge = encode(minEdgeLat, minEdgeLon);
        long encodedMinEdgeCeil = encodeCeil(minEdgeLat, minEdgeLon);
        double maxEdgeLat = Math.nextDown(maxLat);
        double maxEdgeLon = Math.nextDown(maxLon);
        long encodedMaxEdge = encode(maxEdgeLat, maxEdgeLon);
        long encodedMaxEdgeCeil = encodeCeil(maxEdgeLat, maxEdgeLon);

        assertEquals(encodedLat - 1, deinterleave(encodedMinEdge >>> 1));
        assertEquals(encodedLat, deinterleave(encodedMinEdgeCeil >>> 1));
        assertEquals(encodedLon - 1, deinterleave(encodedMinEdge));
        assertEquals(encodedLon, deinterleave(encodedMinEdgeCeil));

        assertEquals(encodedLat - 1, deinterleave(encodedMaxEdge >>> 1));
        assertEquals(encodedLat, deinterleave(encodedMaxEdgeCeil >>> 1));
        assertEquals(encodedLon - 1, deinterleave(encodedMaxEdge));
        assertEquals(encodedLon, deinterleave(encodedMaxEdgeCeil));

        // check random values within the double range
        long minBitsLat = NumericUtils.doubleToSortableLong(minEdgeLat);
        long maxBitsLat = NumericUtils.doubleToSortableLong(maxEdgeLat);
        long minBitsLon = NumericUtils.doubleToSortableLong(minEdgeLon);
        long maxBitsLon = NumericUtils.doubleToSortableLong(maxEdgeLon);
        for (int j = 0; j < 100; j++) {
          double valueLat = NumericUtils.sortableLongToDouble(TestUtil.nextLong(random, minBitsLat, maxBitsLat));
          double valueLon = NumericUtils.sortableLongToDouble(TestUtil.nextLong(random, minBitsLon, maxBitsLon));
          // round down
          assertEquals(encoded,   encode(valueLat, valueLon));
          // round up
          assertEquals(interleave((int)encodedLon, (int)encodedLat), encodeCeil(valueLat, valueLon));
        }
      }
    }
  }
}
