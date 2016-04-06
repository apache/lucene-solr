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

import java.util.Random;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.TestUtil;

/** Simple tests for {@link LatLonPoint} */
public class TestLatLonPoint extends LuceneTestCase {

  public void testToString() throws Exception {
    // looks crazy due to lossiness
    assertEquals("LatLonPoint <field:18.313693958334625,-65.22744401358068>",(new LatLonPoint("field", 18.313694, -65.227444)).toString());
    
    // looks crazy due to lossiness
    assertEquals("field:[18.000000016763806 TO 18.999999999068677],[-65.9999999217689 TO -65.00000006519258]", LatLonPoint.newBoxQuery("field", 18, 19, -66, -65).toString());
    
    // distance query does not quantize inputs
    assertEquals("field:18.0,19.0 +/- 25.0 meters", LatLonPoint.newDistanceQuery("field", 18, 19, 25).toString());
    
    // sort field
    assertEquals("<distance:\"field\" latitude=18.0 longitude=19.0>", LatLonPoint.newDistanceSort("field", 18.0, 19.0).toString());
  }

  /**
   * step through some integers, ensuring they decode to their expected double values.
   * double values start at -90 and increase by LATITUDE_DECODE for each integer.
   * check edge cases within the double range and random doubles within the range too.
   */
  public void testLatitudeQuantization() throws Exception {
    Random random = random();
    for (int i = 0; i < 10000; i++) {
      int encoded = random.nextInt();
      double min = -90.0 + (encoded - (long)Integer.MIN_VALUE) * LatLonPoint.LATITUDE_DECODE;
      double decoded = LatLonPoint.decodeLatitude(encoded);
      // should exactly equal expected value
      assertEquals(min, decoded, 0.0D);
      // should round-trip
      assertEquals(encoded, LatLonPoint.encodeLatitude(decoded));
      assertEquals(encoded, LatLonPoint.encodeLatitudeCeil(decoded));
      // test within the range
      if (i != Integer.MAX_VALUE) {
        // this is the next representable value
        // all double values between [min .. max) should encode to the current integer
        // all double values between (min .. max] should encodeCeil to the next integer.
        double max = min + LatLonPoint.LATITUDE_DECODE;
        assertEquals(max, LatLonPoint.decodeLatitude(encoded+1), 0.0D);
        assertEquals(encoded+1, LatLonPoint.encodeLatitude(max));
        assertEquals(encoded+1, LatLonPoint.encodeLatitudeCeil(max));

        // first and last doubles in range that will be quantized
        double minEdge = Math.nextUp(min);
        double maxEdge = Math.nextDown(max);
        assertEquals(encoded,   LatLonPoint.encodeLatitude(minEdge));
        assertEquals(encoded+1, LatLonPoint.encodeLatitudeCeil(minEdge));
        assertEquals(encoded,   LatLonPoint.encodeLatitude(maxEdge));
        assertEquals(encoded+1, LatLonPoint.encodeLatitudeCeil(maxEdge));
        
        // check random values within the double range
        long minBits = NumericUtils.doubleToSortableLong(minEdge);
        long maxBits = NumericUtils.doubleToSortableLong(maxEdge);
        for (int j = 0; j < 100; j++) {
          double value = NumericUtils.sortableLongToDouble(TestUtil.nextLong(random, minBits, maxBits));
          // round down
          assertEquals(encoded,   LatLonPoint.encodeLatitude(value));
          // round up
          assertEquals(encoded+1, LatLonPoint.encodeLatitudeCeil(value));
        }
      }
    }
  }

  /** 
   * step through some integers, ensuring they decode to their expected double values.
   * double values start at -180 and increase by LONGITUDE_DECODE for each integer.
   * check edge cases within the double range and a random doubles within the range too.
   */
  public void testLongitudeQuantization() throws Exception {
    Random random = random();
    for (int i = 0; i < 10000; i++) {
      int encoded = random.nextInt();
      double min = -180.0 + (encoded - (long)Integer.MIN_VALUE) * LatLonPoint.LONGITUDE_DECODE;
      double decoded = LatLonPoint.decodeLongitude(encoded);
      // should exactly equal expected value
      assertEquals(min, decoded, 0.0D);
      // should round-trip
      assertEquals(encoded, LatLonPoint.encodeLongitude(decoded));
      assertEquals(encoded, LatLonPoint.encodeLongitudeCeil(decoded));
      // test within the range
      if (i != Integer.MAX_VALUE) {
        // this is the next representable value
        // all double values between [min .. max) should encode to the current integer
        // all double values between (min .. max] should encodeCeil to the next integer.
        double max = min + LatLonPoint.LONGITUDE_DECODE;
        assertEquals(max, LatLonPoint.decodeLongitude(encoded+1), 0.0D);
        assertEquals(encoded+1, LatLonPoint.encodeLongitude(max));
        assertEquals(encoded+1, LatLonPoint.encodeLongitudeCeil(max));

        // first and last doubles in range that will be quantized
        double minEdge = Math.nextUp(min);
        double maxEdge = Math.nextDown(max);
        assertEquals(encoded,   LatLonPoint.encodeLongitude(minEdge));
        assertEquals(encoded+1, LatLonPoint.encodeLongitudeCeil(minEdge));
        assertEquals(encoded,   LatLonPoint.encodeLongitude(maxEdge));
        assertEquals(encoded+1, LatLonPoint.encodeLongitudeCeil(maxEdge));
        
        // check random values within the double range
        long minBits = NumericUtils.doubleToSortableLong(minEdge);
        long maxBits = NumericUtils.doubleToSortableLong(maxEdge);
        for (int j = 0; j < 100; j++) {
          double value = NumericUtils.sortableLongToDouble(TestUtil.nextLong(random, minBits, maxBits));
          // round down
          assertEquals(encoded,   LatLonPoint.encodeLongitude(value));
          // round up
          assertEquals(encoded+1, LatLonPoint.encodeLongitudeCeil(value));
        }
      }
    }
  }

  // check edge/interesting cases explicitly
  public void testEncodeEdgeCases() {
    assertEquals(Integer.MIN_VALUE, LatLonPoint.encodeLatitude(-90.0));
    assertEquals(Integer.MIN_VALUE, LatLonPoint.encodeLatitudeCeil(-90.0));
    assertEquals(Integer.MAX_VALUE, LatLonPoint.encodeLatitude(90.0));
    assertEquals(Integer.MAX_VALUE, LatLonPoint.encodeLatitudeCeil(90.0));
    
    assertEquals(Integer.MIN_VALUE, LatLonPoint.encodeLongitude(-180.0));
    assertEquals(Integer.MIN_VALUE, LatLonPoint.encodeLongitudeCeil(-180.0));
    assertEquals(Integer.MAX_VALUE, LatLonPoint.encodeLongitude(180.0));
    assertEquals(Integer.MAX_VALUE, LatLonPoint.encodeLongitudeCeil(180.0));
  }
}
