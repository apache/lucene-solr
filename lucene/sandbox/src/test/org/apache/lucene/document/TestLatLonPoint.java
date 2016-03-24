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

import org.apache.lucene.util.LuceneTestCase;

/** Simple tests for {@link LatLonPoint} */
public class TestLatLonPoint extends LuceneTestCase {

  public void testToString() throws Exception {
    // looks crazy due to lossiness
    assertEquals("LatLonPoint <field:18.313693958334625,-65.22744392976165>",(new LatLonPoint("field", 18.313694, -65.227444)).toString());
    
    // looks crazy due to lossiness
    assertEquals("field:[17.99999997485429 TO 18.999999999068677],[-65.9999999217689 TO -64.99999998137355]", LatLonPoint.newBoxQuery("field", 18, 19, -66, -65).toString());
    
    // distance query does not quantize inputs
    assertEquals("field:18.0,19.0 +/- 25.0 meters", LatLonPoint.newDistanceQuery("field", 18, 19, 25).toString());
    
    // sort field
    assertEquals("<distance:\"field\" latitude=18.0 longitude=19.0>", LatLonPoint.newDistanceSort("field", 18.0, 19.0).toString());
  }
   
  public void testEncodeDecode() throws Exception {
    // just for testing quantization error
    final double ENCODING_TOLERANCE = 1e-7;

    int iters = atLeast(10000);
    for(int iter=0;iter<iters;iter++) {
      double lat = -90 + 180.0 * random().nextDouble();
      double latEnc = LatLonPoint.decodeLatitude(LatLonPoint.encodeLatitude(lat));
      assertEquals("lat=" + lat + " latEnc=" + latEnc + " diff=" + (lat - latEnc), lat, latEnc, ENCODING_TOLERANCE);

      double lon = -180 + 360.0 * random().nextDouble();
      double lonEnc = LatLonPoint.decodeLongitude(LatLonPoint.encodeLongitude(lon));
      assertEquals("lon=" + lon + " lonEnc=" + lonEnc + " diff=" + (lon - lonEnc), lon, lonEnc, ENCODING_TOLERANCE);
    }

    // check edge/interesting cases explicitly
    assertEquals(0.0, LatLonPoint.decodeLatitude(LatLonPoint.encodeLatitude(0.0)), ENCODING_TOLERANCE);
    assertEquals(90.0, LatLonPoint.decodeLatitude(LatLonPoint.encodeLatitude(90.0)), ENCODING_TOLERANCE);
    assertEquals(-90.0, LatLonPoint.decodeLatitude(LatLonPoint.encodeLatitude(-90.0)), ENCODING_TOLERANCE);

    assertEquals(0.0, LatLonPoint.decodeLongitude(LatLonPoint.encodeLongitude(0.0)), ENCODING_TOLERANCE);
    assertEquals(180.0, LatLonPoint.decodeLongitude(LatLonPoint.encodeLongitude(180.0)), ENCODING_TOLERANCE);
    assertEquals(-180.0, LatLonPoint.decodeLongitude(LatLonPoint.encodeLongitude(-180.0)), ENCODING_TOLERANCE);
  }

  public void testEncodeDecodeExtremeValues() throws Exception {
    assertEquals(Integer.MIN_VALUE, LatLonPoint.encodeLatitude(-90.0));
    assertEquals(0, LatLonPoint.encodeLatitude(0.0));
    assertEquals(Integer.MAX_VALUE, LatLonPoint.encodeLatitude(90.0));

    assertEquals(Integer.MIN_VALUE, LatLonPoint.encodeLongitude(-180.0));
    assertEquals(0, LatLonPoint.encodeLatitude(0.0));
    assertEquals(Integer.MAX_VALUE, LatLonPoint.encodeLongitude(180.0));
  }

  public void testEncodeDecodeIsStable() throws Exception {
    int iters = atLeast(1000);
    for(int iter=0;iter<iters;iter++) {
      double lat = -90 + 180.0 * random().nextDouble();
      double lon = -180 + 360.0 * random().nextDouble();

      double latEnc = LatLonPoint.decodeLatitude(LatLonPoint.encodeLatitude(lat));
      double lonEnc = LatLonPoint.decodeLongitude(LatLonPoint.encodeLongitude(lon));

      double latEnc2 = LatLonPoint.decodeLatitude(LatLonPoint.encodeLatitude(latEnc));
      double lonEnc2 = LatLonPoint.decodeLongitude(LatLonPoint.encodeLongitude(lonEnc));
      assertEquals(latEnc, latEnc2, 0.0);
      assertEquals(lonEnc, lonEnc2, 0.0);
    }
  }   
}
