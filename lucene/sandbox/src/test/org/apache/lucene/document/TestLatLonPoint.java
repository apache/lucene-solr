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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

/** Simple tests for {@link LatLonPoint} */
public class TestLatLonPoint extends LuceneTestCase {

  /** Add a single point and search for it in a box */
  // NOTE: we don't currently supply an exact search, only ranges, because of the lossiness...
  public void testBoxQuery() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

    // add a doc with a point
    Document document = new Document();
    document.add(new LatLonPoint("field", 18.313694, -65.227444));
    writer.addDocument(document);
    
    // search and verify we found our doc
    IndexReader reader = writer.getReader();
    IndexSearcher searcher = newSearcher(reader, false);
    assertEquals(1, searcher.count(LatLonPoint.newBoxQuery("field", 18, 19, -66, -65)));

    reader.close();
    writer.close();
    dir.close();
  }
    
  public void testToString() throws Exception {
    // looks crazy due to lossiness
    assertEquals("LatLonPoint <field:18.313693958334625,-65.22744392976165>",(new LatLonPoint("field", 18.313694, -65.227444)).toString());
    
    // looks crazy due to lossiness
    assertEquals("field:[17.99999997485429 TO 18.999999999068677},[-65.9999999217689 TO -64.99999998137355}", LatLonPoint.newBoxQuery("field", 18, 19, -66, -65).toString());
    
    // distance query does not quantize inputs
    assertEquals("field:18.0,19.0 +/- 25.0 meters", LatLonPoint.newDistanceQuery("field", 18, 19, 25).toString());
  }
  
  /** Valid values that should not cause exception */
  public void testExtremeValues() {
    new LatLonPoint("foo", 90.0, 180.0);
    new LatLonPoint("foo", 90.0, -180.0);
    new LatLonPoint("foo", -90.0, 180.0);
    new LatLonPoint("foo", -90.0, -180.0);
  }
  
  /** Invalid values */
  public void testOutOfRangeValues() {
    IllegalArgumentException expected;

    expected = expectThrows(IllegalArgumentException.class, () -> {
      new LatLonPoint("foo", Math.nextUp(90.0), 50.0);
    });
    assertTrue(expected.getMessage().contains("invalid latitude"));
    
    expected = expectThrows(IllegalArgumentException.class, () -> {
      new LatLonPoint("foo", Math.nextDown(-90.0), 50.0);
    });
    assertTrue(expected.getMessage().contains("invalid latitude"));
    
    expected = expectThrows(IllegalArgumentException.class, () -> {
      new LatLonPoint("foo", 90.0, Math.nextUp(180.0));
    });
    assertTrue(expected.getMessage().contains("invalid longitude"));
    
    expected = expectThrows(IllegalArgumentException.class, () -> {
      new LatLonPoint("foo", 90.0, Math.nextDown(-180.0));
    });
    assertTrue(expected.getMessage().contains("invalid longitude"));
  }
  
  /** NaN: illegal */
  public void testNaNValues() {
    IllegalArgumentException expected;

    expected = expectThrows(IllegalArgumentException.class, () -> {
      new LatLonPoint("foo", Double.NaN, 50.0);
    });
    assertTrue(expected.getMessage().contains("invalid latitude"));
    
    expected = expectThrows(IllegalArgumentException.class, () -> {
      new LatLonPoint("foo", 50.0, Double.NaN);
    });
    assertTrue(expected.getMessage().contains("invalid longitude"));
  }
  
  /** Inf: illegal */
  public void testInfValues() {
    IllegalArgumentException expected;

    expected = expectThrows(IllegalArgumentException.class, () -> {
      new LatLonPoint("foo", Double.POSITIVE_INFINITY, 50.0);
    });
    assertTrue(expected.getMessage().contains("invalid latitude"));
    
    expected = expectThrows(IllegalArgumentException.class, () -> {
      new LatLonPoint("foo", Double.NEGATIVE_INFINITY, 50.0);
    });
    assertTrue(expected.getMessage().contains("invalid latitude"));
    
    expected = expectThrows(IllegalArgumentException.class, () -> {
      new LatLonPoint("foo", 50.0, Double.POSITIVE_INFINITY);
    });
    assertTrue(expected.getMessage().contains("invalid longitude"));
    
    expected = expectThrows(IllegalArgumentException.class, () -> {
      new LatLonPoint("foo", 50.0, Double.NEGATIVE_INFINITY);
    });
    assertTrue(expected.getMessage().contains("invalid longitude"));
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
