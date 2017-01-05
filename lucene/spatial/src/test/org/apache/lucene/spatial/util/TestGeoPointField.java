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

import org.apache.lucene.spatial.geopoint.document.GeoPointField;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.LuceneTestCase;

import static org.apache.lucene.geo.GeoTestUtil.nextLatitude;
import static org.apache.lucene.geo.GeoTestUtil.nextLongitude;
import static org.apache.lucene.spatial.geopoint.document.GeoPointField.encodeLatLon;
import static org.apache.lucene.spatial.geopoint.document.GeoPointField.geoCodedToPrefixCoded;
import static org.apache.lucene.spatial.geopoint.document.GeoPointField.prefixCodedToGeoCoded;

/**
 * Tests encoding methods in {@link GeoPointField}
 */
public class TestGeoPointField extends LuceneTestCase {
  /**
   * Tests stability of {@link GeoPointField#geoCodedToPrefixCoded}
   */
  public void testGeoPrefixCoding() throws Exception {
    int numIters = atLeast(1000);
    long hash;
    long decodedHash;
    BytesRefBuilder brb = new BytesRefBuilder();
    while (numIters-- >= 0) {
      hash = encodeLatLon(nextLatitude(), nextLongitude());
      for (int i=32; i<64; ++i) {
        geoCodedToPrefixCoded(hash, i, brb);
        decodedHash = prefixCodedToGeoCoded(brb.get());
        assertEquals((hash >>> i) << i, decodedHash);
      }
    }
  }
}
