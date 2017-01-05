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
package org.apache.lucene.spatial3d;

import org.apache.lucene.spatial3d.geom.GeoPoint;
import org.apache.lucene.spatial3d.geom.PlanetModel;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

public class TestGeo3DDocValues extends LuceneTestCase {

  public void testBasic() throws Exception {
    checkPointEncoding(0.0, 0.0);
    checkPointEncoding(45.0, 72.0);
    checkPointEncoding(-45.0, -100.0);
    final int testAmt = TestUtil.nextInt(random(), 1000, 2000);
    for (int i = 0; i < testAmt; i++) {
      checkPointEncoding(random().nextDouble() * 180.0 - 90.0, random().nextDouble() * 360.0 - 180.0);
    }
  }
    
  void checkPointEncoding(final double latitude, final double longitude) {
    final GeoPoint point = new GeoPoint(PlanetModel.WGS84, Geo3DUtil.fromDegrees(latitude), Geo3DUtil.fromDegrees(longitude));
    long pointValue = Geo3DDocValuesField.encodePoint(point);
    final double x = Geo3DDocValuesField.decodeXValue(pointValue);
    final double y = Geo3DDocValuesField.decodeYValue(pointValue);
    final double z = Geo3DDocValuesField.decodeZValue(pointValue);
    final GeoPoint pointR = new GeoPoint(x,y,z);
    // Check whether stable
    pointValue = Geo3DDocValuesField.encodePoint(x, y, z);
    assertEquals(x, Geo3DDocValuesField.decodeXValue(pointValue), 0.0);
    assertEquals(y, Geo3DDocValuesField.decodeYValue(pointValue), 0.0);
    assertEquals(z, Geo3DDocValuesField.decodeZValue(pointValue), 0.0);
    // Check whether has some relationship with original point
    assertEquals(0.0, point.arcDistance(pointR), 0.02);
  }
  
}
