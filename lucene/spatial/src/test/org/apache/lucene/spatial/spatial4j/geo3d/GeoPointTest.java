package org.apache.lucene.spatial.spatial4j.geo3d;

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

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Test basic GeoPoint functionality.
 */
public class GeoPointTest {

  @Test
  public void testConversion() {
    final double pLat = 0.123;
    final double pLon = -0.456;
    final GeoPoint p1 = new GeoPoint(PlanetModel.SPHERE, pLat, pLon);
    assertEquals(pLat, p1.getLatitude(), 1e-12);
    assertEquals(pLon, p1.getLongitude(), 1e-12);
    final GeoPoint p2 = new GeoPoint(PlanetModel.WGS84, pLat, pLon);
    assertEquals(pLat, p2.getLatitude(), 1e-12);
    assertEquals(pLon, p2.getLongitude(), 1e-12);
    
  }

}


