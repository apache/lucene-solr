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
package org.apache.lucene.util;


import static org.apache.lucene.util.SloppyMath.cos;
import static org.apache.lucene.util.SloppyMath.asin;
import static org.apache.lucene.util.SloppyMath.haversin;

import java.util.Random;

public class TestSloppyMath extends LuceneTestCase {
  // accuracy for cos()
  static double COS_DELTA = 1E-15;
  // accuracy for asin()
  static double ASIN_DELTA = 1E-7;
  
  public void testCos() {
    assertTrue(Double.isNaN(cos(Double.NaN)));
    assertTrue(Double.isNaN(cos(Double.NEGATIVE_INFINITY)));
    assertTrue(Double.isNaN(cos(Double.POSITIVE_INFINITY)));
    assertEquals(StrictMath.cos(1), cos(1), COS_DELTA);
    assertEquals(StrictMath.cos(0), cos(0), COS_DELTA);
    assertEquals(StrictMath.cos(Math.PI/2), cos(Math.PI/2), COS_DELTA);
    assertEquals(StrictMath.cos(-Math.PI/2), cos(-Math.PI/2), COS_DELTA);
    assertEquals(StrictMath.cos(Math.PI/4), cos(Math.PI/4), COS_DELTA);
    assertEquals(StrictMath.cos(-Math.PI/4), cos(-Math.PI/4), COS_DELTA);
    assertEquals(StrictMath.cos(Math.PI*2/3), cos(Math.PI*2/3), COS_DELTA);
    assertEquals(StrictMath.cos(-Math.PI*2/3), cos(-Math.PI*2/3), COS_DELTA);
    assertEquals(StrictMath.cos(Math.PI/6), cos(Math.PI/6), COS_DELTA);
    assertEquals(StrictMath.cos(-Math.PI/6), cos(-Math.PI/6), COS_DELTA);
    
    // testing purely random longs is inefficent, as for stupid parameters we just 
    // pass thru to Math.cos() instead of doing some huperduper arg reduction
    for (int i = 0; i < 10000; i++) {
      double d = random().nextDouble() * SloppyMath.SIN_COS_MAX_VALUE_FOR_INT_MODULO;
      if (random().nextBoolean()) {
        d = -d;
      }
      assertEquals(StrictMath.cos(d), cos(d), COS_DELTA);
    }
  }
  
  public void testAsin() {
    assertTrue(Double.isNaN(asin(Double.NaN)));
    assertTrue(Double.isNaN(asin(2)));
    assertTrue(Double.isNaN(asin(-2)));
    assertEquals(-Math.PI/2, asin(-1), ASIN_DELTA);
    assertEquals(-Math.PI/3, asin(-0.8660254), ASIN_DELTA);
    assertEquals(-Math.PI/4, asin(-0.7071068), ASIN_DELTA);
    assertEquals(-Math.PI/6, asin(-0.5), ASIN_DELTA);
    assertEquals(0, asin(0), ASIN_DELTA);
    assertEquals(Math.PI/6, asin(0.5), ASIN_DELTA);
    assertEquals(Math.PI/4, asin(0.7071068), ASIN_DELTA);
    assertEquals(Math.PI/3, asin(0.8660254), ASIN_DELTA);
    assertEquals(Math.PI/2, asin(1), ASIN_DELTA);
    // only values -1..1 are useful
    for (int i = 0; i < 10000; i++) {
      double d = random().nextDouble();
      if (random().nextBoolean()) {
        d = -d;
      }
      assertEquals(StrictMath.asin(d), asin(d), ASIN_DELTA);
      assertTrue(asin(d) >= -Math.PI/2);
      assertTrue(asin(d) <= Math.PI/2);
    }
  }
  
  public void testHaversin() {
    assertTrue(Double.isNaN(haversin(1, 1, 1, Double.NaN)));
    assertTrue(Double.isNaN(haversin(1, 1, Double.NaN, 1)));
    assertTrue(Double.isNaN(haversin(1, Double.NaN, 1, 1)));
    assertTrue(Double.isNaN(haversin(Double.NaN, 1, 1, 1)));
    
    assertEquals(0, haversin(0, 0, 0, 0), 0D);
    assertEquals(0, haversin(0, -180, 0, -180), 0D);
    assertEquals(0, haversin(0, -180, 0, 180), 0D);
    assertEquals(0, haversin(0, 180, 0, 180), 0D);
    assertEquals(0, haversin(90, 0, 90, 0), 0D);
    assertEquals(0, haversin(90, -180, 90, -180), 0D);
    assertEquals(0, haversin(90, -180, 90, 180), 0D);
    assertEquals(0, haversin(90, 180, 90, 180), 0D);
    
    // Test half a circle on the equator, using WGS84 earth radius
    double earthRadiusKMs = 6378.137;
    double halfCircle = earthRadiusKMs * Math.PI;
    assertEquals(halfCircle, haversin(0, 0, 0, 180), 0D);

    Random r = random();
    double randomLat1 = 40.7143528 + (r.nextInt(10) - 5) * 360;
    double randomLon1 = -74.0059731 + (r.nextInt(10) - 5) * 360;

    double randomLat2 = 40.65 + (r.nextInt(10) - 5) * 360;
    double randomLon2 = -73.95 + (r.nextInt(10) - 5) * 360;
    
    assertEquals(8.572, haversin(randomLat1, randomLon1, randomLat2, randomLon2), 0.01D);
    
    
    // from solr and ES tests (with their respective epsilons)
    assertEquals(0, haversin(40.7143528, -74.0059731, 40.7143528, -74.0059731), 0D);
    assertEquals(5.286, haversin(40.7143528, -74.0059731, 40.759011, -73.9844722), 0.01D);
    assertEquals(0.4621, haversin(40.7143528, -74.0059731, 40.718266, -74.007819), 0.01D);
    assertEquals(1.055, haversin(40.7143528, -74.0059731, 40.7051157, -74.0088305), 0.01D);
    assertEquals(1.258, haversin(40.7143528, -74.0059731, 40.7247222, -74), 0.01D);
    assertEquals(2.029, haversin(40.7143528, -74.0059731, 40.731033, -73.9962255), 0.01D);
    assertEquals(8.572, haversin(40.7143528, -74.0059731, 40.65, -73.95), 0.01D);
  }
}
