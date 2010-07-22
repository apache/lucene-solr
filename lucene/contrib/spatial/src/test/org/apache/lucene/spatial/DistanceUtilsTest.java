package org.apache.lucene.spatial;

import junit.framework.TestCase;
import org.apache.lucene.spatial.tier.InvalidGeoException;


/**
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


/**
 *
 *
 **/
public class DistanceUtilsTest extends TestCase {

  public void testBoxCorner() throws Exception {
    double[] zero = new double[]{0, 0};
    double[] zeroOne = new double[]{0, 1};
    double[] oneOne = new double[]{1, 1};
    double[] pt1 = new double[]{1.5, 110.3};
    double[] result = DistanceUtils.vectorBoxCorner(zero, null, Math.sqrt(2), true);
    assertEquals(1.0, result[0]);
    assertEquals(1.0, result[1]);

    result = DistanceUtils.vectorBoxCorner(zero, null, Math.sqrt(2), false);
    assertEquals(-1.0, result[0]);
    assertEquals(-1.0, result[1]);

    result = DistanceUtils.vectorBoxCorner(oneOne, null, Math.sqrt(2), true);
    assertEquals(2.0, result[0]);
    assertEquals(2.0, result[1]);

    result = DistanceUtils.vectorBoxCorner(zeroOne, null, Math.sqrt(2), true);
    assertEquals(1.0, result[0]);
    assertEquals(2.0, result[1]);

    result = DistanceUtils.vectorBoxCorner(pt1, null, Math.sqrt(2), true);
    assertEquals(2.5, result[0]);
    assertEquals(111.3, result[1]);

    result = DistanceUtils.vectorBoxCorner(pt1, null, Math.sqrt(2), false);
    assertEquals(0.5, result[0]);
    assertEquals(109.3, result[1]);

  }

  public void testNormLatLon() throws Exception {

  }

  public void testLatLonCorner() throws Exception {
    double[] zero = new double[]{0, 0};
    double[] zero45 = new double[]{0, DistanceUtils.DEG_45};
    double[] result;
    // 	00°38′09″N, 000°38′09″E
    //Verify at http://www.movable-type.co.uk/scripts/latlong.html
    result = DistanceUtils.latLonCorner(zero[0], zero[1], 100, null, true, DistanceUtils.EARTH_MEAN_RADIUS_KM);
    assertEquals(0.63583 * DistanceUtils.DEGREES_TO_RADIANS, result[0], 0.001);
    assertEquals(0.63583 * DistanceUtils.DEGREES_TO_RADIANS, result[1], 0.001);

    result = DistanceUtils.latLonCornerDegs(zero[0], zero[1], 100, null, true, DistanceUtils.EARTH_MEAN_RADIUS_KM);
    // 	00°38′09″N, 000°38′09″E
    assertEquals(0.63583, result[0], 0.001);
    assertEquals(0.63583, result[1], 0.001);

    result = DistanceUtils.latLonCornerDegs(zero[0], zero[1], 100, null, false, DistanceUtils.EARTH_MEAN_RADIUS_KM);
    // 	00°38′09″N, 000°38′09″E
    assertEquals(-0.63583, result[0], 0.001);
    assertEquals(-0.63583, result[1], 0.001);

    //test some edge cases
    //89°16′02″N, 060°12′35″E
    result = DistanceUtils.latLonCornerDegs(89.0, 0, 100, null, true, DistanceUtils.EARTH_MEAN_RADIUS_KM);
    assertEquals(89.26722, result[0], 0.001);
    assertEquals(60.20972, result[1], 0.001);

    result = DistanceUtils.latLonCornerDegs(0, -179.0, 100, null, true, DistanceUtils.EARTH_MEAN_RADIUS_KM);
    assertEquals(0.63583, result[0], 0.001);
    assertEquals(-178.36417, result[1], 0.001);

  }

  public void testVectorDistance() throws Exception {
    double[] zero = new double[]{0, 0};
    double[] zeroOne = new double[]{0, 1};
    double[] oneZero = new double[]{1, 0};
    double[] oneOne = new double[]{1, 1};
    double distance;
    distance = DistanceUtils.vectorDistance(zero, zeroOne, 2);
    assertEquals(1.0, distance);
    distance = DistanceUtils.vectorDistance(zero, oneZero, 2);
    assertEquals(1.0, distance);
    distance = DistanceUtils.vectorDistance(zero, oneOne, 2);
    assertEquals(Math.sqrt(2), distance, 0.001);

    distance = DistanceUtils.squaredEuclideanDistance(zero, oneOne);
    assertEquals(2, distance, 0.001);
  }

  public void testHaversine() throws Exception {
    double distance;
    //compare to http://www.movable-type.co.uk/scripts/latlong.html
    distance = DistanceUtils.haversine(0, 0, Math.PI / 4.0, Math.PI / 4.0, DistanceUtils.EARTH_MEAN_RADIUS_KM);
    assertEquals(6672.0, distance, 0.5);

    distance = DistanceUtils.haversine(0, 0, Math.toRadians(20), Math.toRadians(20), DistanceUtils.EARTH_MEAN_RADIUS_KM);
    assertEquals(3112, distance, 0.5);

    distance = DistanceUtils.haversine(0, 0, Math.toRadians(1), Math.toRadians(1), DistanceUtils.EARTH_MEAN_RADIUS_KM);
    assertEquals(157.2, distance, 0.5);

    //Try some around stuff
    distance = DistanceUtils.haversine(Math.toRadians(1), Math.toRadians(-1),
            Math.toRadians(1), Math.toRadians(1), DistanceUtils.EARTH_MEAN_RADIUS_KM);
    assertEquals(222.4, distance, 0.5);

    distance = DistanceUtils.haversine(Math.toRadians(89), Math.toRadians(-1),
            Math.toRadians(89), Math.toRadians(179), DistanceUtils.EARTH_MEAN_RADIUS_KM);
    assertEquals(222.4, distance, 0.5);

    distance = DistanceUtils.haversine(Math.toRadians(89), Math.toRadians(-1),
            Math.toRadians(49), Math.toRadians(179), DistanceUtils.EARTH_MEAN_RADIUS_KM);
    assertEquals(4670, distance, 0.5);

    distance = DistanceUtils.haversine(Math.toRadians(0), Math.toRadians(-179),
            Math.toRadians(0), Math.toRadians(179), DistanceUtils.EARTH_MEAN_RADIUS_KM);
    assertEquals(222.4, distance, 0.5);

  }

  public void testParse() throws Exception {
    String[] parse;
    parse = DistanceUtils.parsePoint(null, "89.0,73.2", 2);
    assertEquals(2, parse.length);
    assertEquals("89.0", parse[0]);
    assertEquals("73.2", parse[1]);

    parse = DistanceUtils.parsePoint(null, "89.0,73.2,-92.3", 3);
    assertEquals(3, parse.length);
    assertEquals("89.0", parse[0]);
    assertEquals("73.2", parse[1]);
    assertEquals("-92.3", parse[2]);

    parse = DistanceUtils.parsePoint(null, "    89.0         ,   73.2  ,              -92.3   ", 3);
    assertEquals(3, parse.length);
    assertEquals("89.0", parse[0]);
    assertEquals("73.2", parse[1]);
    assertEquals("-92.3", parse[2]);


    String[] foo = DistanceUtils.parsePoint(parse, "89.0         ,   73.2 ,              -92.3", 3);
    //should be same piece of memory
    assertTrue(foo == parse);
    assertEquals(3, parse.length);
    assertEquals("89.0", parse[0]);
    assertEquals("73.2", parse[1]);
    assertEquals("-92.3", parse[2]);
    //array should get automatically resized
    parse = DistanceUtils.parsePoint(new String[1], "89.0         ,   73.2 ,              -92.3", 3);
    assertEquals(3, parse.length);
    assertEquals("89.0", parse[0]);
    assertEquals("73.2", parse[1]);
    assertEquals("-92.3", parse[2]);


    try {
      parse = DistanceUtils.parsePoint(null, "89.0         ,   ", 3);
      assertTrue(false);
    } catch (InvalidGeoException e) {
    }
    try {
      parse = DistanceUtils.parsePoint(null, " , 89.0          ", 3);
      assertTrue(false);
    } catch (InvalidGeoException e) {
    }

    try {
      parse = DistanceUtils.parsePoint(null, "", 3);
      assertTrue(false);
    } catch (InvalidGeoException e) {
    }


    double[] dbls = DistanceUtils.parsePointDouble(null, "89.0         ,   73.2 ,              -92.3", 3);
    assertEquals(3, dbls.length);
    assertEquals(89.0, dbls[0]);
    assertEquals(73.2, dbls[1]);
    assertEquals(-92.3, dbls[2]);

    try {
      dbls = DistanceUtils.parsePointDouble(null, "89.0         ,   foo ,              -92.3", 3);
      assertTrue(false);
    } catch (NumberFormatException e) {
    }

    dbls = DistanceUtils.parseLatitudeLongitude(null, "89.0         ,   73.2    ");
    assertEquals(2, dbls.length);
    assertEquals(89.0, dbls[0]);
    assertEquals(73.2, dbls[1]);

    //test some bad lat/long pairs
    try {
      dbls = DistanceUtils.parseLatitudeLongitude(null, "189.0         ,   73.2    ");
      assertTrue(false);
    } catch (InvalidGeoException e) {

    }

    try {
      dbls = DistanceUtils.parseLatitudeLongitude(null, "89.0         ,   273.2    ");
      assertTrue(false);
    } catch (InvalidGeoException e) {

    }

  }

}
