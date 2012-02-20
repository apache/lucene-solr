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

package org.apache.lucene.spatial.base.prefix.geohash;

import org.apache.lucene.spatial.base.context.SpatialContext;
import org.apache.lucene.spatial.base.context.simple.SimpleSpatialContext;
import org.apache.lucene.spatial.base.distance.DistanceUnits;
import org.apache.lucene.spatial.base.shape.Point;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link GeohashUtils}
 */
public class TestGeohashUtils {
  SpatialContext ctx = new SimpleSpatialContext( DistanceUnits.KILOMETERS );

  /**
   * Pass condition: lat=42.6, lng=-5.6 should be encoded as "ezs42e44yx96",
   * lat=57.64911 lng=10.40744 should be encoded as "u4pruydqqvj8"
   */
  @Test
  public void testEncode() {
    String hash = GeohashUtils.encodeLatLon(42.6, -5.6);
    assertEquals("ezs42e44yx96", hash);

    hash = GeohashUtils.encodeLatLon(57.64911, 10.40744);
    assertEquals("u4pruydqqvj8", hash);
  }

  /**
   * Pass condition: lat=52.3738007, lng=4.8909347 should be encoded and then
   * decoded within 0.00001 of the original value
   */
  @Test
  public void testDecodePreciseLongitudeLatitude() {
    String hash = GeohashUtils.encodeLatLon(52.3738007, 4.8909347);

    Point point = GeohashUtils.decode(hash,ctx);

    assertEquals(52.3738007, point.getY(), 0.00001D);
    assertEquals(4.8909347, point.getX(), 0.00001D);
  }

  /**
   * Pass condition: lat=84.6, lng=10.5 should be encoded and then decoded
   * within 0.00001 of the original value
   */
  @Test
  public void testDecodeImpreciseLongitudeLatitude() {
    String hash = GeohashUtils.encodeLatLon(84.6, 10.5);

    Point point = GeohashUtils.decode(hash, ctx);

    assertEquals(84.6, point.getY(), 0.00001D);
    assertEquals(10.5, point.getX(), 0.00001D);
  }

  /*
   * see https://issues.apache.org/jira/browse/LUCENE-1815 for details
   */
  @Test
  public void testDecodeEncode() {
    String geoHash = "u173zq37x014";
    assertEquals(geoHash, GeohashUtils.encodeLatLon(52.3738007, 4.8909347));
    Point point = GeohashUtils.decode(geoHash,ctx);
    assertEquals(52.37380061d, point.getY(), 0.000001d);
    assertEquals(4.8909343d, point.getX(), 0.000001d);

    assertEquals(geoHash, GeohashUtils.encodeLatLon(point.getY(), point.getX()));

    geoHash = "u173";
    point = GeohashUtils.decode("u173",ctx);
    geoHash = GeohashUtils.encodeLatLon(point.getY(), point.getX());
    final Point point2 = GeohashUtils.decode(geoHash, ctx);
    assertEquals(point.getY(), point2.getY(), 0.000001d);
    assertEquals(point.getX(), point2.getX(), 0.000001d);
  }

  /** see the table at http://en.wikipedia.org/wiki/Geohash */
  @Test
  public void testHashLenToWidth() {
    double[] box = GeohashUtils.lookupDegreesSizeForHashLen(3);
    assertEquals(1.40625,box[0],0.0001);
    assertEquals(1.40625,box[1],0.0001);
  }
}
