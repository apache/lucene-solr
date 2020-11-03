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
package org.apache.lucene.geo;

import org.apache.lucene.util.LuceneTestCase;

public class TestCircle extends LuceneTestCase {

  /** latitude should be on range */
  public void testInvalidLat() {
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      new Circle(134.14, 45.23, 1000);
    });
    assertTrue(expected.getMessage().contains("invalid latitude 134.14; must be between -90.0 and 90.0"));
  }

  /** longitude should be on range */
  public void testInvalidLon() {
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      new Circle(43.5, 180.5, 1000);
    });
    assertTrue(expected.getMessage().contains("invalid longitude 180.5; must be between -180.0 and 180.0"));
  }

  /** radius must be positive */
  public void testNegativeRadius() {
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      new Circle(43.5, 45.23, -1000);
    });
    assertTrue(expected.getMessage().contains("radiusMeters: '-1000.0' is invalid"));
  }

  /** radius must cannot be infinite */
  public void testInfiniteRadius() {
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      new Circle(43.5, 45.23, Double.POSITIVE_INFINITY);
    });
    assertTrue(expected.getMessage().contains("radiusMeters: 'Infinity' is invalid"));
  }

  /** equals and hashcode */
  public void testEqualsAndHashCode() {
    Circle circle = GeoTestUtil.nextCircle();
    Circle copy = new Circle(circle.getLat(), circle.getLon(), circle.getRadius());
    assertEquals(circle, copy);
    assertEquals(circle.hashCode(), copy.hashCode());
    Circle otherCircle = GeoTestUtil.nextCircle();
    if (circle.getLon() != otherCircle.getLon() || circle.getLat() != otherCircle.getLat() || circle.getRadius() != otherCircle.getRadius()) {
      assertNotEquals(circle, otherCircle);
      assertNotEquals(circle.hashCode(), otherCircle.hashCode());
    } else {
      assertEquals(circle, otherCircle);
      assertEquals(circle.hashCode(), otherCircle.hashCode());
    }
  }
}
