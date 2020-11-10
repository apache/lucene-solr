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

public class TestXYCircle extends LuceneTestCase {

  /** point values cannot be NaN */
  public void testNaN() {
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      new XYCircle(Float.NaN, 45.23f, 35.5f);
    });
    assertTrue(expected.getMessage().contains("invalid value NaN"));

    expected = expectThrows(IllegalArgumentException.class, () -> {
      new XYCircle(43.5f, Float.NaN, 35.5f);
    });
    assertTrue(expected.getMessage(), expected.getMessage().contains("invalid value NaN"));
  }

  /** point values mist be finite */
  public void testPositiveInf() {
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      new XYCircle(Float.POSITIVE_INFINITY, 45.23f, 35.5f);
    });
    assertTrue(expected.getMessage().contains("invalid value Inf"));

    expected = expectThrows(IllegalArgumentException.class, () -> {
      new XYCircle(43.5f, Float.POSITIVE_INFINITY, 35.5f);
    });
    assertTrue(expected.getMessage(), expected.getMessage().contains("invalid value Inf"));
  }

  /** point values mist be finite */
  public void testNegativeInf() {
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      new XYCircle(Float.NEGATIVE_INFINITY, 45.23f, 35.5f);
    });
    assertTrue(expected.getMessage().contains("invalid value -Inf"));

    expected = expectThrows(IllegalArgumentException.class, () -> {
      new XYCircle(43.5f, Float.NEGATIVE_INFINITY, 35.5f);
    });
    assertTrue(expected.getMessage(), expected.getMessage().contains("invalid value -Inf"));
  }

  /** radius must be positive */
  public void testNegativeRadius() {
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      new XYCircle(43.5f, 45.23f, -1000f);
    });
    assertTrue(expected.getMessage(), expected.getMessage().contains("radius must be bigger than 0, got -1000.0"));
  }

  /** radius must be lower than 3185504.3857 */
  public void testInfiniteRadius() {
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      new XYCircle(43.5f, 45.23f, Float.POSITIVE_INFINITY);
    });
    assertTrue(expected.getMessage(), expected.getMessage().contains("radius must be finite, got Infinity"));
  }

  /** equals and hashcode */
  public void testEqualsAndHashCode() {
    XYCircle circle = ShapeTestUtil.nextCircle();
    XYCircle copy = new XYCircle(circle.getX(), circle.getY(), circle.getRadius());
    assertEquals(circle, copy);
    assertEquals(circle.hashCode(), copy.hashCode());
    XYCircle otherCircle = ShapeTestUtil.nextCircle();
    if (circle.getX() != otherCircle.getX() || circle.getY() != otherCircle.getY() || circle.getRadius() != otherCircle.getRadius()) {
      assertNotEquals(circle, otherCircle);
      assertNotEquals(circle.hashCode(), otherCircle.hashCode());
    } else {
      assertEquals(circle, otherCircle);
      assertEquals(circle.hashCode(), otherCircle.hashCode());
    }
  }
}