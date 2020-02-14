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

public class TestXYPoint extends LuceneTestCase {

  /** point values cannot be NaN */
  public void testNaN() {
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      new XYPoint(Float.NaN, 45.23f);
    });
    assertTrue(expected.getMessage().contains("invalid value NaN"));

    expected = expectThrows(IllegalArgumentException.class, () -> {
      new XYPoint(43.5f, Float.NaN);
    });
    assertTrue(expected.getMessage().contains("invalid value NaN"));
  }

  /** point values mist be finite */
  public void testPositiveInf() {
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      new XYPoint(Float.POSITIVE_INFINITY, 45.23f);
    });
    assertTrue(expected.getMessage().contains("invalid value Inf"));

    expected = expectThrows(IllegalArgumentException.class, () -> {
      new XYPoint(43.5f, Float.POSITIVE_INFINITY);
    });
    assertTrue(expected.getMessage().contains("invalid value Inf"));
  }

  /** point values mist be finite */
  public void testNegativeInf() {
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      new XYPoint(Float.NEGATIVE_INFINITY, 45.23f);
    });
    assertTrue(expected.getMessage().contains("invalid value -Inf"));

    expected = expectThrows(IllegalArgumentException.class, () -> {
      new XYPoint(43.5f, Float.NEGATIVE_INFINITY);
    });
    assertTrue(expected.getMessage().contains("invalid value -Inf"));
  }

  /** equals and hashcode */
  public void testEqualsAndHashCode() {
    XYPoint point = new XYPoint(ShapeTestUtil.nextFloat(random()), ShapeTestUtil.nextFloat(random()));
    XYPoint copy = new XYPoint(point.getX(), point.getY());
    assertEquals(point, copy);
    assertEquals(point.hashCode(), copy.hashCode());
    XYPoint otherPoint = new XYPoint(ShapeTestUtil.nextFloat(random()), ShapeTestUtil.nextFloat(random()));
    if (point.getX() != otherPoint.getX() || point.getY() != otherPoint.getY()) {
      assertNotEquals(point, otherPoint);
      assertNotEquals(point.hashCode(), otherPoint.hashCode());
    } else {
      assertEquals(point, otherPoint);
      assertEquals(point.hashCode(), otherPoint.hashCode());
    }
  }
}