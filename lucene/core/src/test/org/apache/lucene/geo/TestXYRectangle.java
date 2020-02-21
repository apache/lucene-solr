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

public class TestXYRectangle extends LuceneTestCase {

  /** maxX must be gte minX */
  public void tesInvalidMinMaxX() {
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      new XYRectangle(5, 4, 3 ,4);
    });
    assertTrue(expected.getMessage().contains("5 > 4"));
  }

  /** maxY must be gte minY */
  public void tesInvalidMinMaxY() {
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      new XYRectangle(4, 5, 5 ,4);
    });
    assertTrue(expected.getMessage().contains("5 > 4"));
  }

  /** rectangle values cannot be NaN */
  public void testNaN() {
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      new XYRectangle(Float.NaN, 4, 3 ,4);
    });
    assertTrue(expected.getMessage().contains("invalid value NaN"));

    expected = expectThrows(IllegalArgumentException.class, () -> {
      new XYRectangle(3, Float.NaN, 3 ,4);
    });
    assertTrue(expected.getMessage().contains("invalid value NaN"));

    expected = expectThrows(IllegalArgumentException.class, () -> {
      new XYRectangle(3, 4, Float.NaN ,4);
    });
    assertTrue(expected.getMessage().contains("invalid value NaN"));

    expected = expectThrows(IllegalArgumentException.class, () -> {
      new XYRectangle(3, 4, 3 , Float.NaN);
    });
    assertTrue(expected.getMessage().contains("invalid value NaN"));
  }

  /** rectangle values must be finite */
  public void testPositiveInf() {
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      new XYRectangle(3, Float.POSITIVE_INFINITY, 3 ,4);
    });
    assertTrue(expected.getMessage().contains("invalid value Inf"));

    expected = expectThrows(IllegalArgumentException.class, () -> {
      new XYRectangle(3, 4, 3 , Float.POSITIVE_INFINITY);
    });
    assertTrue(expected.getMessage().contains("invalid value Inf"));
  }

  /** rectangle values must be finite */
  public void testNegativeInf() {
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      new XYRectangle(Float.NEGATIVE_INFINITY, 4, 3 ,4);
    });
    assertTrue(expected.getMessage().contains("invalid value -Inf"));

    expected = expectThrows(IllegalArgumentException.class, () -> {
      new XYRectangle(3, 4, Float.NEGATIVE_INFINITY ,4);
    });
    assertTrue(expected.getMessage().contains("invalid value -Inf"));
  }

  /** equals and hashcode */
  public void testEqualsAndHashCode() {
    XYRectangle rectangle = ShapeTestUtil.nextBox(random());
    XYRectangle copy = new XYRectangle(rectangle.minX, rectangle.maxX, rectangle.minY, rectangle.maxY);
    assertEquals(rectangle, copy);
    assertEquals(rectangle.hashCode(), copy.hashCode());
    XYRectangle otherRectangle = ShapeTestUtil.nextBox(random());
    if (rectangle.minX != otherRectangle.minX || rectangle.maxX != otherRectangle.maxX ||
        rectangle.minY != otherRectangle.minY || rectangle.maxY != otherRectangle.maxY) {
      assertNotEquals(rectangle, otherRectangle);
      assertNotEquals(rectangle.hashCode(), otherRectangle.hashCode());
    } else {
      assertEquals(rectangle, otherRectangle);
      assertEquals(rectangle.hashCode(), otherRectangle.hashCode());
    }
  }

}