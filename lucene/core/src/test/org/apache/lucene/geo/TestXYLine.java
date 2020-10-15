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


import java.util.Arrays;

import org.apache.lucene.util.LuceneTestCase;

public class TestXYLine extends LuceneTestCase {

  /** null x not allowed */
  public void testLineNullXs() {
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      new XYLine(null, new float[] { -66, -65, -65, -66, -66 });
    });
    assertTrue(expected.getMessage().contains("x must not be null"));
  }

  /** null y not allowed */
  public void testPolygonNullYs() {
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      new XYLine(new float[] {18, 18, 19, 19, 18 }, null);
    });
    assertTrue(expected.getMessage().contains("y must not be null"));
  }

  /** needs at least 3 vertices */
  public void testLineEnoughPoints() {
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      new XYLine(new float[] {18}, new float[] { -66});
    });
    assertTrue(expected.getMessage().contains("at least 2 line points required"));
  }

  /** lines needs same number of x as y */
  public void testLinesBogus() {
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      new XYLine(new float[] { 18, 18, 19, 19 }, new float[] { -66, -65, -65, -66, -66 });
    });
    assertTrue(expected.getMessage().contains("must be equal length"));
  }

  /** line values cannot be NaN */
  public void testLineNaN() {
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      new XYLine(new float[] { 18, 18, 19, Float.NaN, 18 }, new float[] { -66, -65, -65, -66, -66 });
    });
    assertTrue(expected.getMessage(), expected.getMessage().contains("invalid value NaN"));
  }

  /** line values cannot be finite */
  public void testLinePositiveInfinite() {
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      new XYLine(new float[] { 18, 18, 19, 19, 18 }, new float[] { -66, Float.POSITIVE_INFINITY, -65, -66, -66 });
    });
    assertTrue(expected.getMessage(), expected.getMessage().contains("invalid value Inf"));
  }

  /** line values cannot be finite */
  public void testLineNegativeInfinite() {
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      new XYLine(new float[] { 18, 18, 19, 19, 18 }, new float[] { -66, -65, -65, Float.NEGATIVE_INFINITY, -66 });
    });
    assertTrue(expected.getMessage(), expected.getMessage().contains("invalid value -Inf"));
  }

  /** equals and hashcode */
  public void testEqualsAndHashCode() {
    XYLine line = ShapeTestUtil.nextLine();
    XYLine copy = new XYLine(line.getX(), line.getY());
    assertEquals(line, copy);
    assertEquals(line.hashCode(), copy.hashCode());
    XYLine otherLine = ShapeTestUtil.nextLine();
    if (Arrays.equals(line.getX(), otherLine.getX()) == false  ||
        Arrays.equals(line.getY(), otherLine.getY()) == false) {
      assertNotEquals(line, otherLine);
      assertNotEquals(line.hashCode(), otherLine.hashCode());
    } else {
      assertEquals(line, otherLine);
      assertEquals(line.hashCode(), otherLine.hashCode());
    }

  }
}