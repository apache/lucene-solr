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

public class TestXYPolygon extends LuceneTestCase {

  /** null x not allowed */
  public void testPolygonNullPolyLats() {
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      new XYPolygon(null, new float[] { -66, -65, -65, -66, -66 });
    });
    assertTrue(expected.getMessage().contains("x must not be null"));
  }

  /** null y not allowed */
  public void testPolygonNullPolyLons() {
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      new XYPolygon(new float[] {18, 18, 19, 19, 18 }, null);
    });
    assertTrue(expected.getMessage().contains("y must not be null"));
  }

  /** polygon needs at least 3 vertices */
  public void testPolygonLine() {
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      new XYPolygon(new float[] { 18, 18, 18 }, new float[] { -66, -65, -66 });
    });
    assertTrue(expected.getMessage().contains("at least 4 polygon points required"));
  }

  /** polygon needs same number of latitudes as longitudes */
  public void testPolygonBogus() {
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      new XYPolygon(new float[] { 18, 18, 19, 19 }, new float[] { -66, -65, -65, -66, -66 });
    });
    assertTrue(expected.getMessage().contains("must be equal length"));
  }

  /** polygon must be closed */
  public void testPolygonNotClosed() {
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      new XYPolygon(new float[] { 18, 18, 19, 19, 19 }, new float[] { -66, -65, -65, -66, -67 });
    });
    assertTrue(expected.getMessage(), expected.getMessage().contains("it must close itself"));
  }

  /** polygon values cannot be NaN */
  public void testPolygonNaN() {
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      new XYPolygon(new float[] { 18, 18, 19, Float.NaN, 18 }, new float[] { -66, -65, -65, -66, -66 });
    });
    assertTrue(expected.getMessage(), expected.getMessage().contains("invalid value NaN"));
  }

  /** polygon values cannot be finite */
  public void testPolygonPositiveInfinite() {
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      new XYPolygon(new float[] { 18, 18, 19, 19, 18 }, new float[] { -66, Float.POSITIVE_INFINITY, -65, -66, -66 });
    });
    assertTrue(expected.getMessage(), expected.getMessage().contains("invalid value Inf"));
  }

  /** polygon values cannot be finite */
  public void testPolygonNegativeInfinite() {
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      new XYPolygon(new float[] { 18, 18, 19, 19, 18 }, new float[] { -66, -65, -65, Float.NEGATIVE_INFINITY, -66 });
    });
    assertTrue(expected.getMessage(), expected.getMessage().contains("invalid value -Inf"));
  }

  /** equals and hashcode */
  public void testEqualsAndHashCode() {
    XYPolygon polygon = ShapeTestUtil.nextPolygon();
    XYPolygon copy = new XYPolygon(polygon.getPolyX(), polygon.getPolyY(), polygon.getHoles());
    assertEquals(polygon, copy);
    assertEquals(polygon.hashCode(), copy.hashCode());
    XYPolygon otherPolygon = ShapeTestUtil.nextPolygon();
    if (Arrays.equals(polygon.getPolyX(), otherPolygon.getPolyX()) == false ||
        Arrays.equals(polygon.getPolyY(), otherPolygon.getPolyY()) == false ||
        Arrays.equals(polygon.getHoles(), otherPolygon.getHoles()) == false) {
      assertNotEquals(polygon, otherPolygon);
      assertNotEquals(polygon.hashCode(), otherPolygon.hashCode());
    } else {
      assertEquals(polygon, otherPolygon);
      assertEquals(polygon.hashCode(), otherPolygon.hashCode());
    }
  }
}