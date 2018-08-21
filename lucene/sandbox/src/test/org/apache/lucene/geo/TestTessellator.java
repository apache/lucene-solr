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

import static org.apache.lucene.geo.GeoTestUtil.nextBoxNotCrossingDateline;

/** Test case for the Polygon {@link Tessellator} class */
public class TestTessellator extends LuceneTestCase {

  /** test line intersection */
  public void testLinesIntersect() {
    Rectangle rect = nextBoxNotCrossingDateline();
    // simple case; test intersecting diagonals
    // note: we don't quantize because the tessellator operates on non quantized vertices
    assertTrue(Tessellator.linesIntersect(rect.minLon, rect.minLat, rect.maxLon, rect.maxLat, rect.maxLon, rect.minLat, rect.minLon, rect.maxLat));
    // test closest encoded value
    assertFalse(Tessellator.linesIntersect(rect.minLon, rect.maxLat, rect.maxLon, rect.maxLat, rect.minLon - 1d, rect.minLat, rect.minLon - 1, rect.maxLat));
  }

  public void testSimpleTessellation() throws Exception {
    Polygon poly = GeoTestUtil.createRegularPolygon(0.0, 0.0, 1000000, 1000000);
    Polygon inner = new Polygon(new double[] {-1.0, -1.0, 0.5, 1.0, 1.0, 0.5, -1.0},
        new double[]{1.0, -1.0, -0.5, -1.0, 1.0, 0.5, 1.0});
    Polygon inner2 = new Polygon(new double[] {-1.0, -1.0, 0.5, 1.0, 1.0, 0.5, -1.0},
        new double[]{-2.0, -4.0, -3.5, -4.0, -2.0, -2.5, -2.0});
    poly = new Polygon(poly.getPolyLats(), poly.getPolyLons(), inner, inner2);
    assertTrue(Tessellator.tessellate(poly).size() > 0);
  }
}