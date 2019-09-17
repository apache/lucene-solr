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

import java.text.ParseException;

import org.apache.lucene.util.LuceneTestCase;

public class TestPolygon extends LuceneTestCase {
  
  /** null polyLats not allowed */
  public void testPolygonNullPolyLats() {
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      new Polygon(null, new double[] { -66, -65, -65, -66, -66 });
    });
    assertTrue(expected.getMessage().contains("polyLats must not be null"));
  }
  
  /** null polyLons not allowed */
  public void testPolygonNullPolyLons() {
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      new Polygon(new double[] { 18, 18, 19, 19, 18 }, null);
    });
    assertTrue(expected.getMessage().contains("polyLons must not be null"));
  }
  
  /** polygon needs at least 3 vertices */
  public void testPolygonLine() {
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      new Polygon(new double[] { 18, 18, 18 }, new double[] { -66, -65, -66 });
    });
    assertTrue(expected.getMessage().contains("at least 4 polygon points required"));
  }
  
  /** polygon needs same number of latitudes as longitudes */
  public void testPolygonBogus() {
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      new Polygon(new double[] { 18, 18, 19, 19 }, new double[] { -66, -65, -65, -66, -66 });
    });
    assertTrue(expected.getMessage().contains("must be equal length"));
  }
  
  /** polygon must be closed */
  public void testPolygonNotClosed() {
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      new Polygon(new double[] { 18, 18, 19, 19, 19 }, new double[] { -66, -65, -65, -66, -67 });
    });
    assertTrue(expected.getMessage(), expected.getMessage().contains("it must close itself"));
  }

  public void testGeoJSONPolygon() throws Exception {
    StringBuilder b = new StringBuilder();
    b.append("{\n");
    b.append("  \"type\": \"Polygon\",\n");
    b.append("  \"coordinates\": [\n");
    b.append("    [ [100.0, 0.0], [101.0, 0.0], [101.0, 1.0],\n");
    b.append("      [100.0, 1.0], [100.0, 0.0] ]\n");
    b.append("  ]\n");
    b.append("}\n");
     
    Polygon[] polygons = Polygon.fromGeoJSON(b.toString());
    assertEquals(1, polygons.length);
    assertEquals(new Polygon(new double[] {0.0, 0.0, 1.0, 1.0, 0.0},
                             new double[] {100.0, 101.0, 101.0, 100.0, 100.0}), polygons[0]);
  }

  public void testGeoJSONPolygonWithHole() throws Exception {
    StringBuilder b = new StringBuilder();
    b.append("{\n");
    b.append("  \"type\": \"Polygon\",\n");
    b.append("  \"coordinates\": [\n");
    b.append("    [ [100.0, 0.0], [101.0, 0.0], [101.0, 1.0],\n");
    b.append("      [100.0, 1.0], [100.0, 0.0] ],\n");
    b.append("    [ [100.5, 0.5], [100.5, 0.75], [100.75, 0.75], [100.75, 0.5], [100.5, 0.5]]\n");
    b.append("  ]\n");
    b.append("}\n");
     
    Polygon hole = new Polygon(new double[] {0.5, 0.75, 0.75, 0.5, 0.5},
                               new double[] {100.5, 100.5, 100.75, 100.75, 100.5});
    Polygon expected = new Polygon(new double[] {0.0, 0.0, 1.0, 1.0, 0.0},    
                                   new double[] {100.0, 101.0, 101.0, 100.0, 100.0}, hole);
    Polygon[] polygons = Polygon.fromGeoJSON(b.toString());

    assertEquals(1, polygons.length);
    assertEquals(expected, polygons[0]);
  }

  // a MultiPolygon returns multiple Polygons
  public void testGeoJSONMultiPolygon() throws Exception {
    StringBuilder b = new StringBuilder();
    b.append("{\n");
    b.append("  \"type\": \"MultiPolygon\",\n");
    b.append("  \"coordinates\": [\n");
    b.append("    [\n");
    b.append("      [ [100.0, 0.0], [101.0, 0.0], [101.0, 1.0],\n");
    b.append("        [100.0, 1.0], [100.0, 0.0] ]\n");
    b.append("    ],\n");
    b.append("    [\n");
    b.append("      [ [10.0, 2.0], [11.0, 2.0], [11.0, 3.0],\n");
    b.append("        [10.0, 3.0], [10.0, 2.0] ]\n");
    b.append("    ]\n");
    b.append("  ],\n");
    b.append("}\n");
     
    Polygon[] polygons = Polygon.fromGeoJSON(b.toString());
    assertEquals(2, polygons.length);
    assertEquals(new Polygon(new double[] {0.0, 0.0, 1.0, 1.0, 0.0},
                             new double[] {100.0, 101.0, 101.0, 100.0, 100.0}), polygons[0]);
    assertEquals(new Polygon(new double[] {2.0, 2.0, 3.0, 3.0, 2.0},
                             new double[] {10.0, 11.0, 11.0, 10.0, 10.0}), polygons[1]);
  }

  // make sure type can appear last (JSON allows arbitrary key/value order for objects)
  public void testGeoJSONTypeComesLast() throws Exception {
    StringBuilder b = new StringBuilder();
    b.append("{\n");
    b.append("  \"coordinates\": [\n");
    b.append("    [ [100.0, 0.0], [101.0, 0.0], [101.0, 1.0],\n");
    b.append("      [100.0, 1.0], [100.0, 0.0] ]\n");
    b.append("  ],\n");
    b.append("  \"type\": \"Polygon\",\n");
    b.append("}\n");
     
    Polygon[] polygons = Polygon.fromGeoJSON(b.toString());
    assertEquals(1, polygons.length);
    assertEquals(new Polygon(new double[] {0.0, 0.0, 1.0, 1.0, 0.0},
                             new double[] {100.0, 101.0, 101.0, 100.0, 100.0}), polygons[0]);
  }

  // make sure Polygon inside a type: Feature also works
  public void testGeoJSONPolygonFeature() throws Exception {
    StringBuilder b = new StringBuilder();
    b.append("{ \"type\": \"Feature\",\n");
    b.append("  \"geometry\": {\n");
    b.append("    \"type\": \"Polygon\",\n");
    b.append("    \"coordinates\": [\n");
    b.append("      [ [100.0, 0.0], [101.0, 0.0], [101.0, 1.0],\n");
    b.append("        [100.0, 1.0], [100.0, 0.0] ]\n");
    b.append("      ]\n");
    b.append("  },\n");
    b.append("  \"properties\": {\n");
    b.append("    \"prop0\": \"value0\",\n");
    b.append("    \"prop1\": {\"this\": \"that\"}\n");
    b.append("  }\n");
    b.append("}\n");
     
    Polygon[] polygons = Polygon.fromGeoJSON(b.toString());
    assertEquals(1, polygons.length);
    assertEquals(new Polygon(new double[] {0.0, 0.0, 1.0, 1.0, 0.0},
                             new double[] {100.0, 101.0, 101.0, 100.0, 100.0}), polygons[0]);
  }

  // make sure MultiPolygon inside a type: Feature also works
  public void testGeoJSONMultiPolygonFeature() throws Exception {
    StringBuilder b = new StringBuilder();
    b.append("{ \"type\": \"Feature\",\n");
    b.append("  \"geometry\": {\n");
    b.append("      \"type\": \"MultiPolygon\",\n");
    b.append("      \"coordinates\": [\n");
    b.append("        [\n");
    b.append("          [ [100.0, 0.0], [101.0, 0.0], [101.0, 1.0],\n");
    b.append("            [100.0, 1.0], [100.0, 0.0] ]\n");
    b.append("        ],\n");
    b.append("        [\n");
    b.append("          [ [10.0, 2.0], [11.0, 2.0], [11.0, 3.0],\n");
    b.append("            [10.0, 3.0], [10.0, 2.0] ]\n");
    b.append("        ]\n");
    b.append("      ]\n");
    b.append("  },\n");
    b.append("  \"properties\": {\n");
    b.append("    \"prop0\": \"value0\",\n");
    b.append("    \"prop1\": {\"this\": \"that\"}\n");
    b.append("  }\n");
    b.append("}\n");
     
    Polygon[] polygons = Polygon.fromGeoJSON(b.toString());
    assertEquals(2, polygons.length);
    assertEquals(new Polygon(new double[] {0.0, 0.0, 1.0, 1.0, 0.0},
                             new double[] {100.0, 101.0, 101.0, 100.0, 100.0}), polygons[0]);
    assertEquals(new Polygon(new double[] {2.0, 2.0, 3.0, 3.0, 2.0},
                             new double[] {10.0, 11.0, 11.0, 10.0, 10.0}), polygons[1]);
  }

  // FeatureCollection with one geometry is allowed:
  public void testGeoJSONFeatureCollectionWithSinglePolygon() throws Exception {
    StringBuilder b = new StringBuilder();
    b.append("{ \"type\": \"FeatureCollection\",\n");
    b.append("  \"features\": [\n");
    b.append("    { \"type\": \"Feature\",\n");
    b.append("      \"geometry\": {\n");
    b.append("        \"type\": \"Polygon\",\n");
    b.append("        \"coordinates\": [\n");
    b.append("          [ [100.0, 0.0], [101.0, 0.0], [101.0, 1.0],\n");
    b.append("            [100.0, 1.0], [100.0, 0.0] ]\n");
    b.append("          ]\n");
    b.append("      },\n");
    b.append("      \"properties\": {\n");
    b.append("        \"prop0\": \"value0\",\n");
    b.append("        \"prop1\": {\"this\": \"that\"}\n");
    b.append("      }\n");
    b.append("    }\n");
    b.append("  ]\n");
    b.append("}    \n");

    Polygon expected = new Polygon(new double[] {0.0, 0.0, 1.0, 1.0, 0.0},    
                                   new double[] {100.0, 101.0, 101.0, 100.0, 100.0});
    Polygon[] actual = Polygon.fromGeoJSON(b.toString());
    assertEquals(1, actual.length);
    assertEquals(expected, actual[0]);
  }

  // stuff after the object is not allowed
  public void testIllegalGeoJSONExtraCrapAtEnd() throws Exception {
    StringBuilder b = new StringBuilder();
    b.append("{\n");
    b.append("  \"type\": \"Polygon\",\n");
    b.append("  \"coordinates\": [\n");
    b.append("    [ [100.0, 0.0], [101.0, 0.0], [101.0, 1.0],\n");
    b.append("      [100.0, 1.0], [100.0, 0.0] ]\n");
    b.append("  ]\n");
    b.append("}\n");
    b.append("foo\n");
     
    Exception e = expectThrows(ParseException.class, () -> Polygon.fromGeoJSON(b.toString()));
    assertTrue(e.getMessage().contains("unexpected character 'f' after end of GeoJSON object"));
  }

  public void testIllegalGeoJSONLinkedCRS() throws Exception {

    StringBuilder b = new StringBuilder();
    b.append("{\n");
    b.append("  \"type\": \"Polygon\",\n");
    b.append("  \"coordinates\": [\n");
    b.append("    [ [100.0, 0.0], [101.0, 0.0], [101.0, 1.0],\n");
    b.append("      [100.0, 1.0], [100.0, 0.0] ]\n");
    b.append("  ],\n");
    b.append("  \"crs\": {\n");
    b.append("    \"type\": \"link\",\n");
    b.append("    \"properties\": {\n");
    b.append("      \"href\": \"http://example.com/crs/42\",\n");
    b.append("      \"type\": \"proj4\"\n");
    b.append("    }\n");
    b.append("  }    \n");
    b.append("}\n");
    Exception e = expectThrows(ParseException.class, () -> Polygon.fromGeoJSON(b.toString()));
    assertTrue(e.getMessage().contains("cannot handle linked crs"));
  }

  // FeatureCollection with more than one geometry is not supported:
  public void testIllegalGeoJSONMultipleFeatures() throws Exception {
    StringBuilder b = new StringBuilder();
    b.append("{ \"type\": \"FeatureCollection\",\n");
    b.append("  \"features\": [\n");
    b.append("    { \"type\": \"Feature\",\n");
    b.append("      \"geometry\": {\"type\": \"Point\", \"coordinates\": [102.0, 0.5]},\n");
    b.append("      \"properties\": {\"prop0\": \"value0\"}\n");
    b.append("    },\n");
    b.append("    { \"type\": \"Feature\",\n");
    b.append("      \"geometry\": {\n");
    b.append("      \"type\": \"LineString\",\n");
    b.append("      \"coordinates\": [\n");
    b.append("        [102.0, 0.0], [103.0, 1.0], [104.0, 0.0], [105.0, 1.0]\n");
    b.append("        ]\n");
    b.append("      },\n");
    b.append("      \"properties\": {\n");
    b.append("        \"prop0\": \"value0\",\n");
    b.append("        \"prop1\": 0.0\n");
    b.append("      }\n");
    b.append("    },\n");
    b.append("    { \"type\": \"Feature\",\n");
    b.append("      \"geometry\": {\n");
    b.append("        \"type\": \"Polygon\",\n");
    b.append("        \"coordinates\": [\n");
    b.append("          [ [100.0, 0.0], [101.0, 0.0], [101.0, 1.0],\n");
    b.append("            [100.0, 1.0], [100.0, 0.0] ]\n");
    b.append("          ]\n");
    b.append("      },\n");
    b.append("      \"properties\": {\n");
    b.append("        \"prop0\": \"value0\",\n");
    b.append("        \"prop1\": {\"this\": \"that\"}\n");
    b.append("      }\n");
    b.append("    }\n");
    b.append("  ]\n");
    b.append("}    \n");

    Exception e = expectThrows(ParseException.class, () -> Polygon.fromGeoJSON(b.toString()));
    assertTrue(e.getMessage().contains("can only handle type FeatureCollection (if it has a single polygon geometry), Feature, Polygon or MutiPolygon, but got Point"));
  }

  public void testPolygonPropertiesCanBeStringArrays() throws Exception {
    StringBuilder b = new StringBuilder();
    b.append("{\n");
    b.append("  \"type\": \"Polygon\",\n");
    b.append("  \"coordinates\": [\n");
    b.append("    [ [100.0, 0.0], [101.0, 0.0], [101.0, 1.0],\n");
    b.append("      [100.0, 1.0], [100.0, 0.0] ]\n");
    b.append("  ],\n");
    b.append("  \"properties\": {\n");
    b.append("    \"array\": [ \"value\" ]\n");
    b.append("  }\n");
    b.append("}\n");

    Polygon[] polygons = Polygon.fromGeoJSON(b.toString());
    assertEquals(1, polygons.length);
  }
}
