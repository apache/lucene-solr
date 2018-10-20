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

package org.apache.lucene.spatial3d.geom;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Check relationship between polygon and GeoShapes of composite polygons. Normally we construct
 * the composite polygon (when possible) and the complex one.
 */
public class CompositeGeoPolygonRelationshipsTest {

  @Test
  public void testGeoCompositePolygon1() {

    //POLYGON ((19.845091 -60.452631, 20.119948 -61.655652, 23.207901 -61.453298, 22.820804 -60.257713, 21 -61,19.845091 -60.452631))
    GeoPolygon originalConvexPol = buildGeoPolygon(19.84509, -60.452631,
        20.119948, -61.655652,
        23.207901, -61.453298,
        22.820804, -60.257713,
        21, -61);

    //POLYGON ((19.845091 -60.452631, 21 -61,22.820804 -60.257713,23.207901 -61.453298, 20.119948 -61.655652, 19.845091 -60.452631))
    GeoPolygon originalConcavePol = buildGeoPolygon(19.84509, -60.452631,
        21, -61,
        22.820804, -60.257713,
        23.207901, -61.453298,
        20.119948, -61.655652);

    GeoPolygon polConvex = buildGeoPolygon(20.0, -60.4,
        20.1, -60.4,
        20.1, -60.3,
        20.0, -60.3,
        20.0, -60.3);

    GeoPolygon polConcave = buildConcaveGeoPolygon(20.0, -60.4,
        20.1, -60.4,
        20.1, -60.3,
        20.0, -60.3);

    //convex
    int rel = originalConvexPol.getRelationship(polConvex);
    assertEquals(GeoArea.DISJOINT, rel);
    rel = polConvex.getRelationship(originalConvexPol);
    assertEquals(GeoArea.DISJOINT, rel);


    rel = originalConvexPol.getRelationship(polConcave);
    assertEquals(GeoArea.CONTAINS, rel);
    rel = polConcave.getRelationship(originalConvexPol);
    assertEquals(GeoArea.WITHIN, rel);

    //concave
    rel = originalConcavePol.getRelationship(polConvex);
    assertEquals(GeoArea.WITHIN, rel);
    rel = polConvex.getRelationship(originalConcavePol);
    assertEquals(GeoArea.CONTAINS, rel);

    rel = originalConcavePol.getRelationship(polConcave);
    assertEquals(GeoArea.OVERLAPS, rel);
    rel = polConcave.getRelationship(originalConcavePol);
    assertEquals(GeoArea.OVERLAPS, rel);
  }

  @Test
  public void testGeoCompositePolygon2() {

    //POLYGON ((19.845091 -60.452631, 20.119948 -61.655652, 23.207901 -61.453298, 22.820804 -60.257713, 21 -61,19.845091 -60.452631))
    GeoPolygon originalConvexPol = buildGeoPolygon(19.84509, -60.452631,
        20.119948, -61.655652,
        23.207901, -61.453298,
        22.820804, -60.257713,
        21, -61);

    //POLYGON ((19.845091 -60.452631, 21 -61,22.820804 -60.257713,23.207901 -61.453298, 20.119948 -61.655652, 19.845091 -60.452631))
    GeoPolygon originalConcavePol = buildGeoPolygon(19.84509, -60.452631,
        21, -61,
        22.820804, -60.257713,
        23.207901, -61.453298,
        20.119948, -61.655652);

    //POLYGON ((20.9 -60.8, 21.1 -60.8, 21.1 -60.6, 20.9  -60.6,20.9 -60.8))
    GeoPolygon polConvex = buildGeoPolygon(20.9, -60.8,
        21.1, -60.8,
        21.1, -60.6,
        20.9, -60.6,
        20.9, -60.6);

    GeoPolygon polConcave = buildConcaveGeoPolygon(20.9, -60.8,
        21.1, -60.8,
        21.1, -60.6,
        20.9, -60.6);

    //convex
    int rel = originalConvexPol.getRelationship(polConvex);
    assertEquals(GeoArea.DISJOINT, rel);
    rel = polConvex.getRelationship(originalConvexPol);
    assertEquals(GeoArea.DISJOINT, rel);

    rel = originalConvexPol.getRelationship(polConcave);
    assertEquals(GeoArea.CONTAINS, rel);
    rel = polConcave.getRelationship(originalConvexPol);
    assertEquals(GeoArea.WITHIN, rel);

    //concave
    rel = originalConcavePol.getRelationship(polConvex);
    assertEquals(GeoArea.WITHIN, rel);
    rel = polConvex.getRelationship(originalConcavePol);
    assertEquals(GeoArea.CONTAINS, rel);

    rel = originalConcavePol.getRelationship(polConcave);
    assertEquals(GeoArea.OVERLAPS, rel);
    rel = polConcave.getRelationship(originalConcavePol);
    assertEquals(GeoArea.OVERLAPS, rel);
  }

  @Test
  public void testGeoCompositePolygon3() {

    //POLYGON ((19.845091 -60.452631, 20.119948 -61.655652, 23.207901 -61.453298, 22.820804 -60.257713, 21 -61,19.845091 -60.452631))
    GeoPolygon originalConvexPol = buildGeoPolygon(19.84509, -60.452631,
        20.119948, -61.655652,
        23.207901, -61.453298,
        22.820804, -60.257713,
        21, -61);

    //POLYGON ((19.845091 -60.452631, 21 -61,22.820804 -60.257713,23.207901 -61.453298, 20.119948 -61.655652, 19.845091 -60.452631))
    GeoPolygon originalConcavePol = buildGeoPolygon(19.84509, -60.452631,
        21, -61,
        22.820804, -60.257713,
        23.207901, -61.453298,
        20.119948, -61.655652);

    //POLYGON ((20.9 -61.1, 21.1 -61.1, 21.1 -60.9, 20.9  -60.9,20.9 -61.1))
    GeoPolygon polConvex = buildGeoPolygon(20.9, -61.1,
        21.1, -61.1,
        21.1, -60.9,
        20.9, -60.9,
        20.9, -60.9);

    GeoPolygon polConcave = buildConcaveGeoPolygon(20.9, -61.1,
        21.1, -61.1,
        21.1, -60.9,
        20.9, -60.9);

    //convex
    int rel = originalConvexPol.getRelationship(polConvex);
    assertEquals(GeoArea.OVERLAPS, rel);
    rel = polConvex.getRelationship(originalConvexPol);
    assertEquals(GeoArea.OVERLAPS, rel);

    rel = originalConvexPol.getRelationship(polConcave);
    assertEquals(GeoArea.OVERLAPS, rel);
    rel = polConcave.getRelationship(originalConvexPol);
    assertEquals(GeoArea.OVERLAPS, rel);

    //concave
    rel = originalConcavePol.getRelationship(polConvex);
    assertEquals(GeoArea.OVERLAPS, rel);
    rel = polConvex.getRelationship(originalConcavePol);
    assertEquals(GeoArea.OVERLAPS, rel);

    rel = originalConcavePol.getRelationship(polConcave);
    assertEquals(GeoArea.OVERLAPS, rel);
    rel = polConcave.getRelationship(originalConcavePol);
    assertEquals(GeoArea.OVERLAPS, rel);
  }

  @Test
  public void testGeoCompositePolygon4() {

    //POLYGON ((19.845091 -60.452631, 20.119948 -61.655652, 23.207901 -61.453298, 22.820804 -60.257713, 21 -61,19.845091 -60.452631))
    GeoPolygon originalConvexPol = buildGeoPolygon(19.84509, -60.452631,
        20.119948, -61.655652,
        23.207901, -61.453298,
        22.820804, -60.257713,
        21, -61);

    //POLYGON ((19.845091 -60.452631, 21 -61,22.820804 -60.257713,23.207901 -61.453298, 20.119948 -61.655652, 19.845091 -60.452631))
    GeoPolygon originalConcavePol = buildGeoPolygon(19.84509, -60.452631,
        21, -61,
        22.820804, -60.257713,
        23.207901, -61.453298,
        20.119948, -61.655652);

    //POLYGON ((20.9 -61.4, 21.1 -61.4, 21.1 -61.2, 20.9  -61.2,20.9 -61.4))
    GeoPolygon polConvex = buildGeoPolygon(20.9, -61.4,
        21.1, -61.4,
        21.1, -61.2,
        20.9, -61.2,
        20.9, -61.2);

    GeoPolygon polConcave = buildConcaveGeoPolygon(20.9, -61.4,
        21.1, -61.4,
        21.1, -61.2,
        20.9, -61.2);

    //convex
    int rel = originalConvexPol.getRelationship(polConvex);
    assertEquals(GeoArea.WITHIN, rel);
    rel = polConvex.getRelationship(originalConvexPol);
    assertEquals(GeoArea.CONTAINS, rel);

    rel = originalConvexPol.getRelationship(polConcave);
    assertEquals(GeoArea.OVERLAPS, rel);
    rel = polConcave.getRelationship(originalConvexPol);
    assertEquals(GeoArea.OVERLAPS, rel);

    //concave
    rel = originalConcavePol.getRelationship(polConvex);
    assertEquals(GeoArea.DISJOINT, rel);
    rel = polConvex.getRelationship(originalConcavePol);
    assertEquals(GeoArea.DISJOINT, rel);

    rel = originalConcavePol.getRelationship(polConcave);
    assertEquals(GeoArea.CONTAINS, rel);
    rel = polConcave.getRelationship(originalConcavePol);
    assertEquals(GeoArea.WITHIN, rel);

  }

  @Test
  public void testGeoCompositePolygon5() {

    //POLYGON ((19.845091 -60.452631, 20.119948 -61.655652, 23.207901 -61.453298, 22.820804 -60.257713, 21 -61,19.845091 -60.452631))
    GeoPolygon originaConvexlPol = buildGeoPolygon(19.84509, -60.452631,
        20.119948, -61.655652,
        23.207901, -61.453298,
        22.820804, -60.257713,
        21, -61);

    //POLYGON ((19.845091 -60.452631, 21 -61,22.820804 -60.257713,23.207901 -61.453298, 20.119948 -61.655652, 19.845091 -60.452631))
    GeoPolygon originalConcavePol = buildGeoPolygon(19.84509, -60.452631,
        21, -61,
        22.820804, -60.257713,
        23.207901, -61.453298,
        20.119948, -61.655652);

    //POLYGON ((19 -62, 23 -62, 23 -60, 19 -60,19 -62))
    GeoPolygon polConvex = buildGeoPolygon(19, -62,
        23, -62,
        23, -60,
        19, -60,
        19, -60);

    GeoPolygon polConcave = buildConcaveGeoPolygon(19, -62,
        23, -62,
        23, -60,
        19, -60);

    //convex
    int rel = originaConvexlPol.getRelationship(polConvex);
    assertEquals(GeoArea.OVERLAPS, rel);
    rel = polConvex.getRelationship(originaConvexlPol);
    assertEquals(GeoArea.OVERLAPS, rel);

    rel = originaConvexlPol.getRelationship(polConcave);
    assertEquals(GeoArea.OVERLAPS, rel);
    rel = polConcave.getRelationship(originaConvexlPol);
    assertEquals(GeoArea.OVERLAPS, rel);

    //concave
    rel = originalConcavePol.getRelationship(polConvex);
    assertEquals(GeoArea.OVERLAPS, rel);
    rel = polConvex.getRelationship(originalConcavePol);
    assertEquals(GeoArea.OVERLAPS, rel);

    rel = originalConcavePol.getRelationship(polConcave);
    assertEquals(GeoArea.OVERLAPS, rel);
    rel = polConcave.getRelationship(originalConcavePol);
    assertEquals(GeoArea.OVERLAPS, rel);
  }

  @Test
  public void testGeoCompositePolygon6() {

    //POLYGON ((19.845091 -60.452631, 20.119948 -61.655652, 23.207901 -61.453298, 22.820804 -60.257713, 21 -61,19.845091 -60.452631))
    GeoPolygon originalConvexPol = buildGeoPolygon(19.84509, -60.452631,
        20.119948, -61.655652,
        23.207901, -61.453298,
        22.820804, -60.257713,
        21, -61);

    //POLYGON ((19.845091 -60.452631, 21 -61,22.820804 -60.257713,23.207901 -61.453298, 20.119948 -61.655652, 19.845091 -60.452631))
    GeoPolygon originalConcavePol = buildGeoPolygon(19.84509, -60.452631,
        21, -61,
        22.820804, -60.257713,
        23.207901, -61.453298,
        20.119948, -61.655652);

    //POLYGON ((19 -62, 24 -62, 24 -60, 19 -60,19 -62))
    GeoPolygon polConvex = buildGeoPolygon(19, -62,
        24, -62,
        24, -60,
        19, -60,
        19, -60);

    GeoPolygon polConcave = buildConcaveGeoPolygon(19, -62,
        24, -62,
        24, -60,
        19, -60);

    //convex
    int rel = originalConvexPol.getRelationship(polConvex);
    assertEquals(GeoArea.CONTAINS, rel);
    rel = polConvex.getRelationship(originalConvexPol);
    assertEquals(GeoArea.WITHIN, rel);

    rel = originalConvexPol.getRelationship(polConcave);
    assertEquals(GeoArea.DISJOINT, rel);
    rel = polConcave.getRelationship(originalConvexPol);
    assertEquals(GeoArea.DISJOINT, rel);

    //concave
    rel = originalConcavePol.getRelationship(polConvex);
    assertEquals(GeoArea.OVERLAPS, rel);
    rel = polConvex.getRelationship(originalConcavePol);
    assertEquals(GeoArea.OVERLAPS, rel);

    rel = originalConcavePol.getRelationship(polConcave);
    assertEquals(GeoArea.WITHIN, rel);
    rel = polConcave.getRelationship(originalConcavePol);
    assertEquals(GeoArea.CONTAINS, rel);
  }



  @Test
  public void testGeoCompositePolygon7() {

    //POLYGON ((19.845091 -60.452631, 20.119948 -61.655652, 23.207901 -61.453298, 22.820804 -60.257713, 21 -61,19.845091 -60.452631))
    GeoPolygon originalConvexPol = buildGeoPolygon(19.84509, -60.452631,
        20.119948, -61.655652,
        23.207901, -61.453298,
        22.820804, -60.257713,
        21, -61);

    //POLYGON ((19.845091 -60.452631, 21 -61,22.820804 -60.257713,23.207901 -61.453298, 20.119948 -61.655652, 19.845091 -60.452631))
    GeoPolygon originalConcavePol = buildGeoPolygon(19.84509, -60.452631,
        21, -61,
        22.820804, -60.257713,
        23.207901, -61.453298,
        20.119948, -61.655652);

    //POLYGON ((20.2 -61.4, 20.5 -61.4, 20.5 -60.8, 20.2 -60.8,20.2  -61.4))
    GeoPolygon polConvex = buildGeoPolygon(20.2, -61.4,
        20.5, -61.4,
        20.5, -60.8,
        20.2, -60.8,
        20.2, -60.8);

    GeoPolygon polConcave = buildConcaveGeoPolygon(20.2, -61.4,
        20.5, -61.4,
        20.5, -60.8,
        20.2, -60.8);

    //convex
    int rel = originalConvexPol.getRelationship(polConvex);
    assertEquals(GeoArea.WITHIN, rel);
    rel = polConvex.getRelationship(originalConvexPol);
    assertEquals(GeoArea.CONTAINS, rel);

    rel = originalConvexPol.getRelationship(polConcave);
    assertEquals(GeoArea.OVERLAPS, rel);
    rel = polConcave.getRelationship(originalConvexPol);
    assertEquals(GeoArea.OVERLAPS, rel);

    //concave
    rel = originalConcavePol.getRelationship(polConvex);
    assertEquals(GeoArea.DISJOINT, rel);
    rel = polConvex.getRelationship(originalConvexPol);
    assertEquals(GeoArea.CONTAINS, rel);

    rel = originalConvexPol.getRelationship(polConcave);
    assertEquals(GeoArea.OVERLAPS, rel);
    rel = polConcave.getRelationship(originalConvexPol);
    assertEquals(GeoArea.OVERLAPS, rel);
  }

  @Test
  public void testGeoCompositePolygon8() {

    //POLYGON ((19.845091 -60.452631, 20.119948 -61.655652, 23.207901 -61.453298, 22.820804 -60.257713,21 -61, 19.845091 -60.452631))
    GeoPolygon originalPol = buildGeoPolygon(19.84509, -60.452631,
        20.119948, -61.655652,
        23.207901, -61.453298,
        22.820804, -60.257713,
        21, -61);


    GeoShape shape  = getInsideCompositeShape();

    int rel = originalPol.getRelationship(shape);
    assertEquals(GeoArea.WITHIN, rel);

  }


  @Test
  public void testGeoPolygonPole1() {
    //POLYGON((0 80, 45 85 ,90 80,135 85,180 80, -135 85, -90 80, -45 85,0 80))
    GeoPolygon compositePol=  getCompositePolygon();
    GeoPolygon complexPol=  getComplexPolygon();

    //POLYGON ((20.9 -61.4, 21.1 -61.4, 21.1 -61.2, 20.9  -61.2,20.9 -61.4))
    GeoPolygon polConvex = buildGeoPolygon(20.9, -61.4,
        21.1, -61.4,
        21.1, -61.2,
        20.9, -61.2,
        20.9, -61.2);

    GeoPolygon polConcave = buildConcaveGeoPolygon(20.9, -61.4,
        21.1, -61.4,
        21.1, -61.2,
        20.9, -61.2);

    int rel = compositePol.getRelationship(polConvex);
    assertEquals(GeoArea.DISJOINT, rel);
    rel = polConvex.getRelationship(compositePol);
    assertEquals(GeoArea.DISJOINT, rel);

    rel = compositePol.getRelationship(polConcave);
    assertEquals(GeoArea.CONTAINS, rel);
    rel = polConcave.getRelationship(compositePol);
    assertEquals(GeoArea.WITHIN, rel);

    rel = complexPol.getRelationship(polConvex);
    assertEquals(GeoArea.DISJOINT, rel);
    rel = polConvex.getRelationship(complexPol);
    assertEquals(GeoArea.DISJOINT, rel);

    rel = complexPol.getRelationship(polConcave);
    assertEquals(GeoArea.CONTAINS, rel);
    rel = polConcave.getRelationship(complexPol);
    assertEquals(GeoArea.WITHIN, rel);
  }

  @Test
  public void testGeoPolygonPole2() {
    //POLYGON((0 80, 45 85 ,90 80,135 85,180 80, -135 85, -90 80, -45 85,0 80))
    GeoPolygon compositePol=  getCompositePolygon();
    GeoPolygon complexPol=  getComplexPolygon();

    //POLYGON((-1 81, -1 79,1 79,1 81, -1 81))
    GeoPolygon polConvex = buildGeoPolygon(-1,81,
        -1,79,
        1,79,
        1,81,
        1,81);

    GeoPolygon polConcave = buildConcaveGeoPolygon(-1,81,
        -1,79,
        1,79,
        1,81);

    int rel = compositePol.getRelationship(polConvex);
    assertEquals(GeoArea.OVERLAPS, rel);
    rel = polConvex.getRelationship(compositePol);
    assertEquals(GeoArea.OVERLAPS, rel);

    rel = compositePol.getRelationship(polConcave);
    assertEquals(GeoArea.OVERLAPS, rel);
    rel = polConcave.getRelationship(compositePol);
    assertEquals(GeoArea.OVERLAPS, rel);

    rel = complexPol.getRelationship(polConvex);
    assertEquals(GeoArea.OVERLAPS, rel);
    rel = polConvex.getRelationship(complexPol);
    assertEquals(GeoArea.OVERLAPS, rel);

    rel = complexPol.getRelationship(polConcave);
    assertEquals(GeoArea.OVERLAPS, rel);
    rel = polConcave.getRelationship(complexPol);
    assertEquals(GeoArea.OVERLAPS, rel);
  }

  @Test
  public void testGeoPolygonPole3() {
    //POLYGON((0 80, 45 85 ,90 80,135 85,180 80, -135 85, -90 80, -45 85,0 80))
    GeoPolygon compositePol=  getCompositePolygon();
    GeoPolygon complexPol=  getComplexPolygon();

    //POLYGON((-1 86, -1 84,1 84,1 86, -1 86))
    GeoPolygon polConvex = buildGeoPolygon(-1,86,
        -1,84,
        1,84,
        1,86,
        1,86);

    GeoPolygon polConcave = buildConcaveGeoPolygon(-1,86,
        -1,84,
        1,84,
        1,86);

    int rel = compositePol.getRelationship(polConvex);
    assertEquals(GeoArea.WITHIN, rel);
    rel = polConvex.getRelationship(compositePol);
    assertEquals(GeoArea.CONTAINS, rel);

    rel = compositePol.getRelationship(polConcave);
    assertEquals(GeoArea.OVERLAPS, rel);
    rel = polConcave.getRelationship(compositePol);
    assertEquals(GeoArea.OVERLAPS, rel);

    rel = complexPol.getRelationship(polConvex);
    assertEquals(GeoArea.WITHIN, rel);
    rel = polConvex.getRelationship(complexPol);
    assertEquals(GeoArea.CONTAINS, rel);

    rel = complexPol.getRelationship(polConcave);
    assertEquals(GeoArea.OVERLAPS, rel);
    rel = polConcave.getRelationship(complexPol);
    assertEquals(GeoArea.OVERLAPS, rel);
  }

  @Test
  public void testMultiPolygon1() {
    //MULTIPOLYGON(((-145.790967486 -5.17543698881, -145.790854979 -5.11348060995, -145.853073512 -5.11339421216, -145.853192037 -5.17535061936, -145.790967486 -5.17543698881)),
    //((-145.8563923 -5.17527125408, -145.856222168 -5.11332154814, -145.918433943 -5.11317773171, -145.918610092 -5.17512738429, -145.8563923 -5.17527125408)))
    GeoPolygon multiPol=  getMultiPolygon();

    //POLYGON((-145.8555 -5.13, -145.8540 -5.13, -145.8540 -5.12, -145.8555 -5.12, -145.8555 -5.13))
    GeoPolygon polConvex = buildGeoPolygon(-145.8555, -5.13,
        -145.8540, -5.13,
        -145.8540, -5.12,
        -145.8555, -5.12,
        -145.8555, -5.12);

    GeoPolygon polConcave = buildConcaveGeoPolygon(-145.8555, -5.13,
        -145.8540, -5.13,
        -145.8540, -5.12,
        -145.8555, -5.12);

    int rel = multiPol.getRelationship(polConvex);
    assertEquals(GeoArea.DISJOINT, rel);
    rel = polConvex.getRelationship(multiPol);
    assertEquals(GeoArea.DISJOINT, rel);
    assertEquals(false,multiPol.intersects(polConvex));
    assertEquals(false,polConvex.intersects(multiPol));

    rel = multiPol.getRelationship(polConcave);
    assertEquals(GeoArea.CONTAINS, rel);
    rel = polConcave.getRelationship(multiPol);
    assertEquals(GeoArea.WITHIN, rel);
    assertEquals(false,multiPol.intersects(polConcave));
    assertEquals(false,polConcave.intersects(multiPol));
  }

  @Test
  public void testMultiPolygon2() {
    //MULTIPOLYGON(((-145.790967486 -5.17543698881, -145.790854979 -5.11348060995, -145.853073512 -5.11339421216, -145.853192037 -5.17535061936, -145.790967486 -5.17543698881)),
    //((-145.8563923 -5.17527125408, -145.856222168 -5.11332154814, -145.918433943 -5.11317773171, -145.918610092 -5.17512738429, -145.8563923 -5.17527125408)))
    GeoPolygon multiPol=  getMultiPolygon();

    //POLYGON((-145.8555 -5.13, -145.85 -5.13, -145.85 -5.12, -145.8555 -5.12, -145.8555 -5.13))
    GeoPolygon polConvex = buildGeoPolygon(-145.8555, -5.13,
        -145.85, -5.13,
        -145.85, -5.12,
        -145.8555, -5.12,
        -145.8555, -5.12);

    GeoPolygon polConcave = buildConcaveGeoPolygon(-145.8555, -5.13,
        -145.85, -5.13,
        -145.85, -5.12,
        -145.8555, -5.12);

    int rel = multiPol.getRelationship(polConvex);
    assertEquals(GeoArea.OVERLAPS, rel);
    rel = polConvex.getRelationship(multiPol);
    assertEquals(GeoArea.OVERLAPS, rel);
    assertEquals(true,multiPol.intersects(polConvex));
    assertEquals(true,polConvex.intersects(multiPol));

    rel = multiPol.getRelationship(polConcave);
    assertEquals(GeoArea.OVERLAPS, rel);
    rel = polConcave.getRelationship(multiPol);
    assertEquals(GeoArea.OVERLAPS, rel);
    assertEquals(true,multiPol.intersects(polConcave));
    assertEquals(true,polConcave.intersects(multiPol));
  }

  @Test
  public void testMultiPolygon3() {
    //MULTIPOLYGON(((-145.790967486 -5.17543698881, -145.790854979 -5.11348060995, -145.853073512 -5.11339421216, -145.853192037 -5.17535061936, -145.790967486 -5.17543698881)),
    //((-145.8563923 -5.17527125408, -145.856222168 -5.11332154814, -145.918433943 -5.11317773171, -145.918610092 -5.17512738429, -145.8563923 -5.17527125408)))
    GeoPolygon multiPol=  getMultiPolygon();

    //POLYGON((-146 -5.18, -145.854 -5.18, -145.854 -5.11, -146 -5.11, -146 -5.18))
    //Case overlapping one of the polygons so intersection is false!
    GeoPolygon polConvex = buildGeoPolygon(-146, -5.18,
        -145.854, -5.18,
        -145.854, -5.11,
        -146, -5.11,
        -146, -5.11);

    GeoPolygon polConcave = buildConcaveGeoPolygon(-146, -5.18,
        -145.854, -5.18,
        -145.854, -5.11,
        -146, -5.11);

    int rel = multiPol.getRelationship(polConvex);
    assertEquals(GeoArea.OVERLAPS, rel);
    rel = polConvex.getRelationship(multiPol);
    assertEquals(GeoArea.OVERLAPS, rel);
    assertEquals(false,multiPol.intersects(polConvex));
    assertEquals(false,polConvex.intersects(multiPol));

    rel = multiPol.getRelationship(polConcave);
    assertEquals(GeoArea.OVERLAPS, rel);
    rel = polConcave.getRelationship(multiPol);
    assertEquals(GeoArea.OVERLAPS, rel);
    assertEquals(false,multiPol.intersects(polConcave));
    assertEquals(false,polConcave.intersects(multiPol));

  }

  @Test
  public void testMultiPolygon4() {
    //MULTIPOLYGON(((-145.790967486 -5.17543698881, -145.790854979 -5.11348060995, -145.853073512 -5.11339421216, -145.853192037 -5.17535061936, -145.790967486 -5.17543698881)),
    //((-145.8563923 -5.17527125408, -145.856222168 -5.11332154814, -145.918433943 -5.11317773171, -145.918610092 -5.17512738429, -145.8563923 -5.17527125408)))
    GeoPolygon multiPol=  getMultiPolygon();

    //POLYGON((-145.88 -5.13, -145.87 -5.13, -145.87 -5.12, -145.88 -5.12, -145.88 -5.13))
    GeoPolygon polConvex = buildGeoPolygon(-145.88, -5.13,
        -145.87, -5.13,
        -145.87, -5.12,
        -145.88, -5.12,
        -145.88, -5.12);

    GeoPolygon polConcave = buildConcaveGeoPolygon(-145.88, -5.13,
        -145.87, -5.13,
        -145.87, -5.12,
        -145.88, -5.12);

    int rel = multiPol.getRelationship(polConvex);
    assertEquals(GeoArea.WITHIN, rel);
    rel = polConvex.getRelationship(multiPol);
    assertEquals(GeoArea.CONTAINS, rel);
    assertEquals(false,multiPol.intersects(polConvex));
    assertEquals(false,polConvex.intersects(multiPol));

    rel = multiPol.getRelationship(polConcave);
    assertEquals(GeoArea.OVERLAPS, rel);
    rel = polConcave.getRelationship(multiPol);
    assertEquals(GeoArea.OVERLAPS, rel);
    assertEquals(false,multiPol.intersects(polConcave));
    assertEquals(false,polConcave.intersects(multiPol));
  }

  @Test
  public void testMultiPolygon5() {
    //MULTIPOLYGON(((-145.790967486 -5.17543698881, -145.790854979 -5.11348060995, -145.853073512 -5.11339421216, -145.853192037 -5.17535061936, -145.790967486 -5.17543698881)),
    //((-145.8563923 -5.17527125408, -145.856222168 -5.11332154814, -145.918433943 -5.11317773171, -145.918610092 -5.17512738429, -145.8563923 -5.17527125408)))
    GeoPolygon multiPol=  getMultiPolygon();

    //POLYGON((-146 -5.18, -145 -5.18, -145 -5.11, -146 -5.11, -146 -5.18))
    GeoPolygon polConvex = buildGeoPolygon(-146, -5.18,
        -145, -5.18,
        -145, -5.11,
        -146, -5.11,
        -146, -5.11);

    GeoPolygon polConcave = buildConcaveGeoPolygon(-146, -5.18,
        -145, -5.18,
        -145, -5.11,
        -146, -5.11);

    int rel = multiPol.getRelationship(polConvex);
    assertEquals(GeoArea.CONTAINS, rel);
    rel = polConvex.getRelationship(multiPol);
    assertEquals(GeoArea.WITHIN, rel);
    assertEquals(false,multiPol.intersects(polConvex));

    rel = multiPol.getRelationship(polConcave);
    assertEquals(GeoArea.DISJOINT, rel);
    rel = polConcave.getRelationship(multiPol);
    assertEquals(GeoArea.DISJOINT, rel);
    assertEquals(false,multiPol.intersects(polConcave));
  }

  private GeoPolygon buildGeoPolygon(double lon1,double lat1,
                                     double lon2,double lat2,
                                     double lon3,double lat3,
                                     double lon4,double lat4,
                                     double lon5,double lat5)
  {
    GeoPoint point1 = new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(lat1), Geo3DUtil.fromDegrees(lon1));
    GeoPoint point2 = new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(lat2), Geo3DUtil.fromDegrees(lon2));
    GeoPoint point3 = new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(lat3), Geo3DUtil.fromDegrees(lon3));
    GeoPoint point4 = new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(lat4), Geo3DUtil.fromDegrees(lon4));
    GeoPoint point5 = new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(lat5), Geo3DUtil.fromDegrees(lon5));
    final List<GeoPoint> points = new ArrayList<>();
    points.add(point1);
    points.add(point2);
    points.add(point3);
    points.add(point4);
    points.add(point5);
    return GeoPolygonFactory.makeGeoPolygon(PlanetModel.SPHERE, points);
  }

  private GeoPolygon buildConcaveGeoPolygon(double lon1,double lat1,
                                            double lon2,double lat2,
                                            double lon3,double lat3,
                                            double lon4,double lat4)
  {
    GeoPoint point1 = new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(lat1), Geo3DUtil.fromDegrees(lon1));
    GeoPoint point2 = new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(lat2), Geo3DUtil.fromDegrees(lon2));
    GeoPoint point3 = new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(lat3), Geo3DUtil.fromDegrees(lon3));
    GeoPoint point4 = new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(lat4), Geo3DUtil.fromDegrees(lon4));
    final List<GeoPoint> points = new ArrayList<>();
    points.add(point1);
    points.add(point2);
    points.add(point3);
    points.add(point4);
    return GeoPolygonFactory.makeGeoConcavePolygon(PlanetModel.SPHERE,points);
  }

  private GeoPolygon getCompositePolygon(){
    //POLYGON((0 80, 45 85 ,90 80,135 85,180 80, -135 85, -90 80, -45 85,0 80))
    GeoPoint point1 = new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(80), Geo3DUtil.fromDegrees(0));
    GeoPoint point2 = new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(85), Geo3DUtil.fromDegrees(45));
    GeoPoint point3 = new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(80), Geo3DUtil.fromDegrees(90));
    GeoPoint point4 = new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(85), Geo3DUtil.fromDegrees(135));
    GeoPoint point5 = new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(80), Geo3DUtil.fromDegrees(180));
    GeoPoint point6 = new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(85), Geo3DUtil.fromDegrees(-135));
    GeoPoint point7 = new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(80), Geo3DUtil.fromDegrees(-90));
    GeoPoint point8 = new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(85), Geo3DUtil.fromDegrees(-45));
    final List<GeoPoint> points = new ArrayList<>();
    points.add(point1);
    points.add(point2);
    points.add(point3);
    points.add(point4);
    points.add(point5);
    points.add(point6);
    points.add(point7);
    points.add(point8);
    return GeoPolygonFactory.makeGeoPolygon(PlanetModel.SPHERE, points);
  }

  private GeoPolygon getComplexPolygon(){
    //POLYGON((0 80, 45 85 ,90 80,135 85,180 80, -135 85, -90 80, -45 85,0 80))
    GeoPoint point1 = new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(80), Geo3DUtil.fromDegrees(0));
    GeoPoint point2 = new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(85), Geo3DUtil.fromDegrees(45));
    GeoPoint point3 = new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(80), Geo3DUtil.fromDegrees(90));
    GeoPoint point4 = new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(85), Geo3DUtil.fromDegrees(135));
    GeoPoint point5 = new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(80), Geo3DUtil.fromDegrees(180));
    GeoPoint point6 = new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(85), Geo3DUtil.fromDegrees(-135));
    GeoPoint point7 = new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(80), Geo3DUtil.fromDegrees(-90));
    GeoPoint point8 = new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(85), Geo3DUtil.fromDegrees(-45));
    final List<GeoPoint> points = new ArrayList<>();
    points.add(point1);
    points.add(point2);
    points.add(point3);
    points.add(point4);
    points.add(point5);
    points.add(point6);
    points.add(point7);
    points.add(point8);
    GeoPolygonFactory.PolygonDescription pd = new GeoPolygonFactory.PolygonDescription(points);
    return GeoPolygonFactory.makeLargeGeoPolygon(PlanetModel.SPHERE, Collections.singletonList(pd));
  }

  private GeoPolygon getMultiPolygon(){
    //MULTIPOLYGON(((-145.790967486 -5.17543698881, -145.790854979 -5.11348060995, -145.853073512 -5.11339421216, -145.853192037 -5.17535061936, -145.790967486 -5.17543698881)),
    //((-145.8563923 -5.17527125408, -145.856222168 -5.11332154814, -145.918433943 -5.11317773171, -145.918610092 -5.17512738429, -145.8563923 -5.17527125408)))
    GeoPoint point1 = new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(-5.17543698881), Geo3DUtil.fromDegrees(-145.790967486));
    GeoPoint point2 = new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(-5.11348060995), Geo3DUtil.fromDegrees(-145.790854979));
    GeoPoint point3 = new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(-5.11339421216), Geo3DUtil.fromDegrees(-145.853073512));
    GeoPoint point4 = new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(-5.17535061936), Geo3DUtil.fromDegrees(-145.853192037));
    final List<GeoPoint> points1 = new ArrayList<>();
    points1.add(point1);
    points1.add(point2);
    points1.add(point3);
    points1.add(point4);
    GeoPolygonFactory.PolygonDescription pd1 = new GeoPolygonFactory.PolygonDescription(points1);
    GeoPoint point5 = new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(-5.17527125408), Geo3DUtil.fromDegrees(-145.8563923));
    GeoPoint point6 = new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(-5.11332154814), Geo3DUtil.fromDegrees(-145.856222168));
    GeoPoint point7 = new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(-5.11317773171), Geo3DUtil.fromDegrees(-145.918433943));
    GeoPoint point8 = new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(-5.17512738429), Geo3DUtil.fromDegrees(-145.918610092));
    final List<GeoPoint> points2 = new ArrayList<>();
    points2.add(point5);
    points2.add(point6);
    points2.add(point7);
    points2.add(point8);
    GeoPolygonFactory.PolygonDescription pd2 = new GeoPolygonFactory.PolygonDescription(points2);
    final List<GeoPolygonFactory.PolygonDescription> pds = new ArrayList<>();
    pds.add(pd1);
    pds.add(pd2);
    return GeoPolygonFactory.makeLargeGeoPolygon(PlanetModel.SPHERE, pds);
  }

  public GeoShape getInsideCompositeShape(){
    //MULTIPOLYGON(((19.945091 -60.552631, 20.319948 -61.555652, 20.9 -61.5, 20.9 -61, 19.945091 -60.552631)),
    // ((21.1 -61.5,  23.107901 -61.253298, 22.720804 -60.457713,21.1 -61, 21.1 -61.5)))
    GeoPoint point1 = new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(-60.552631), Geo3DUtil.fromDegrees(19.945091));
    GeoPoint point2 = new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(-61.555652), Geo3DUtil.fromDegrees(20.319948));
    GeoPoint point3 = new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(-61.5), Geo3DUtil.fromDegrees(20.9));
    GeoPoint point4 = new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(-61), Geo3DUtil.fromDegrees(20.9));
    final List<GeoPoint> points1 = new ArrayList<>();
    points1.add(point1);
    points1.add(point2);
    points1.add(point3);
    points1.add(point4);
    GeoPoint point5 = new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(-61.5), Geo3DUtil.fromDegrees(21.1));
    GeoPoint point6 = new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(-61.253298), Geo3DUtil.fromDegrees(23.107901));
    GeoPoint point7 = new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(-60.457713), Geo3DUtil.fromDegrees(22.720804));
    GeoPoint point8 = new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(-61), Geo3DUtil.fromDegrees(21.1));
    final List<GeoPoint> points2 = new ArrayList<>();
    points2.add(point5);
    points2.add(point6);
    points2.add(point7);
    points2.add(point8);
    GeoPolygon p1 = GeoPolygonFactory.makeGeoPolygon(PlanetModel.SPHERE, points1);
    GeoPolygon p2 = GeoPolygonFactory.makeGeoPolygon(PlanetModel.SPHERE, points2);
    GeoCompositeMembershipShape compositeMembershipShape = new GeoCompositeMembershipShape(PlanetModel.SPHERE);
    compositeMembershipShape.addShape(p1);
    compositeMembershipShape.addShape(p2);
    return compositeMembershipShape;
  }

}
