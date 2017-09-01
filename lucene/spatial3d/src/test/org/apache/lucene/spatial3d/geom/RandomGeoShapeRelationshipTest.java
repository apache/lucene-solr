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

import com.carrotsearch.randomizedtesting.annotations.Repeat;
import org.junit.Test;

/**
 * Random test to check relationship between GeoAreaShapes and GeoShapes.
 */
public class RandomGeoShapeRelationshipTest extends RandomGeoShapeGenerator {


  /**
   * Test for WITHIN points. We build a WITHIN shape with respect the geoAreaShape
   * and create a point WITHIN the shape. The resulting shape should be WITHIN
   * the original shape.
   *
   */
  @Test
  @Repeat(iterations = 5)
  public void testRandomPointWithin() {
    int referenceShapeType = CONVEX_POLYGON;
    PlanetModel planetModel = randomPlanetModel();
    int shapeType = randomShapeType();
    GeoAreaShape shape = null;
    GeoPoint point = null;
    while (point == null) {
      shape = randomGeoAreaShape(shapeType, planetModel);
      Constraints constraints = getEmptyConstraint();
      constraints.put(shape, GeoArea.WITHIN);
      GeoAreaShape reference =  randomGeoAreaShape(referenceShapeType, planetModel, constraints);
      if (reference != null) {
        constraints = new Constraints();
        constraints.put(reference, GeoArea.WITHIN);
        point = randomGeoPoint(planetModel, constraints);
      }
    }
    assertTrue(shape.isWithin(point));
  }

  /**
   * Test for NOT WITHIN points. We build a DIJOINT shape with respect the geoAreaShape
   * and create a point WITHIN that shape. The resulting shape should not be WITHIN
   * the original shape.
   *
   */
  @Repeat(iterations = 5)
  public void testRandomPointNotWithin() {
    int referenceShapeType = CONVEX_POLYGON;
    PlanetModel planetModel = randomPlanetModel();
    int shapeType = randomShapeType();
    GeoAreaShape shape = null;
    GeoPoint point = null;
    while (point == null) {
      shape = randomGeoAreaShape(shapeType, planetModel);
      Constraints constraints = getEmptyConstraint();
      constraints.put(shape, GeoArea.DISJOINT);
      GeoAreaShape reference =  randomGeoAreaShape(referenceShapeType, planetModel, constraints);
      if (reference != null) {
        constraints = new Constraints();
        constraints.put(reference, GeoArea.WITHIN);
        point = randomGeoPoint(planetModel, constraints);
      }
    }
    assertFalse(shape.isWithin(point));
  }

  /**
   * Test for disjoint shapes. We build a DISJOINT shape with respect the geoAreaShape
   * and create shapes WITHIN that shapes. The resulting shape should be DISJOINT
   * to the geoAreaShape.
   *
   * Note that both shapes cannot be concave.
   */
  @Test
  @Repeat(iterations = 5)
  public void testRandomDisjoint() {
    int referenceShapeType = CONVEX_SIMPLE_POLYGON;
    PlanetModel planetModel = randomPlanetModel();
    int geoAreaShapeType = randomGeoAreaShapeType();
    int shapeType =randomConvexShapeType();

    GeoShape shape = null;
    GeoAreaShape geoAreaShape = null;
    while (shape == null) {
      geoAreaShape = randomGeoAreaShape(geoAreaShapeType, planetModel);
      Constraints constraints = new Constraints();
      constraints.put(geoAreaShape, GeoArea.DISJOINT);
      GeoAreaShape reference = randomGeoAreaShape(referenceShapeType, planetModel, constraints);
      if (reference != null) {
        constraints = getEmptyConstraint();
        constraints.put(reference, GeoArea.WITHIN);
        shape = randomGeoShape(shapeType, planetModel, constraints);
      }
    }
    int rel = geoAreaShape.getRelationship(shape);
    assertEquals(GeoArea.DISJOINT, rel);
    if (shape instanceof GeoArea) {
      rel = ((GeoArea)shape).getRelationship(geoAreaShape);
      assertEquals(GeoArea.DISJOINT, rel);
    }
  }

  /**
   * Test for within shapes. We build a shape WITHIN the geoAreaShape and create
   * shapes WITHIN that shape. The resulting shape should be WITHIN
   * to the geoAreaShape.
   *
   * Note that if the geoAreaShape is not concave the other shape must be not concave.
   */
  @Test
  @Repeat(iterations = 5)
  public void testRandomWithIn() {
    PlanetModel planetModel = randomPlanetModel();
    int geoAreaShapeType = randomGeoAreaShapeType();
    int shapeType =randomShapeType();
    int referenceShapeType = CONVEX_SIMPLE_POLYGON;
    if (!isConcave(geoAreaShapeType)){
      shapeType =randomConvexShapeType();
    }
    if(isConcave(shapeType)){//both concave
      referenceShapeType = CONCAVE_SIMPLE_POLYGON;
    }
    GeoShape shape = null;
    GeoAreaShape geoAreaShape = null;
    while (shape == null) {
      geoAreaShape = randomGeoAreaShape(geoAreaShapeType, planetModel);
      Constraints constraints = new Constraints();
      constraints.put(geoAreaShape, GeoArea.WITHIN);
      GeoAreaShape reference = randomGeoAreaShape(referenceShapeType, planetModel, constraints);
      if (reference != null) {
        constraints = new Constraints();
        constraints.put(reference, GeoArea.WITHIN);
        shape = randomGeoShape(shapeType, planetModel, constraints);
      }
    }
    int rel = geoAreaShape.getRelationship(shape);
    assertEquals(GeoArea.WITHIN, rel);
    if (shape instanceof GeoArea) {
      rel = ((GeoArea)shape).getRelationship(geoAreaShape);
      assertEquals(GeoArea.CONTAINS, rel);
    }
  }


  /**
   * Test for contains shapes. We build a shape containing the geoAreaShape and create
   * shapes WITHIN that shape. The resulting shape should CONTAIN
   * the geoAreaShape.
   *
   * Note that if the geoAreaShape is concave the other shape must be concave.
   * If shape is concave, the shape for reference should be concave as well.
   *
   */
  @Test
  @Repeat(iterations = 1)
  public void testRandomContains() {
    int referenceShapeType = CONVEX_SIMPLE_POLYGON;
    PlanetModel planetModel = randomPlanetModel();
    int geoAreaShapeType = randomGeoAreaShapeType();
    while (geoAreaShapeType == COLLECTION){
      geoAreaShapeType = randomGeoAreaShapeType();
    }
    int shapeType = randomShapeType();
    if (isConcave(geoAreaShapeType)){
      shapeType = randomConcaveShapeType();
    }
    if (isConcave(shapeType)){
      referenceShapeType = CONCAVE_SIMPLE_POLYGON;
    }
    GeoShape shape = null;
    GeoAreaShape geoAreaShape = null;
    while (shape == null) {
      geoAreaShape = randomGeoAreaShape(geoAreaShapeType, planetModel);
      Constraints constraints = getEmptyConstraint();
      constraints.put(geoAreaShape, GeoArea.CONTAINS);
      GeoPolygon reference =(GeoPolygon)randomGeoAreaShape(referenceShapeType, planetModel, constraints);
      if (reference != null) {
        constraints = getEmptyConstraint();
        constraints.put(reference, GeoArea.CONTAINS);
        shape = randomGeoShape(shapeType, planetModel, constraints);
      }
    }
    int rel = geoAreaShape.getRelationship(shape);
    assertEquals(GeoArea.CONTAINS, rel);
    if (shape instanceof GeoArea) {
      rel = ((GeoArea)shape).getRelationship(geoAreaShape);
      assertEquals(GeoArea.WITHIN, rel);
    }
  }

  /**
   * Test for overlapping shapes. We build a shape that contains part of the
   * geoAreaShape, is disjoint to other part and contains a disjoint shape. We create
   * shapes  according the criteria. The resulting shape should OVERLAP
   * the geoAreaShape.
   */
  @Test
  @Repeat(iterations = 5)
  public void testRandomOverlaps() {
    PlanetModel planetModel = randomPlanetModel();
    int geoAreaShapeType = randomGeoAreaShapeType();
    int shapeType = randomShapeType();

    GeoShape shape = null;
    GeoAreaShape geoAreaShape = null;
    while (shape == null) {
      geoAreaShape = randomGeoAreaShape(geoAreaShapeType, planetModel);
      Constraints constraints = getEmptyConstraint();
      constraints.put(geoAreaShape,GeoArea.WITHIN);
      GeoAreaShape reference1 = randomGeoAreaShape(CONVEX_SIMPLE_POLYGON, planetModel, constraints);
      if (reference1 == null){
        continue;
      }
      constraints = getEmptyConstraint();
      constraints.put(geoAreaShape, GeoArea.WITHIN);
      constraints.put(reference1, GeoArea.DISJOINT);
      GeoAreaShape reference2 = randomGeoAreaShape(CONVEX_SIMPLE_POLYGON, planetModel, constraints);
      if (reference2 == null){
        continue;
      }
      constraints = getEmptyConstraint();
      constraints.put(geoAreaShape, GeoArea.DISJOINT);
      GeoAreaShape reference3 = randomGeoAreaShape(CONVEX_SIMPLE_POLYGON, planetModel, constraints);
      if (reference3 != null) {
        constraints = new Constraints();
        constraints.put(reference1, GeoArea.DISJOINT);
        constraints.put(reference2, GeoArea.CONTAINS);
        constraints.put(reference3, GeoArea.CONTAINS);
        shape = randomGeoShape(shapeType, planetModel, constraints);
      }
    }
    int rel = geoAreaShape.getRelationship(shape);
    assertEquals(GeoArea.OVERLAPS, rel);
    if (shape instanceof GeoArea) {
      rel = ((GeoArea)shape).getRelationship(geoAreaShape);
      assertEquals(GeoArea.OVERLAPS, rel);
    }
  }
}

