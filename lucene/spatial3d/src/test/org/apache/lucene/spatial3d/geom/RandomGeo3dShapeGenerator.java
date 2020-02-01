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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.util.LuceneTestCase;

import static com.carrotsearch.randomizedtesting.RandomizedTest.randomDouble;

/**
 * Class for generating random Geo3dShapes. They can be generated under
 * given constraints which are expressed as a shape and a relationship.
 *
 * note that convexity for polygons is defined as polygons that contains
 * antipodal points, otherwise they are convex. Internally they can be
 * created using GeoConvexPolygons and GeoConcavePolygons.
 *
 */
public class RandomGeo3dShapeGenerator extends LuceneTestCase {

  /* Max num of iterations to find right shape under given constrains */
  final private static int MAX_SHAPE_ITERATIONS = 20;
  /* Max num of iterations to find right point under given constrains */
  final private static int MAX_POINT_ITERATIONS = 1000;

  /* Supported shapes */
  final protected static int CONVEX_POLYGON = 0;
  final protected static int CONVEX_POLYGON_WITH_HOLES = 1;
  final protected static int CONCAVE_POLYGON = 2;
  final protected static int CONCAVE_POLYGON_WITH_HOLES = 3;
  final protected static int COMPLEX_POLYGON = 4;
  final protected static int CIRCLE = 5;
  final protected static int RECTANGLE = 6;
  final protected static int PATH = 7;
  final protected static int COLLECTION = 8;
  final protected static int POINT = 9;
  final protected static int LINE = 10;
  final protected static int EXACT_CIRCLE = 11;

  /* Helper shapes for generating constraints whch are just three sided polygons */
  final protected static int CONVEX_SIMPLE_POLYGON = 500;
  final protected static int CONCAVE_SIMPLE_POLYGON = 501;

  /**
   * Method that returns a random generated Planet model from the supported
   * Planet models. currently SPHERE and WGS84
   *
   * @return a random generated Planet model
   */
  public PlanetModel randomPlanetModel() {
    final int shapeType = random().nextInt(2);
    switch (shapeType) {
      case 0: {
        return PlanetModel.SPHERE;
      }
      case 1: {
        return PlanetModel.WGS84;
      }
      default:
        throw new IllegalStateException("Unexpected planet model");
    }
  }

  /**
   * Method that returns a random generated a random Shape code from all
   * supported shapes.
   *
   * @return a random generated shape code
   */
  public int randomShapeType(){
    return random().nextInt(12);
  }

  /**
   * Method that returns a random generated GeoAreaShape code from all
   * supported GeoAreaShapes.
   *
   * We are removing Collections because it is difficult to create shapes
   * with properties in some cases.
   *
   * @return a random generated polygon code
   */
  public int randomGeoAreaShapeType(){
    return random().nextInt(12);
  }

  /**
   * Method that returns a random generated a random Shape code from all
   * convex supported shapes.
   *
   * @return a random generated convex shape code
   */
  public int randomConvexShapeType(){
    int shapeType = randomShapeType();
    while (isConcave(shapeType)){
      shapeType = randomShapeType();
    }
    return shapeType;
  }

  /**
   * Method that returns a random generated a random Shape code from all
   * concave supported shapes.
   *
   * @return a random generated concave shape code
   */
  public int randomConcaveShapeType(){
    int shapeType = randomShapeType();
    while (!isConcave(shapeType)){
      shapeType = randomShapeType();
    }
    return shapeType;
  }

  /**
   * Check if a shape code represents a concave shape
   *
   * @return true if the shape represented by the code is concave
   */
  public boolean isConcave(int shapeType){
    return (shapeType == CONCAVE_POLYGON);
  }

  /**
   * Method that returns empty Constraints object..
   *
   * @return an empty Constraints object
   */
  public Constraints getEmptyConstraint(){
    return new Constraints();
  }

  /**
   * Method that returns a random generated GeoPoint.
   *
   * @param planetModel The planet model.
   * @return The random generated GeoPoint.
   */
  public GeoPoint randomGeoPoint(PlanetModel planetModel) {
    GeoPoint point = null;
    while (point == null) {
      point = randomGeoPoint(planetModel, getEmptyConstraint());
    }
    return point;
  }

  /**
   * Method that returns a random generated GeoPoint under given constraints. Returns
   * NULL if it cannot find a point under the given constraints.
   *
   * @param planetModel The planet model.
   * @param constraints The given constraints.
   * @return The random generated GeoPoint.
   */
  public GeoPoint randomGeoPoint(PlanetModel planetModel, Constraints constraints) {
    int iterations = 0;
    while (iterations < MAX_POINT_ITERATIONS) {
      double lat = randomDouble() * Math.PI/2;
      if (random().nextBoolean()) {
        lat = (-1)*lat;
      }
      double lon =  randomDouble() * Math.PI;
      if (random().nextBoolean()) {
        lon = (-1)*lon;
      }
      iterations++;
      GeoPoint point = new GeoPoint(planetModel, lat, lon);
      if (constraints.isWithin(point)) {
        return point;
      }
    }
    return null;
  }

  /**
   * Method that returns a random generated GeoAreaShape.
   *
   * @param shapeType The GeoAreaShape code.
   * @param planetModel The planet model.
   * @return The random generated GeoAreaShape.
   */
  public GeoAreaShape randomGeoAreaShape(int shapeType, PlanetModel planetModel){
    GeoAreaShape geoAreaShape = null;
    while (geoAreaShape == null){
      geoAreaShape = randomGeoAreaShape(shapeType,planetModel,new Constraints());
    }
    return geoAreaShape;
  }

  /**
   * Method that returns a random generated GeoAreaShape under given constraints. Returns
   * NULL if it cannot build the GeoAreaShape under the given constraints.
   *
   * @param shapeType The GeoAreaShape code.
   * @param planetModel The planet model.
   * @param constraints The given constraints.
   * @return The random generated GeoAreaShape.
   */
  public GeoAreaShape randomGeoAreaShape(int shapeType, PlanetModel planetModel, Constraints constraints){
    return (GeoAreaShape)randomGeoShape(shapeType, planetModel, constraints);
  }

  /**
   * Method that returns a random generated GeoShape.
   *
   * @param shapeType The shape code.
   * @param planetModel The planet model.
   * @return The random generated GeoShape.
   */
  public GeoShape randomGeoShape(int shapeType, PlanetModel planetModel){
    GeoShape geoShape = null;
    while (geoShape == null){
      geoShape = randomGeoShape(shapeType,planetModel,new Constraints());
    }
    return geoShape;
  }

  /**
   * Method that returns a random generated GeoShape under given constraints. Returns
   * NULL if it cannot build the GeoShape under the given constraints.
   *
   * @param shapeType The polygon code.
   * @param planetModel The planet model.
   * @param constraints The given constraints.
   * @return The random generated GeoShape.
   */
  public GeoShape randomGeoShape(int shapeType, PlanetModel planetModel, Constraints constraints){
    switch (shapeType) {
      case CONVEX_POLYGON: {
        return convexPolygon(planetModel, constraints);
      }
      case CONVEX_POLYGON_WITH_HOLES: {
        return convexPolygonWithHoles(planetModel, constraints);
      }
      case CONCAVE_POLYGON: {
        return concavePolygon(planetModel, constraints);
      }
      case CONCAVE_POLYGON_WITH_HOLES: {
        return concavePolygonWithHoles(planetModel, constraints);
      }
      case COMPLEX_POLYGON: {
        return complexPolygon(planetModel, constraints);
      }
      case CIRCLE: {
        return circle(planetModel, constraints);
      }
      case RECTANGLE: {
        return rectangle(planetModel, constraints);
      }
      case PATH: {
        return path(planetModel, constraints);
      }
      case COLLECTION: {
        return collection(planetModel, constraints);
      }
      case POINT: {
        return point(planetModel, constraints);
      }
      case LINE: {
        return line(planetModel, constraints);
      }
      case CONVEX_SIMPLE_POLYGON: {
        return simpleConvexPolygon(planetModel, constraints);
      }
      case CONCAVE_SIMPLE_POLYGON: {
        return concaveSimplePolygon(planetModel, constraints);
      }
      case EXACT_CIRCLE: {
        return exactCircle(planetModel, constraints);
      }
      default:
        throw new IllegalStateException("Unexpected shape type");
    }
  }

  /**
   * Method that returns a random generated a GeoPointShape under given constraints. Returns
   * NULL if it cannot build the GeoCircle under the given constraints.
   *
   * @param planetModel The planet model.
   * @param constraints The given constraints.
   * @return The random generated GeoPointShape.
   */
  private GeoPointShape point(PlanetModel planetModel , Constraints constraints) {
    int iterations=0;
    while (iterations < MAX_SHAPE_ITERATIONS) {
      iterations++;
      final GeoPoint point = randomGeoPoint(planetModel, constraints);
      if (point == null){
        continue;
      }
      try {

        GeoPointShape pointShape = GeoPointShapeFactory.makeGeoPointShape(planetModel, point.getLatitude(), point.getLongitude());
        if (!constraints.valid(pointShape)) {
          continue;
        }
        return pointShape;
      } catch (IllegalArgumentException e) {
        continue;
      }
    }
    return null;
  }

  /**
   * Method that returns a random generated a GeoCircle under given constraints. Returns
   * NULL if it cannot build the GeoCircle under the given constraints.
   *
   * @param planetModel The planet model.
   * @param constraints The given constraints.
   * @return The random generated GeoCircle.
   */
  private GeoCircle circle(PlanetModel planetModel , Constraints constraints) {
    int iterations=0;
    while (iterations < MAX_SHAPE_ITERATIONS) {
      iterations++;
      final GeoPoint center = randomGeoPoint(planetModel, constraints);
      if (center == null){
        continue;
      }
      final double radius = randomCutoffAngle();
      try {

        GeoCircle circle = GeoCircleFactory.makeGeoCircle(planetModel, center.getLatitude(), center.getLongitude(), radius);
        if (!constraints.valid(circle)) {
          continue;
        }
        return circle;
      } catch (IllegalArgumentException e) {
        continue;
      }
    }
    return null;
  }

  /**
   * Method that returns a random generated a GeoCircle under given constraints. Returns
   * NULL if it cannot build the GeoCircle under the given constraints.
   *
   * @param planetModel The planet model.
   * @param constraints The given constraints.
   * @return The random generated GeoCircle.
   */
  private GeoCircle exactCircle(PlanetModel planetModel , Constraints constraints) {
    int iterations=0;
    while (iterations < MAX_SHAPE_ITERATIONS) {
      iterations++;
      final GeoPoint center = randomGeoPoint(planetModel, constraints);
      if (center == null){
        continue;
      }
      final double radius = randomCutoffAngle();
      final int pow = random().nextInt(10) +3;
      final double accuracy = random().nextDouble() * Math.pow(10, (-1) * pow);
      try {
        GeoCircle circle = GeoCircleFactory.makeExactGeoCircle(planetModel, center.getLatitude(), center.getLongitude(), radius, accuracy);
        if (!constraints.valid(circle)) {
          continue;
        }
        return circle;
      } catch (IllegalArgumentException e) {
        continue;
      }
    }
    return null;
  }

  /**
   * Method that returns a random generated a GeoBBox under given constraints. Returns
   * NULL if it cannot build the GeoBBox under the given constraints.
   *
   * @param planetModel The planet model.
   * @param constraints The given constraints.
   * @return The random generated GeoBBox.
   */
  private GeoBBox rectangle(PlanetModel planetModel, Constraints constraints) {

    int iterations = 0;
    while (iterations < MAX_SHAPE_ITERATIONS) {
      iterations++;
      final GeoPoint point1 = randomGeoPoint(planetModel, constraints);
      if (point1 == null){
        continue;
      }
      final GeoPoint point2 = randomGeoPoint(planetModel, constraints);
      if (point2 == null){
        continue;
      }

      double minLat = Math.min(point1.getLatitude(), point2.getLatitude());
      double maxLat = Math.max(point1.getLatitude(), point2.getLatitude());
      double minLon = Math.min(point1.getLongitude(), point2.getLongitude());
      double maxLon = Math.max(point1.getLongitude(), point2.getLongitude());

      try {
        GeoBBox bbox = GeoBBoxFactory.makeGeoBBox(planetModel, maxLat, minLat, minLon, maxLon);
        if (!constraints.valid(bbox)) {
          continue;
        }
        return bbox;
      } catch (IllegalArgumentException e) {
        continue;
      }
    }
    return null;
  }

  /**
   * Method that returns a random generated degenerate GeoPath under given constraints. Returns
   * NULL if it cannot build the degenerate GeoPath under the given constraints.
   *
   * @param planetModel The planet model.
   * @param constraints The given constraints.
   * @return The random generated degenerated GeoPath.
   */
  private GeoPath line(PlanetModel planetModel, Constraints constraints) {
    int iterations = 0;
    while (iterations < MAX_SHAPE_ITERATIONS) {
      iterations++;
      int vertexCount =  random().nextInt(2) + 2;
      List<GeoPoint> geoPoints = points(vertexCount, planetModel, constraints);
      if (geoPoints.size() < 2){
        continue;
      }
      try {
        GeoPath path = GeoPathFactory.makeGeoPath(planetModel, 0, geoPoints.toArray(new GeoPoint[geoPoints.size()]));
        if (!constraints.valid(path)) {
          continue;
        }
        return path;
      } catch (IllegalArgumentException e) {
        continue;
      }
    }
    return null;
  }

  /**
   * Method that returns a random generated a GeoPath under given constraints. Returns
   * NULL if it cannot build the GeoPath under the given constraints.
   *
   * @param planetModel The planet model.
   * @param constraints The given constraints.
   * @return The random generated GeoPath.
   */
  private GeoPath path(PlanetModel planetModel, Constraints constraints) {
    int iterations = 0;
    while (iterations < MAX_SHAPE_ITERATIONS) {
      iterations++;
      int vertexCount =  random().nextInt(2) + 2;
      List<GeoPoint> geoPoints = points(vertexCount, planetModel, constraints);
      if (geoPoints.size() < 2){
        continue;
      }
      double width =randomCutoffAngle();
      try {
        GeoPath path = GeoPathFactory.makeGeoPath(planetModel, width, geoPoints.toArray(new GeoPoint[geoPoints.size()]));
        if (!constraints.valid(path)) {
          continue;
        }
        return path;
      } catch (IllegalArgumentException e) {
        continue;
      }
    }
    return null;
  }

  /**
   * Method that returns a random generated a GeoCompositeMembershipShape under given constraints. Returns
   * NULL if it cannot build the GGeoCompositeMembershipShape under the given constraints.
   *
   * @param planetModel The planet model.
   * @param constraints The given constraints.
   * @return The random generated GeoCompositeMembershipShape.
   */
  private GeoCompositeAreaShape collection(PlanetModel planetModel, Constraints constraints) {
    int iterations = 0;
    while (iterations < MAX_SHAPE_ITERATIONS) {
      iterations++;
      int numberShapes =  random().nextInt(3) + 2;
      GeoCompositeAreaShape collection = new GeoCompositeAreaShape(planetModel);
      for(int i=0; i<numberShapes;i++){
        GeoPolygon member = convexPolygon(planetModel, constraints);
        if (member != null){
          collection.addShape(member);
        }
      }
      if (collection.shapes.size() ==0){
        continue;
      }
      return collection;
    }
    return null;
  }

  /**
   * Method that returns a random generated a convex GeoPolygon under given constraints. Returns
   * NULL if it cannot build the GePolygon under the given constraints.
   *
   * @param planetModel The planet model.
   * @param constraints The given constraints.
   * @return The random generated GeoPolygon.
   */
  private GeoPolygon convexPolygon(PlanetModel planetModel, Constraints constraints) {
    int vertexCount = random().nextInt(4) + 3;
    int iterations = 0;
    while (iterations < MAX_SHAPE_ITERATIONS) {
      iterations++;
      List<GeoPoint> geoPoints = points(vertexCount,planetModel, constraints);
      if (geoPoints.size() < 3){
        continue;
      }
      List<GeoPoint> orderedGeoPoints = orderPoints(geoPoints);
      try {
        GeoPolygon polygon = GeoPolygonFactory.makeGeoPolygon(planetModel, orderedGeoPoints);
        if (!constraints.valid(polygon) || isConcave(planetModel, polygon)) {
          continue;
        }
        return polygon;
      } catch (IllegalArgumentException e) {
        continue;
      }
    }
    return null;
  }

  /**
   * Method that returns a random generated a convex GeoPolygon with holes under given constraints. Returns
   * NULL if it cannot build the GeoPolygon with holes under the given constraints.
   *
   * @param planetModel The planet model.
   * @param constraints The given constraints.
   * @return The random generated GeoPolygon.
   */
  private GeoPolygon convexPolygonWithHoles(PlanetModel planetModel, Constraints constraints) {
    int vertexCount = random().nextInt(4) + 3;
    int iterations = 0;
    while (iterations < MAX_SHAPE_ITERATIONS) {
      iterations++;
      List<GeoPoint> geoPoints = points(vertexCount,planetModel, constraints);
      if (geoPoints.size() < 3){
        continue;
      }
      List<GeoPoint> orderedGeoPoints = orderPoints(geoPoints);
      try {
        GeoPolygon polygon = GeoPolygonFactory.makeGeoPolygon(planetModel, orderedGeoPoints);
        //polygon should comply with all constraints except disjoint as we have holes
        Constraints polygonConstraints = new Constraints();
        polygonConstraints.putAll(constraints.getContains());
        polygonConstraints.putAll(constraints.getWithin());
        polygonConstraints.putAll(constraints.getDisjoint());
        if (!polygonConstraints.valid(polygon) || isConcave(planetModel, polygon)){
          continue;
        }
        //hole must overlap with polygon and comply with any CONTAINS constraint.
        Constraints holeConstraints = new Constraints();
        holeConstraints.putAll(constraints.getContains());
        holeConstraints.put(polygon,GeoArea.OVERLAPS);
        //Points must be with in the polygon and must comply
        // CONTAINS and DISJOINT constraints
        Constraints pointsConstraints = new Constraints();
        pointsConstraints.put(polygon,GeoArea.WITHIN);
        pointsConstraints.putAll(constraints.getContains());
        pointsConstraints.putAll(constraints.getDisjoint());
        List<GeoPolygon> holes = concavePolygonHoles(planetModel, polygon, holeConstraints, pointsConstraints);
        //we should have at least one hole
        if (holes.size() == 0){
          continue;
        }
        polygon = GeoPolygonFactory.makeGeoPolygon(planetModel,orderedGeoPoints,holes);
        if (!constraints.valid(polygon) || isConcave(planetModel, polygon)){
          continue;
        }
        return polygon;
      } catch (IllegalArgumentException e) {
        continue;
      }
    }
    return null;
  }

  /**
   * Method that returns a random list if concave GeoPolygons under given constraints. Method
   * use to generate convex holes. Note that constraints for points and holes are different,
   *
   * @param planetModel The planet model.
   * @param polygon The polygon where the holes are within.
   * @param holeConstraints The given constraints that a hole must comply.
   * @param pointConstraints The given constraints that a point must comply.
   * @return The random generated GeoPolygon.
   */
  private List<GeoPolygon> concavePolygonHoles(PlanetModel planetModel,
                                               GeoPolygon polygon,
                                               Constraints holeConstraints,
                                               Constraints pointConstraints) {
    int iterations =0;
    int holesCount = random().nextInt(3) + 1;
    List<GeoPolygon> holes = new ArrayList<>();
    while (iterations < MAX_SHAPE_ITERATIONS) {
      iterations++;
      int vertexCount = random().nextInt(3) + 3;
      List<GeoPoint> geoPoints = points(vertexCount, planetModel, pointConstraints);
      if (geoPoints.size() < 3){
        continue;
      }
      geoPoints = orderPoints(geoPoints);
      GeoPolygon inversePolygon  = GeoPolygonFactory.makeGeoPolygon(planetModel, geoPoints);
      //The convex polygon must be within
      if (inversePolygon == null || polygon.getRelationship(inversePolygon) != GeoArea.WITHIN) {
        continue;
      }
      //make it concave
      Collections.reverse(geoPoints);
      try {
        GeoPolygon hole = GeoPolygonFactory.makeGeoPolygon(planetModel, geoPoints);
        if (!holeConstraints.valid(hole) || isConvex(planetModel, hole)) {
          continue;
        }
        holes.add(hole);
        if (holes.size() == holesCount){
          return holes;
        }
        pointConstraints.put(hole, GeoArea.DISJOINT);
      } catch (IllegalArgumentException e) {
        continue;
      }
    }
    return holes;
  }

  /**
   * Method that returns a random generated a concave GeoPolygon under given constraints. Returns
   * NULL if it cannot build the concave GeoPolygon under the given constraints.
   *
   * @param planetModel The planet model.
   * @param constraints The given constraints.
   * @return The random generated GeoPolygon.
   */
  private GeoPolygon concavePolygon(PlanetModel planetModel, Constraints constraints) {

    int vertexCount = random().nextInt(4) + 3;
    int iterations = 0;
    while (iterations < MAX_SHAPE_ITERATIONS) {
      iterations++;
      List<GeoPoint> geoPoints = points(vertexCount,planetModel, constraints);
      if (geoPoints.size() < 3){
        continue;
      }
      List<GeoPoint> orderedGeoPoints = orderPoints(geoPoints);
      Collections.reverse(orderedGeoPoints);
      try {
        GeoPolygon polygon = GeoPolygonFactory.makeGeoPolygon(planetModel, orderedGeoPoints);
        if (!constraints.valid(polygon) || isConvex(planetModel, polygon)) {
          continue;
        }
        return polygon;
      } catch (IllegalArgumentException e) {
        continue;
      }
    }
    return null;
  }

  /**
   * Method that returns a random generated a concave GeoPolygon with holes under given constraints. Returns
   * NULL if it cannot build the GeoPolygon under the given constraints. Note that the final GeoPolygon is
   * convex as the hole wraps the convex GeoPolygon.
   *
   * @param planetModel The planet model.
   * @param constraints The given constraints.
   * @return The random generated GeoPolygon.
   */
  private GeoPolygon concavePolygonWithHoles(PlanetModel planetModel, Constraints constraints) {
    int vertexCount = random().nextInt(4) + 3;
    int iterations = 0;
    while (iterations < MAX_SHAPE_ITERATIONS) {
      iterations++;
      //we first build the hole. We consider all constraints except
      // disjoint as we have a hole
      Constraints holeConstraints = new Constraints();
      holeConstraints.putAll(constraints.getContains());
      holeConstraints.putAll(constraints.getWithin());
      holeConstraints.putAll(constraints.getOverlaps());
      GeoPolygon hole = convexPolygon(planetModel, holeConstraints);
      if (hole == null){
        continue;
      }
      // Now we get points for polygon. Must we within the hole
      // and we add contain constraints
      Constraints pointConstraints = new Constraints();
      pointConstraints.put(hole, GeoArea.WITHIN);
      pointConstraints.putAll(constraints.getContains());
      List<GeoPoint> geoPoints = points(vertexCount,planetModel, pointConstraints);
      if (geoPoints.size() < 3){
        continue;
      }
      try {
        List<GeoPoint> orderedGeoPoints = orderPoints(geoPoints);
        GeoPolygon inversePolygon  = GeoPolygonFactory.makeGeoPolygon(planetModel, geoPoints);
        //The convex polygon must be within the hole
        if (inversePolygon == null || hole.getRelationship(inversePolygon) != GeoArea.WITHIN) {
          continue;
        }
        Collections.reverse(orderedGeoPoints);
        GeoPolygon polygon = GeoPolygonFactory.makeGeoPolygon(planetModel, orderedGeoPoints, Collections.singletonList(hole));
        //final polygon must be convex
        if (!constraints.valid(polygon) || isConcave(planetModel,polygon)) {
          continue;
        }
        return polygon;
      } catch (IllegalArgumentException e) {
        continue;
      }
    }
    return null;
  }

  /**
   * Method that returns a random generated complex GeoPolygon under given constraints. Returns
   * NULL if it cannot build the complex GeoPolygon under the given constraints.
   *
   * @param planetModel The planet model.
   * @param constraints The given constraints.
   * @return The random generated GeoPolygon.
   */
  private GeoPolygon complexPolygon(PlanetModel planetModel, Constraints constraints) {
    int polygonsCount =random().nextInt(2) + 1;
    int iterations = 0;
    while (iterations < MAX_SHAPE_ITERATIONS) {
      iterations++;
      List<GeoPolygonFactory.PolygonDescription> polDescription = new ArrayList<>();
      while(polDescription.size() < polygonsCount){
        int vertexCount = random().nextInt(14) + 3;
        List<GeoPoint> geoPoints = points(vertexCount,planetModel, constraints);
        if (geoPoints.size() < 3){
          break;
        }
        orderPoints(geoPoints);
        polDescription.add(new GeoPolygonFactory.PolygonDescription(geoPoints));
      }
      try {
        GeoPolygon polygon = GeoPolygonFactory.makeLargeGeoPolygon(planetModel,polDescription);
        if (!constraints.valid(polygon)) {
          continue;
        }
        return polygon;
      } catch (IllegalArgumentException e) {
        continue;
      }
    }
    return null;
  }

  /**
   * Method that returns a random generated a concave square GeoPolygon under given constraints. Returns
   * NULL if it cannot build the concave GeoPolygon under the given constraints. This shape is an utility
   * to build constraints.
   *
   * @param planetModel The planet model.
   * @param constraints The given constraints.
   * @return The random generated GeoPolygon.
   */
  private GeoPolygon simpleConvexPolygon(PlanetModel planetModel, Constraints constraints) {
    int iterations = 0;
    while (iterations < MAX_SHAPE_ITERATIONS) {
      iterations++;
      List<GeoPoint> points = points(3,planetModel,constraints);
      if (points.size() < 3){
        continue;
      }
      points = orderPoints(points);
      try {
        GeoPolygon polygon =  GeoPolygonFactory.makeGeoConvexPolygon(planetModel, points);
        if(!constraints.valid(polygon) || isConcave(planetModel,polygon)){
          continue;
        }
        return polygon;
      } catch (IllegalArgumentException e) {
        continue;
      }
    }
    return null;
  }

  /**
   * Method that returns a random generated a convex square GeoPolygon under given constraints. Returns
   * NULL if it cannot build the convex GeoPolygon under the given constraints. This shape is an utility
   * to build constraints.
   *
   * @param planetModel The planet model.
   * @param constraints The given constraints.
   * @return The random generated GeoPolygon.
   */
  private GeoPolygon concaveSimplePolygon(PlanetModel planetModel, Constraints constraints) {
    int iterations = 0;
    while (iterations < MAX_SHAPE_ITERATIONS) {
      iterations++;
      List<GeoPoint> points = points(3, planetModel, constraints);
      if (points.size() < 3){
        continue;
      }
      points = orderPoints(points);
      Collections.reverse(points);
      try {
        GeoPolygon polygon =  GeoPolygonFactory.makeGeoConcavePolygon(planetModel, points);
        if(!constraints.valid(polygon) || isConvex(planetModel, polygon)){
          continue;
        }
        return polygon;
      } catch (IllegalArgumentException e) {
        continue;
      }
    }
    return null;
  }

  /**
   * Method that returns a random list of generated GeoPoints under given constraints. The
   * number of points returned might be lower than the requested.
   *
   * @param count The number of points
   * @param planetModel The planet model.
   * @param constraints The given constraints.
   * @return The random generated List of GeoPoints.
   */
  private List<GeoPoint> points(int count, PlanetModel planetModel, Constraints constraints){
    List<GeoPoint> geoPoints = new ArrayList<>(count);
    for(int i= 0; i< count; i++) {
      GeoPoint point = randomGeoPoint(planetModel, constraints);
      if (point != null){
        geoPoints.add(point);
      }
    }
    return  geoPoints;
  }

  /**
   * Check if a GeoPolygon is pure concave. Note that our definition for concavity is that the polygon
   * contains antipodal points.
   *
   * @param planetModel The planet model.
   * @param shape The polygon to check.
   * @return True if the polygon contains antipodal points.
   */
  private boolean isConcave(PlanetModel planetModel, GeoPolygon shape){
    return (shape.isWithin(planetModel.NORTH_POLE) && shape.isWithin(planetModel.SOUTH_POLE))||
        (shape.isWithin(planetModel.MAX_X_POLE) && shape.isWithin(planetModel.MIN_X_POLE)) ||
        (shape.isWithin(planetModel.MAX_Y_POLE) && shape.isWithin(planetModel.MIN_Y_POLE));
  }

  /**
   * Check if a GeoPolygon is pure convex. Note that our definition for convexity is that the polygon
   * does not contain antipodal points.
   *
   * @param planetModel The planet model.
   * @param shape The polygon to check.
   * @return True if the polygon dies not contains antipodal points.
   */
  private boolean isConvex(PlanetModel planetModel, GeoPolygon shape){
    return !isConcave(planetModel,shape);
  }

  /**
   * Generates a random number between 0 and PI.
   *
   * @return the cutoff angle.
   */
  private double randomCutoffAngle() {
    return randomDouble() * Math.PI;
  }

  /**
   * Method that orders a lit of points anti-clock-wise to prevent crossing edges.
   *
   * @param points The points to order.
   * @return The list of ordered points anti-clockwise.
   */
  protected List<GeoPoint> orderPoints(List<GeoPoint> points) {
    double x = 0;
    double y = 0;
    double z = 0;
    //get center of mass
    for (GeoPoint point : points) {
      x += point.x;
      y += point.y;
      z += point.z;
    }
    Map<Double, GeoPoint> pointWithAngle = new HashMap<>();
    //get angle respect center of mass
    for (GeoPoint point : points) {
      GeoPoint center = new GeoPoint(x / points.size(), y / points.size(), z / points.size());
      double cs = Math.sin(center.getLatitude()) * Math.sin(point.getLatitude())
          + Math.cos(center.getLatitude()) * Math.cos(point.getLatitude())  * Math.cos(point.getLongitude() - center.getLongitude());
      double posAng = Math.atan2(Math.cos(center.getLatitude()) * Math.cos(point.getLatitude()) * Math.sin(point.getLongitude() - center.getLongitude()),
          Math.sin(point.getLatitude()) - Math.sin(center.getLatitude())*cs);
      pointWithAngle.put(posAng, point);
    }
    //order points
    List<Double> angles = new ArrayList<>(pointWithAngle.keySet());
    Collections.sort(angles);
    Collections.reverse(angles);
    List<GeoPoint> orderedPoints = new ArrayList<>();
    for (Double d : angles) {
      orderedPoints.add(pointWithAngle.get(d));
    }
    return orderedPoints;
  }

  /**
   * Class that holds the constraints that are given to
   * build shapes. It consists in a list of GeoAreaShapes
   * and relationships the new shape needs to satisfy.
   */
  class Constraints extends HashMap<GeoAreaShape, Integer>{

    /**
     * Check if the shape is valid under the constraints.
     *
     * @param shape The shape to check
     * @return true if the shape satisfy the constraints, else false.
     */
    public boolean valid(GeoShape shape) {
      if (shape == null){
        return false;
      }
      for (GeoAreaShape constraint : keySet()) {
        if (constraint.getRelationship(shape) != get(constraint)) {
          return false;
        }
      }
      return true;
    }

    /**
     * Check if a point is Within the constraints.
     *
     * @param point The point to check
     * @return true if the point satisfy the constraints, else false.
     */
    public boolean isWithin(GeoPoint point) {
      for (GeoShape constraint : keySet()) {
        if (!(validPoint(point, constraint, get(constraint)))) {
          return false;
        }
      }
      return true;
    }

    /**
     * Check if a point is Within one constraint given by a shape and a relationship.
     *
     * @param point The point to check
     * @param shape The shape of the constraint
     * @param relationship The relationship of the constraint.
     * @return true if the point satisfy the constraint, else false.
     */
    private boolean validPoint(GeoPoint point, GeoShape shape, int relationship) {
      //For GeoCompositeMembershipShape we only consider the first shape to help
      // converging
      if (relationship == GeoArea.WITHIN && shape instanceof GeoCompositeMembershipShape) {
        shape = (((GeoCompositeMembershipShape) shape).shapes.get(0));
      }
      switch (relationship) {
        case GeoArea.DISJOINT:
          return !shape.isWithin(point);
        case GeoArea.OVERLAPS:
          return true;
        case GeoArea.CONTAINS:
          return !shape.isWithin(point);
        case GeoArea.WITHIN:
          return shape.isWithin(point);
        default:
          return true;
      }
    }

    /**
     * Collect the CONTAINS constraints in the object
     *
     * @return the CONTAINS constraints.
     */
    public Constraints getContains(){
      return getConstraintsOfType(GeoArea.CONTAINS);
    }

    /**
     * Collect the WITHIN constraints in the object
     *
     * @return the WITHIN constraints.
     */
    public Constraints getWithin(){
      return getConstraintsOfType(GeoArea.WITHIN);
    }

    /**
     * Collect the OVERLAPS constraints in the object
     *
     * @return the OVERLAPS constraints.
     */
    public Constraints getOverlaps(){
      return getConstraintsOfType(GeoArea.OVERLAPS);
    }

    /**
     * Collect the DISJOINT constraints in the object
     *
     * @return the DISJOINT constraints.
     */
    public Constraints getDisjoint(){
      return getConstraintsOfType(GeoArea.DISJOINT);
    }

    private Constraints getConstraintsOfType(int type){
      Constraints constraints = new Constraints();
      for (GeoAreaShape constraint : keySet()) {
        if (type == get(constraint)) {
          constraints.put(constraint, type);
        }
      }
      return constraints;
    }
  }
}
