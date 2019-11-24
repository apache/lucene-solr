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
package org.apache.lucene.spatial.spatial4j;

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.spatial3d.geom.GeoArea;
import org.apache.lucene.spatial3d.geom.GeoBBox;
import org.apache.lucene.spatial3d.geom.GeoBBoxFactory;
import org.apache.lucene.spatial3d.geom.GeoCircle;
import org.apache.lucene.spatial3d.geom.GeoCircleFactory;
import org.apache.lucene.spatial3d.geom.GeoPoint;
import org.apache.lucene.spatial3d.geom.GeoPolygonFactory;
import org.apache.lucene.spatial3d.geom.GeoShape;
import org.apache.lucene.spatial3d.geom.PlanetModel;
import org.junit.Test;
import org.locationtech.spatial4j.shape.Circle;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.Rectangle;
import org.locationtech.spatial4j.shape.SpatialRelation;

public class Geo3dShapeSphereModelRectRelationTest extends ShapeRectRelationTestCase {

  PlanetModel planetModel = PlanetModel.SPHERE;

  public Geo3dShapeSphereModelRectRelationTest() {
    Geo3dSpatialContextFactory factory = new Geo3dSpatialContextFactory();
    factory.planetModel = planetModel;
    this.ctx = factory.newSpatialContext();
  }

  @Test
  public void testFailure1() {
    final GeoBBox rect = GeoBBoxFactory.makeGeoBBox(planetModel, 88 * RADIANS_PER_DEGREE, 30 * RADIANS_PER_DEGREE, -30 * RADIANS_PER_DEGREE, 62 * RADIANS_PER_DEGREE);
    final List<GeoPoint> points = new ArrayList<>();
    points.add(new GeoPoint(planetModel, 30.4579218227 * RADIANS_PER_DEGREE, 14.5238410082 * RADIANS_PER_DEGREE));
    points.add(new GeoPoint(planetModel, 43.684447915 * RADIANS_PER_DEGREE, 46.2210986329 * RADIANS_PER_DEGREE));
    points.add(new GeoPoint(planetModel, 66.2465299717 * RADIANS_PER_DEGREE, -29.1786158537 * RADIANS_PER_DEGREE));
    final GeoShape path = GeoPolygonFactory.makeGeoPolygon(planetModel, points);

    final GeoPoint point = new GeoPoint(planetModel, 34.2730264413182 * RADIANS_PER_DEGREE, 82.75500168892472 * RADIANS_PER_DEGREE);

    // Apparently the rectangle thinks the polygon is completely within it... "shape inside rectangle"
    assertTrue(GeoArea.WITHIN == rect.getRelationship(path));

    // Point is within path? Apparently not...
    assertFalse(path.isWithin(point));

    // If it is within the path, it must be within the rectangle, and similarly visa versa
    assertFalse(rect.isWithin(point));

  }

  @SuppressWarnings({"unchecked", "rawtypes", "deprecation"})
  @Test
  public void testFailure2_LUCENE6475() {
    GeoCircle geo3dCircle = GeoCircleFactory.makeGeoCircle(planetModel, 1.6282053147165243E-4 * RADIANS_PER_DEGREE,
        -70.1600629789353 * RADIANS_PER_DEGREE, 86 * RADIANS_PER_DEGREE);
    Geo3dShape geo3dShape = new Geo3dShape(geo3dCircle, ctx);
    Rectangle rect = ctx.makeRectangle(-118, -114, -2.0, 32.0);
    assertTrue(geo3dShape.relate(rect).intersects());
    // thus the bounding box must intersect too
    assertTrue(geo3dShape.getBoundingBox().relate(rect).intersects());

  }

  @Test
  public void pointBearingTest(){
    double radius = 136;
    double distance = 135.97;
    double bearing = 188;
    Point p = ctx.getShapeFactory().pointXY(35, 85);
    Circle circle = ctx.getShapeFactory().circle(p, radius);
    Point bPoint = ctx.getDistCalc().pointOnBearing(p, distance, bearing, ctx, (Point) null);

    double d = ctx.getDistCalc().distance(p, bPoint);
    assertEquals(d, distance, 10-8);

    assertEquals(circle.relate(bPoint), SpatialRelation.CONTAINS);
  }
}
