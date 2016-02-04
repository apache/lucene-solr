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

import org.apache.lucene.geo3d.GeoArea;
import org.apache.lucene.geo3d.GeoBBox;
import org.apache.lucene.geo3d.GeoBBoxFactory;
import org.apache.lucene.geo3d.GeoCircle;
import org.apache.lucene.geo3d.GeoStandardCircle;
import org.apache.lucene.geo3d.GeoPath;
import org.apache.lucene.geo3d.GeoPoint;
import org.apache.lucene.geo3d.PlanetModel;
import org.junit.Test;

public class Geo3dShapeWGS84ModelRectRelationTest extends Geo3dShapeRectRelationTestCase {

  public Geo3dShapeWGS84ModelRectRelationTest() {
    super(PlanetModel.WGS84);
  }

  @Test
  public void testFailure1() {
    final GeoBBox rect = GeoBBoxFactory.makeGeoBBox(planetModel, 90 * RADIANS_PER_DEGREE, 74 * RADIANS_PER_DEGREE,
        40 * RADIANS_PER_DEGREE, 60 * RADIANS_PER_DEGREE);
    final GeoPath path = new GeoPath(planetModel, 4 * RADIANS_PER_DEGREE);
    path.addPoint(84.4987594274 * RADIANS_PER_DEGREE, -22.8345484402 * RADIANS_PER_DEGREE);
    path.done();
    assertTrue(GeoArea.DISJOINT == rect.getRelationship(path));
    // This is what the test failure claimed...
    //assertTrue(GeoArea.CONTAINS == rect.getRelationship(path));
    //final GeoBBox bbox = getBoundingBox(path);
    //assertFalse(GeoArea.DISJOINT == rect.getRelationship(bbox));
  }

  @Test
  public void testFailure2() {
    final GeoBBox rect = GeoBBoxFactory.makeGeoBBox(planetModel, -74 * RADIANS_PER_DEGREE, -90 * RADIANS_PER_DEGREE,
        0 * RADIANS_PER_DEGREE, 26 * RADIANS_PER_DEGREE);
    final GeoCircle circle = new GeoStandardCircle(planetModel, -87.3647352103 * RADIANS_PER_DEGREE, 52.3769709972 * RADIANS_PER_DEGREE, 1 * RADIANS_PER_DEGREE);
    assertTrue(GeoArea.DISJOINT == rect.getRelationship(circle));
    // This is what the test failure claimed...
    //assertTrue(GeoArea.CONTAINS == rect.getRelationship(circle));
    //final GeoBBox bbox = getBoundingBox(circle);
    //assertFalse(GeoArea.DISJOINT == rect.getRelationship(bbox));
  }

  @Test
  public void testFailure3() {
    /*
   [junit4]   1> S-R Rel: {}, Shape {}, Rectangle {}    lap# {} [CONTAINS, Geo3dShape{planetmodel=PlanetModel: {ab=1.0011188180710464, c=0.9977622539852008}, shape=GeoPath: {planetmodel=PlanetModel: {ab=1.0011188180710464, c=0.9977622539852008}, width=1.53588974175501(87.99999999999999), 
    points={[[X=0.12097657665150223, Y=-0.6754177666095532, Z=0.7265376136709238], [X=-0.3837892785614207, Y=0.4258049113530899, Z=0.8180007850434892]]}}}, 
    Rect(minX=4.0,maxX=36.0,minY=16.0,maxY=16.0), 6981](no slf4j subst; sorry)
   [junit4] FAILURE 0.59s | Geo3dWGS84ShapeRectRelationTest.testGeoPathRect <<<
   [junit4]    > Throwable #1: java.lang.AssertionError: Geo3dShape{planetmodel=PlanetModel: {ab=1.0011188180710464, c=0.9977622539852008}, shape=GeoPath: {planetmodel=PlanetModel: {ab=1.0011188180710464, c=0.9977622539852008}, width=1.53588974175501(87.99999999999999), 
    points={[[X=0.12097657665150223, Y=-0.6754177666095532, Z=0.7265376136709238], [X=-0.3837892785614207, Y=0.4258049113530899, Z=0.8180007850434892]]}}} intersect Pt(x=23.81626064835212,y=16.0)
   [junit4]    >  at __randomizedtesting.SeedInfo.seed([2595268DA3F13FEA:6CC30D8C83453E5D]:0)
   [junit4]    >  at org.apache.lucene.spatial.spatial4j.RandomizedShapeTestCase._assertIntersect(RandomizedShapeTestCase.java:168)
   [junit4]    >  at org.apache.lucene.spatial.spatial4j.RandomizedShapeTestCase.assertRelation(RandomizedShapeTestCase.java:153)
   [junit4]    >  at org.apache.lucene.spatial.spatial4j.RectIntersectionTestHelper.testRelateWithRectangle(RectIntersectionTestHelper.java:128)
   [junit4]    >  at org.apache.lucene.spatial.spatial4j.Geo3dWGS84ShapeRectRelationTest.testGeoPathRect(Geo3dWGS84ShapeRectRelationTest.java:265)
  */
    final GeoBBox rect = GeoBBoxFactory.makeGeoBBox(planetModel, 16 * RADIANS_PER_DEGREE, 16 * RADIANS_PER_DEGREE, 4 * RADIANS_PER_DEGREE, 36 * RADIANS_PER_DEGREE);
    final GeoPoint pt = new GeoPoint(planetModel, 16 * RADIANS_PER_DEGREE, 23.81626064835212 * RADIANS_PER_DEGREE);
    final GeoPath path = new GeoPath(planetModel, 88 * RADIANS_PER_DEGREE);
    path.addPoint(46.6369060853 * RADIANS_PER_DEGREE, -79.8452213228 * RADIANS_PER_DEGREE);
    path.addPoint(54.9779334519 * RADIANS_PER_DEGREE, 132.029177424 * RADIANS_PER_DEGREE);
    path.done();
    System.out.println("rect=" + rect);
    // Rectangle is within path (this is wrong; it's on the other side.  Should be OVERLAPS)
    assertTrue(GeoArea.OVERLAPS == rect.getRelationship(path));
    // Rectangle contains point
    //assertTrue(rect.isWithin(pt));
    // Path contains point (THIS FAILS)
    //assertTrue(path.isWithin(pt));
    // What happens: (1) The center point of the horizontal line is within the path, in fact within a radius of one of the endpoints.
    // (2) The point mentioned is NOT inside either SegmentEndpoint.
    // (3) The point mentioned is NOT inside the path segment, either.  (I think it should be...)
  }

}

