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

import org.apache.lucene.util.LuceneTestCase;
import org.junit.Test;

public class XYZSolidTest extends LuceneTestCase {

  @Test
  public void testNonDegenerateRelationships() {
    XYZSolid s;
    GeoShape shape;
    // Something bigger than the world
    s = new StandardXYZSolid(PlanetModel.SPHERE, -2.0, 2.0, -2.0, 2.0, -2.0, 2.0);
    // Any shape, except whole world, should be within.
    shape = new GeoStandardCircle(PlanetModel.SPHERE, 0.0, 0.0, 0.1);
    assertEquals(GeoArea.WITHIN, s.getRelationship(shape));
    shape = new GeoWorld(PlanetModel.SPHERE);
    // An XYZSolid represents a surface shape, which when larger than the world is in fact
    // the entire world, so it should overlap the world.
    assertEquals(GeoArea.OVERLAPS, s.getRelationship(shape));

    // Something overlapping the world on only one side
    s = new StandardXYZSolid(PlanetModel.SPHERE, -2.0, 0.0, -2.0, 2.0, -2.0, 2.0);
    // Some things should be disjoint...
    shape = new GeoStandardCircle(PlanetModel.SPHERE, 0.0, 0.0, 0.1);
    assertEquals(GeoArea.DISJOINT, s.getRelationship(shape));
    // And, some things should be within... 
    shape = new GeoStandardCircle(PlanetModel.SPHERE, 0.0, Math.PI, 0.1);
    assertEquals(GeoArea.WITHIN, s.getRelationship(shape));
    // And, some things should overlap.
    shape = new GeoStandardCircle(PlanetModel.SPHERE, 0.0, Math.PI * 0.5, 0.1);
    assertEquals(GeoArea.OVERLAPS, s.getRelationship(shape));

    // Partial world should be contained by GeoWorld object...
    shape = new GeoWorld(PlanetModel.SPHERE);
    assertEquals(GeoArea.CONTAINS, s.getRelationship(shape));
    
    // Something inside the world
    s = new StandardXYZSolid(PlanetModel.SPHERE, -0.1, 0.1, -0.1, 0.1, -0.1, 0.1);
    // All shapes should be disjoint
    shape = new GeoStandardCircle(PlanetModel.SPHERE, 0.0, 0.0, 0.1);
    assertEquals(GeoArea.DISJOINT, s.getRelationship(shape));
    shape = new GeoWorld(PlanetModel.SPHERE);
    assertEquals(GeoArea.DISJOINT, s.getRelationship(shape));
    
  }

  @Test
  public void testDegenerateRelationships() {
    GeoArea solid;
    GeoShape shape;
    
    // Basic test of the factory method - non-degenerate
    solid = GeoAreaFactory.makeGeoArea(PlanetModel.SPHERE, -2.0, 2.0, -2.0, 2.0, -2.0, 2.0);
    // Any shape, except whole world, should be within.
    shape = new GeoStandardCircle(PlanetModel.SPHERE, 0.0, 0.0, 0.1);
    assertEquals(GeoArea.WITHIN, solid.getRelationship(shape));
    shape = new GeoWorld(PlanetModel.SPHERE);
    // An XYZSolid represents a surface shape, which when larger than the world is in fact
    // the entire world, so it should overlap the world.
    assertEquals(GeoArea.OVERLAPS, solid.getRelationship(shape));

    // Build a degenerate point, not on sphere
    solid = GeoAreaFactory.makeGeoArea(PlanetModel.SPHERE, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0);
    // disjoint with everything?
    shape = new GeoStandardCircle(PlanetModel.SPHERE, 0.0, 0.0, 0.1);
    assertEquals(GeoArea.DISJOINT, solid.getRelationship(shape));
    shape = new GeoWorld(PlanetModel.SPHERE);
    assertEquals(GeoArea.DISJOINT, solid.getRelationship(shape));

    // Build a degenerate point that IS on the sphere
    solid = GeoAreaFactory.makeGeoArea(PlanetModel.SPHERE, 1.0, 1.0, 0.0, 0.0, 0.0, 0.0);
    // inside everything that it touches?
    shape = new GeoStandardCircle(PlanetModel.SPHERE, 0.0, 0.0, 0.1);
    assertEquals(GeoArea.CONTAINS, solid.getRelationship(shape));
    shape = new GeoWorld(PlanetModel.SPHERE);
    assertEquals(GeoArea.CONTAINS, solid.getRelationship(shape));
    shape = new GeoStandardCircle(PlanetModel.SPHERE, 0.0, Math.PI, 0.1);
    assertEquals(GeoArea.DISJOINT, solid.getRelationship(shape));

    // Build a shape degenerate in (x,y), which has no points on sphere
    solid = GeoAreaFactory.makeGeoArea(PlanetModel.SPHERE, 0.0, 0.0, 0.0, 0.0, -0.1, 0.1);
    // disjoint with everything?
    shape = new GeoStandardCircle(PlanetModel.SPHERE, 0.0, 0.0, 0.1);
    assertEquals(GeoArea.DISJOINT, solid.getRelationship(shape));
    shape = new GeoWorld(PlanetModel.SPHERE);
    assertEquals(GeoArea.DISJOINT, solid.getRelationship(shape));

    // Build a shape degenerate in (x,y) which has one point on sphere
    solid = GeoAreaFactory.makeGeoArea(PlanetModel.SPHERE, 0.0, 0.0, 0.0, 0.0, -0.1, 1.1);
    // inside everything that it touches?
    shape = new GeoStandardCircle(PlanetModel.SPHERE, 0.0, 0.0, 0.1);
    assertEquals(GeoArea.DISJOINT, solid.getRelationship(shape));
    shape = new GeoWorld(PlanetModel.SPHERE);
    assertEquals(GeoArea.CONTAINS, solid.getRelationship(shape));
    shape = new GeoStandardCircle(PlanetModel.SPHERE, Math.PI * 0.5, 0.0, 0.1);
    assertEquals(GeoArea.CONTAINS, solid.getRelationship(shape));
    shape = new GeoStandardCircle(PlanetModel.SPHERE, -Math.PI * 0.5, 0.0, 0.1);
    assertEquals(GeoArea.DISJOINT, solid.getRelationship(shape));

    // Build a shape degenerate in (x,y) which has two points on sphere
    solid = GeoAreaFactory.makeGeoArea(PlanetModel.SPHERE, 0.0, 0.0, 0.0, 0.0, -1.1, 1.1);
    // inside everything that it touches?
    shape = new GeoStandardCircle(PlanetModel.SPHERE, 0.0, 0.0, 0.1);
    assertEquals(GeoArea.DISJOINT, solid.getRelationship(shape));
    shape = new GeoWorld(PlanetModel.SPHERE);
    assertEquals(GeoArea.CONTAINS, solid.getRelationship(shape));
    shape = new GeoStandardCircle(PlanetModel.SPHERE, Math.PI * 0.5, 0.0, 0.1);
    assertEquals(GeoArea.OVERLAPS, solid.getRelationship(shape));
    shape = new GeoStandardCircle(PlanetModel.SPHERE, -Math.PI * 0.5, 0.0, 0.1);
    assertEquals(GeoArea.OVERLAPS, solid.getRelationship(shape));
    
    // Build a shape degenerate in (x,z), which has no points on sphere
    solid = GeoAreaFactory.makeGeoArea(PlanetModel.SPHERE, 0.0, 0.0, -0.1, 0.1, 0.0, 0.0);
    // disjoint with everything?
    shape = new GeoStandardCircle(PlanetModel.SPHERE, 0.0, 0.0, 0.1);
    assertEquals(GeoArea.DISJOINT, solid.getRelationship(shape));
    shape = new GeoWorld(PlanetModel.SPHERE);
    assertEquals(GeoArea.DISJOINT, solid.getRelationship(shape));

    // Build a shape degenerate in (x,z) which has one point on sphere
    solid = GeoAreaFactory.makeGeoArea(PlanetModel.SPHERE, 0.0, 0.0, -0.1, 1.1, 0.0, 0.0);
    // inside everything that it touches?
    shape = new GeoStandardCircle(PlanetModel.SPHERE, 0.0, 0.0, 0.1);
    assertEquals(GeoArea.DISJOINT, solid.getRelationship(shape));
    shape = new GeoWorld(PlanetModel.SPHERE);
    assertEquals(GeoArea.CONTAINS, solid.getRelationship(shape));
    shape = new GeoStandardCircle(PlanetModel.SPHERE, 0.0, Math.PI * 0.5, 0.1);
    assertEquals(GeoArea.CONTAINS, solid.getRelationship(shape));
    shape = new GeoStandardCircle(PlanetModel.SPHERE, 0.0, -Math.PI * 0.5, 0.1);
    assertEquals(GeoArea.DISJOINT, solid.getRelationship(shape));

    // Build a shape degenerate in (x,y) which has two points on sphere
    solid = GeoAreaFactory.makeGeoArea(PlanetModel.SPHERE, 0.0, 0.0, -1.1, 1.1, 0.0, 0.0);
    // inside everything that it touches?
    shape = new GeoStandardCircle(PlanetModel.SPHERE, 0.0, 0.0, 0.1);
    assertEquals(GeoArea.DISJOINT, solid.getRelationship(shape));
    shape = new GeoWorld(PlanetModel.SPHERE);
    assertEquals(GeoArea.CONTAINS, solid.getRelationship(shape));
    shape = new GeoStandardCircle(PlanetModel.SPHERE, 0.0, Math.PI * 0.5, 0.1);
    assertEquals(GeoArea.OVERLAPS, solid.getRelationship(shape));
    shape = new GeoStandardCircle(PlanetModel.SPHERE, 0.0, -Math.PI * 0.5, 0.1);
    assertEquals(GeoArea.OVERLAPS, solid.getRelationship(shape));

    // MHL for y-z check
    
    // Build a shape that is degenerate in x, which has zero points intersecting sphere
    solid = GeoAreaFactory.makeGeoArea(PlanetModel.SPHERE, 0.0, 0.0, -0.1, 0.1, -0.1, 0.1);
    // disjoint with everything?
    shape = new GeoStandardCircle(PlanetModel.SPHERE, 0.0, 0.0, 0.1);
    assertEquals(GeoArea.DISJOINT, solid.getRelationship(shape));
    shape = new GeoWorld(PlanetModel.SPHERE);
    assertEquals(GeoArea.DISJOINT, solid.getRelationship(shape));

    // Build a shape that is degenerate in x, which has zero points intersecting sphere, second variation
    solid = GeoAreaFactory.makeGeoArea(PlanetModel.SPHERE, 0.0, 0.0, -0.1, 0.1, 1.1, 1.2);
    // disjoint with everything?
    shape = new GeoStandardCircle(PlanetModel.SPHERE, 0.0, 0.0, 0.1);
    assertEquals(GeoArea.DISJOINT, solid.getRelationship(shape));
    shape = new GeoWorld(PlanetModel.SPHERE);
    assertEquals(GeoArea.DISJOINT, solid.getRelationship(shape));

    // Build a shape that is disjoint in X but intersects sphere in a complete circle
    solid = GeoAreaFactory.makeGeoArea(PlanetModel.SPHERE, 0.0, 0.0, -1.1, 1.1, -1.1, 1.1);
    // inside everything that it touches?
    shape = new GeoWorld(PlanetModel.SPHERE);
    assertEquals(GeoArea.CONTAINS, solid.getRelationship(shape));
    shape = new GeoStandardCircle(PlanetModel.SPHERE, 0.0, 0.0, 0.1);
    assertEquals(GeoArea.DISJOINT, solid.getRelationship(shape));
    shape = new GeoStandardCircle(PlanetModel.SPHERE, 0.0, Math.PI, 0.1);
    assertEquals(GeoArea.DISJOINT, solid.getRelationship(shape));
    shape = new GeoStandardCircle(PlanetModel.SPHERE, 0.0, Math.PI * 0.5, 0.1);
    assertEquals(GeoArea.OVERLAPS, solid.getRelationship(shape));
    shape = new GeoStandardCircle(PlanetModel.SPHERE, 0.0, -Math.PI * 0.5, 0.1);
    assertEquals(GeoArea.OVERLAPS, solid.getRelationship(shape));
    shape = new GeoStandardCircle(PlanetModel.SPHERE, Math.PI * 0.5, 0.0, 0.1);
    assertEquals(GeoArea.OVERLAPS, solid.getRelationship(shape));
    shape = new GeoStandardCircle(PlanetModel.SPHERE, -Math.PI * 0.5, 0.0, 0.1);
    assertEquals(GeoArea.OVERLAPS, solid.getRelationship(shape));

    // Build a shape that is disjoint in X but intersects sphere in a half circle in Y
    solid = GeoAreaFactory.makeGeoArea(PlanetModel.SPHERE, 0.0, 0.0, 0.0, 1.1, -1.1, 1.1);
    // inside everything that it touches?
    shape = new GeoWorld(PlanetModel.SPHERE);
    assertEquals(GeoArea.CONTAINS, solid.getRelationship(shape));
    shape = new GeoStandardCircle(PlanetModel.SPHERE, 0.0, 0.0, 0.1);
    assertEquals(GeoArea.DISJOINT, solid.getRelationship(shape));
    shape = new GeoStandardCircle(PlanetModel.SPHERE, 0.0, Math.PI, 0.1);
    assertEquals(GeoArea.DISJOINT, solid.getRelationship(shape));
    shape = new GeoStandardCircle(PlanetModel.SPHERE, 0.0, Math.PI * 0.5, 0.1);
    assertEquals(GeoArea.OVERLAPS, solid.getRelationship(shape));
    shape = new GeoStandardCircle(PlanetModel.SPHERE, 0.0, -Math.PI * 0.5, 0.1);
    assertEquals(GeoArea.DISJOINT, solid.getRelationship(shape));
    shape = new GeoStandardCircle(PlanetModel.SPHERE, Math.PI * 0.5, 0.0, 0.1);
    assertEquals(GeoArea.OVERLAPS, solid.getRelationship(shape));
    shape = new GeoStandardCircle(PlanetModel.SPHERE, -Math.PI * 0.5, 0.0, 0.1);
    assertEquals(GeoArea.OVERLAPS, solid.getRelationship(shape));

    // MHL for degenerate Y
    // MHL for degenerate Z
    
  }
  
}
