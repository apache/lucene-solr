package org.apache.lucene.geo3d;

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

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class XYZSolidTest {


  @Test
  public void testRelationships() {
    XYZSolid s;
    GeoShape shape;
    // Something bigger than the world
    s = new XYZSolid(PlanetModel.SPHERE, -2.0, 2.0, -2.0, 2.0, -2.0, 2.0);
    // Any shape, including world, should be within.
    shape = new GeoCircle(PlanetModel.SPHERE, 0.0, 0.0, 0.1);
    assertEquals(GeoArea.WITHIN, s.getRelationship(shape));
    shape = new GeoWorld(PlanetModel.SPHERE);
    
    // An XYZSolid represents a surface shape, which when larger than the world is in fact
    // the entire world, so it should overlap the world.
    assertEquals(GeoArea.OVERLAPS, s.getRelationship(shape));

    // Something overlapping the world on only one side
    s = new XYZSolid(PlanetModel.SPHERE, -2.0, 0.0, -2.0, 2.0, -2.0, 2.0);
    // Some things should be disjoint...
    shape = new GeoCircle(PlanetModel.SPHERE, 0.0, 0.0, 0.1);
    assertEquals(GeoArea.DISJOINT, s.getRelationship(shape));
    // And, some things should be within... 
    shape = new GeoCircle(PlanetModel.SPHERE, 0.0, Math.PI, 0.1);
    assertEquals(GeoArea.WITHIN, s.getRelationship(shape));
    // And, some things should overlap.
    shape = new GeoCircle(PlanetModel.SPHERE, 0.0, Math.PI * 0.5, 0.1);
    assertEquals(GeoArea.OVERLAPS, s.getRelationship(shape));

    // Partial world should be contained by GeoWorld object...
    shape = new GeoWorld(PlanetModel.SPHERE);
    assertEquals(GeoArea.CONTAINS, s.getRelationship(shape));
    
    // Something inside the world
    s = new XYZSolid(PlanetModel.SPHERE, -0.1, 0.1, -0.1, 0.1, -0.1, 0.1);
    // All shapes should be disjoint
    shape = new GeoCircle(PlanetModel.SPHERE, 0.0, 0.0, 0.1);
    assertEquals(GeoArea.DISJOINT, s.getRelationship(shape));
    shape = new GeoWorld(PlanetModel.SPHERE);
    assertEquals(GeoArea.DISJOINT, s.getRelationship(shape));
    
  }


}
