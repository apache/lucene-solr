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

package org.apache.lucene.spatial.spatial4j.performance;

import org.apache.lucene.spatial.spatial4j.Geo3dPointShape;
import org.apache.lucene.spatial.spatial4j.Geo3dShape;
import org.apache.lucene.spatial3d.geom.GeoAreaShape;
import org.apache.lucene.spatial3d.geom.GeoPointShape;
import org.locationtech.spatial4j.shape.Shape;

/**
 * Test performance between geohash and s2 for points only
 */
public class Geo3dPerformancePoints extends Geo3dPerformanceRptTest{

  @Override
  protected int numberIndexedshapes() {
    return 50000;
  }

  @Override
  protected Shape randomIndexedShape() {
    int type = 9; //Points (We should make those constants public)
    GeoAreaShape areaShape = shapeGenerator.randomGeoAreaShape(type, planetModel);
    return new Geo3dPointShape((GeoPointShape) areaShape, ctx);
  }

  @Override
  protected int numberQueryShapes() {
    return 50;
  }

  @Override
  protected Shape randomQueryShape() {
    int type = shapeGenerator.randomShapeType();
    GeoAreaShape areaShape = shapeGenerator.randomGeoAreaShape(type, planetModel);
    if (areaShape instanceof GeoPointShape) {
      return new Geo3dPointShape((GeoPointShape) areaShape, ctx);
    }
    return new Geo3dShape<>(areaShape, ctx);
  }

  @Override
  protected boolean pointsOnly() {
    return true;
  }

  @Override
  protected double precision() {
    return 0.000001;
  }

  @Override
  protected double distErrPct() {
    return 0.0;
  }
}
