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

/**
 * Base extended shape object.
 *
 * @lucene.internal
 */
public abstract class GeoBaseShape extends BasePlanetObject implements GeoShape {

  /**
   * Constructor.
   *
   * @param planetModel is the planet model to use.
   */
  public GeoBaseShape(final PlanetModel planetModel) {
    super(planetModel);
  }

  @Override
  public void getBounds(Bounds bounds) {
    if (isWithin(planetModel.NORTH_POLE)) {
      bounds.noTopLatitudeBound().noLongitudeBound().addPoint(planetModel.NORTH_POLE);
    }
    if (isWithin(planetModel.SOUTH_POLE)) {
      bounds.noBottomLatitudeBound().noLongitudeBound().addPoint(planetModel.SOUTH_POLE);
    }
    if (isWithin(planetModel.MIN_X_POLE)) {
      bounds.addPoint(planetModel.MIN_X_POLE);
    }
    if (isWithin(planetModel.MAX_X_POLE)) {
      bounds.addPoint(planetModel.MAX_X_POLE);
    }
    if (isWithin(planetModel.MIN_Y_POLE)) {
      bounds.addPoint(planetModel.MIN_Y_POLE);
    }
    if (isWithin(planetModel.MAX_Y_POLE)) {
      bounds.addPoint(planetModel.MAX_Y_POLE);
    }
  }
}
