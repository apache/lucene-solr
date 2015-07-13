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

/**
 * Base extended shape object.
 *
 * @lucene.internal
 */
public abstract class GeoBaseShape extends BasePlanetObject implements GeoShape {

  /** Constructor.
   *@param planetModel is the planet model to use.
   */
  public GeoBaseShape(final PlanetModel planetModel) {
    super(planetModel);
  }

  @Override
  public Bounds getBounds(Bounds bounds) {
    if (bounds == null)
      bounds = new Bounds();
    if (isWithin(planetModel.NORTH_POLE)) {
      bounds.noTopLatitudeBound().noLongitudeBound();
    }
    if (isWithin(planetModel.SOUTH_POLE)) {
      bounds.noBottomLatitudeBound().noLongitudeBound();
    }
    return bounds;
  }

}


