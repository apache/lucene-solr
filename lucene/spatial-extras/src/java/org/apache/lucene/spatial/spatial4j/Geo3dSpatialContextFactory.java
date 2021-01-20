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

import java.util.Map;
import org.apache.lucene.spatial3d.geom.PlanetModel;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.context.SpatialContextFactory;

/**
 * Geo3d implementation of {@link SpatialContextFactory}
 *
 * @lucene.experimental
 */
public class Geo3dSpatialContextFactory extends SpatialContextFactory {

  /** The default planet model */
  private static final PlanetModel DEFAULT_PLANET_MODEL = PlanetModel.SPHERE;

  /** The planet model */
  public PlanetModel planetModel;

  /** Empty Constructor. */
  public Geo3dSpatialContextFactory() {
    this.binaryCodecClass = Geo3dBinaryCodec.class;
    this.shapeFactoryClass = Geo3dShapeFactory.class;
  }

  @Override
  public SpatialContext newSpatialContext() {
    if (planetModel == null) {
      planetModel = DEFAULT_PLANET_MODEL;
    }
    if (distCalc == null) {
      this.distCalc = new Geo3dDistanceCalculator(planetModel);
    }
    return new SpatialContext(this);
  }

  @Override
  protected void init(Map<String, String> args, ClassLoader classLoader) {
    initPlanetModel(args);
    super.init(args, classLoader);
  }

  protected void initPlanetModel(Map<String, String> args) {
    String planetModel = args.get("planetModel");
    if (planetModel != null) {
      if (planetModel.equalsIgnoreCase("sphere")) {
        this.planetModel = PlanetModel.SPHERE;
      } else if (planetModel.equalsIgnoreCase("wgs84")) {
        this.planetModel = PlanetModel.WGS84;
      } else if (planetModel.equalsIgnoreCase("clarke1866")) {
        this.planetModel = PlanetModel.CLARKE_1866;
      } else {
        throw new RuntimeException("Unknown planet model: " + planetModel);
      }
    } else {
      this.planetModel = DEFAULT_PLANET_MODEL;
    }
  }

  @Override
  protected void initCalculator() {
    String calcStr = this.args.get("distCalculator");
    if (calcStr == null) {
      return;
    } else if (calcStr.equals("geo3d")) {
      this.distCalc = new Geo3dDistanceCalculator(planetModel);
    } else {
      super.initCalculator(); // some other distance calculator
    }
  }
}
