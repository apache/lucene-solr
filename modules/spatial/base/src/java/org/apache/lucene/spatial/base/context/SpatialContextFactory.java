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

package org.apache.lucene.spatial.base.context;

import org.apache.lucene.spatial.base.context.simple.SimpleSpatialContext;
import org.apache.lucene.spatial.base.context.simple.SimpleSpatialContextFactory;
import org.apache.lucene.spatial.base.distance.*;
import org.apache.lucene.spatial.base.shape.Rectangle;

import java.util.Map;

/**
 * Factory for a SpatialContext.
 */
public abstract class SpatialContextFactory {
  protected Map<String, String> args;
  protected ClassLoader classLoader;
  
  protected DistanceUnits units;
  protected DistanceCalculator calculator;
  protected Rectangle worldBounds;

  /**
   * The factory class is lookuped up via "spatialContextFactory" in args
   * then falling back to a Java system property (with initial caps). If neither are specified
   * then {@link SimpleSpatialContextFactory} is chosen.
   * @param args
   * @param classLoader
   * @return
   */
  public static SpatialContext makeSpatialContext(Map<String,String> args, ClassLoader classLoader) {
    SpatialContextFactory instance;
    String cname = args.get("spatialContextFactory");
    if (cname == null)
      cname = System.getProperty("SpatialContextFactory");
    if (cname == null)
      instance = new SimpleSpatialContextFactory();
    else {
      try {
        Class c = classLoader.loadClass(cname);
        instance = (SpatialContextFactory) c.newInstance();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    instance.init(args,classLoader);
    return instance.newSpatialContext();
  }

  protected void init(Map<String, String> args, ClassLoader classLoader) {
    this.args = args;
    this.classLoader = classLoader;
    initUnits();
    initCalculator();
    initWorldBounds();
  }

  protected void initUnits() {
    String unitsStr = args.get("units");
    if (unitsStr != null)
      units = DistanceUnits.findDistanceUnit(unitsStr);
    if (units == null)
      units = DistanceUnits.KILOMETERS;
  }

  protected void initCalculator() {
    String calcStr = args.get("distCalculator");
    if (calcStr == null)
      return;
    if (calcStr.equalsIgnoreCase("haversine")) {
      calculator = new GeodesicSphereDistCalc.Haversine(units.earthRadius());
    } else if (calcStr.equalsIgnoreCase("lawOfCosines")) {
      calculator = new GeodesicSphereDistCalc.LawOfCosines(units.earthRadius());
    } else if (calcStr.equalsIgnoreCase("vincentySphere")) {
      calculator = new GeodesicSphereDistCalc.Vincenty(units.earthRadius());
    } else if (calcStr.equalsIgnoreCase("cartesian")) {
      calculator = new CartesianDistCalc();
    } else if (calcStr.equalsIgnoreCase("cartesian^2")) {
      calculator = new CartesianDistCalc(true);
    } else {
      throw new RuntimeException("Unknown calculator: "+calcStr);
    }
  }

  protected void initWorldBounds() {
    String worldBoundsStr = args.get("worldBounds");
    if (worldBoundsStr == null)
      return;
    //kinda ugly we do this just to read a rectangle.  TODO refactor
    SimpleSpatialContext simpleCtx = new SimpleSpatialContext(units, calculator, null);
    worldBounds = (Rectangle) simpleCtx.readShape(worldBoundsStr);
  }

  /** Subclasses should simply construct the instance from the initialized configuration. */
  protected abstract SpatialContext newSpatialContext();

}
