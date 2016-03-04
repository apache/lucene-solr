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
package org.apache.solr.util;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import org.locationtech.spatial4j.distance.DistanceUtils;
import org.apache.solr.schema.AbstractSpatialFieldType;

/**
 * Used with a spatial field type for all distance measurements.
 * 
 * @see AbstractSpatialFieldType
 */
public class DistanceUnits {
  public final static String KILOMETERS_PARAM = "kilometers";
  public final static String MILES_PARAM = "miles";
  public final static String DEGREES_PARAM = "degrees";

  // Singleton distance units instances
  public final static DistanceUnits KILOMETERS = new DistanceUnits(KILOMETERS_PARAM, DistanceUtils.EARTH_MEAN_RADIUS_KM, 
      DistanceUtils.KM_TO_DEG);
  public final static DistanceUnits MILES = new DistanceUnits(MILES_PARAM, DistanceUtils.EARTH_MEAN_RADIUS_MI, 
      DistanceUtils.MILES_TO_KM * DistanceUtils.KM_TO_DEG);
  public final static DistanceUnits DEGREES = new DistanceUnits(DEGREES_PARAM, 180.0/Math.PI, 1.0);

  //volatile so other threads see when we replace when copy-on-write
  private static volatile Map<String, DistanceUnits> instances = ImmutableMap.of(
      KILOMETERS_PARAM, KILOMETERS,
      MILES_PARAM, MILES,
      DEGREES_PARAM, DEGREES);

  private final String stringIdentifier;
  private final double earthRadius;
  private final double multiplierThisToDegrees;
  private final double multiplierDegreesToThis;

  private DistanceUnits(String str, double earthRadius, double multiplierThisToDegrees) {
    this.stringIdentifier = str;
    this.earthRadius = earthRadius;
    this.multiplierThisToDegrees = multiplierThisToDegrees;
    this.multiplierDegreesToThis = 1.0 / multiplierThisToDegrees;
  }

  /**
   * Parses a string representation of distance units and returns its implementing class instance.
   * Preferred way to parse a DistanceUnits would be to use {@link AbstractSpatialFieldType#parseDistanceUnits(String)},
   * since it will default to one defined on the field type if the string is null.
   * 
   * @param str String representation of distance units, e.g. "kilometers", "miles" etc. (null ok)
   * @return an instance of the concrete DistanceUnits, null if not found.
   */
  public static DistanceUnits valueOf(String str) {
    return instances.get(str);
  }

  public static Set<String> getSupportedUnits() {
    return instances.keySet();
  }

  /**
   * 
   * @return Radius of the earth in this distance units
   */
  public double getEarthRadius() {
    return earthRadius;
  }

  /**
   * 
   * @return multiplier needed to convert a distance in current units to degrees
   */
  public double multiplierFromThisUnitToDegrees() {
    return multiplierThisToDegrees;
  }

  /**
   * 
   * @return multiplier needed to convert a distance in degrees to current units
   */
  public double multiplierFromDegreesToThisUnit() {
    return multiplierDegreesToThis;
  }

  /**
   * 
   * @return the string identifier associated with this units instance
   */
  public String getStringIdentifier() {
    return stringIdentifier;
  }

  /**
   * Custom distance units can be supplied using this method. It's thread-safe.
   * 
   * @param strId string identifier for the units
   * @param earthRadius radius of earth in supplied units
   * @param multiplierThisToDegrees multiplier to convert to degrees
   */
  public static synchronized void addUnits(String strId, double earthRadius, double multiplierThisToDegrees) {
    //copy-on-write.
    Map<String, DistanceUnits> map = new HashMap<String, DistanceUnits>(instances);
    map.put(strId, new DistanceUnits(strId, earthRadius, multiplierThisToDegrees));
    instances = ImmutableMap.copyOf(map);
  }

  @Override
  public String toString() {
    return getStringIdentifier();
  }
}
