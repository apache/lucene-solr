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

package org.apache.lucene.spatial.base.distance;

/**
 * Enum representing difference distance units, currently only kilometers and
 * miles
 */
public enum DistanceUnits {

  //TODO do we need circumference?
  KILOMETERS("km", DistanceUtils.EARTH_MEAN_RADIUS_KM, 40076),
  MILES("miles", DistanceUtils.EARTH_MEAN_RADIUS_MI, 24902),
  RADIANS("radians", 1, Math.PI * 2),//experimental
  CARTESIAN("u", -1, -1);


  private final String units;

  private final double earthCircumference;

  private final double earthRadius;

  /**
   * Creates a new DistanceUnit that represents the given unit
   *
   * @param units Distance unit in String form
   * @param earthRadius Radius of the Earth in the specific distance unit
   * @param earthCircumfence Circumference of the Earth in the specific distance unit
   */
  DistanceUnits(String units, double earthRadius, double earthCircumfence) {
    this.units = units;
    this.earthCircumference = earthCircumfence;
    this.earthRadius = earthRadius;
  }

  /**
   * Returns the DistanceUnit which represents the given unit
   *
   * @param unit Unit whose DistanceUnit should be found
   * @return DistanceUnit representing the unit
   * @throws IllegalArgumentException if no DistanceUnit which represents the given unit is found
   */
  public static DistanceUnits findDistanceUnit(String unit) {
    if (MILES.getUnits().equalsIgnoreCase(unit) || unit.equalsIgnoreCase("mi")) {
      return MILES;
    }
    if (KILOMETERS.getUnits().equalsIgnoreCase(unit)) {
      return KILOMETERS;
    }
    if (CARTESIAN.getUnits().equalsIgnoreCase(unit) || unit.length()==0) {
      return CARTESIAN;
    }
    throw new IllegalArgumentException("Unknown distance unit " + unit);
  }

  /**
   * Converts the given distance in given DistanceUnit, to a distance in the unit represented by {@code this}
   *
   * @param distance Distance to convert
   * @param from Unit to convert the distance from
   * @return Given distance converted to the distance in the given unit
   */
  public double convert(double distance, DistanceUnits from) {
    if (from == this) {
      return distance;
    }
    if (this == CARTESIAN || from == CARTESIAN) {
      throw new IllegalStateException("Can't convert cartesian distances: "+from+" -> "+this);
    }
    return (this == MILES) ? distance * DistanceUtils.KM_TO_MILES : distance * DistanceUtils.MILES_TO_KM;
  }

  /**
   * Returns the string representation of the distance unit
   *
   * @return String representation of the distance unit
   */
  public String getUnits() {
    return units;
  }

  /**
   * Returns the <a href="http://en.wikipedia.org/wiki/Earth_radius">average earth radius</a>
   *
   * @return the average earth radius
   */
  public double earthRadius() {
    return earthRadius;
  }

  /**
   * Returns the <a href="http://www.lyberty.com/encyc/articles/earth.html">circumference of the Earth</a>
   *
   * @return  the circumference of the Earth
   */
  public double earthCircumference() {
    return earthCircumference;
  }
  
  public boolean isGeo() {
    return earthRadius > 0;
  }
}

