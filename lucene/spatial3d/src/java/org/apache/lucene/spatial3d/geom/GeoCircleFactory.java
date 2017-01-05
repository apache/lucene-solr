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
 * Class which constructs a GeoCircle representing an arbitrary circle.
 *
 * @lucene.experimental
 */
public class GeoCircleFactory {
  private GeoCircleFactory() {
  }

  /**
   * Create a GeoCircle of the right kind given the specified bounds.
   * @param planetModel is the planet model.
   * @param latitude is the center latitude.
   * @param longitude is the center longitude.
   * @param radius is the radius angle.
   * @return a GeoCircle corresponding to what was specified.
   */
  public static GeoCircle makeGeoCircle(final PlanetModel planetModel, final double latitude, final double longitude, final double radius) {
    if (radius < Vector.MINIMUM_ANGULAR_RESOLUTION) {
      return new GeoDegeneratePoint(planetModel, latitude, longitude);
    }
    return new GeoStandardCircle(planetModel, latitude, longitude, radius);
  }

}
