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
 * Factory for {@link org.apache.lucene.geo3d.GeoArea}.
 *
 * @lucene.experimental
 */
public class GeoAreaFactory {
  private GeoAreaFactory() {
  }

  /**
   * Create a GeoArea of the right kind given the specified bounds.
   *
   * @param topLat    is the top latitude
   * @param bottomLat is the bottom latitude
   * @param leftLon   is the left longitude
   * @param rightLon  is the right longitude
   * @return a GeoArea corresponding to what was specified.
   */
  public static GeoArea makeGeoArea(final PlanetModel planetModel, final double topLat, final double bottomLat, final double leftLon, final double rightLon) {
    return GeoBBoxFactory.makeGeoBBox(planetModel, topLat, bottomLat, leftLon, rightLon);
  }

}
