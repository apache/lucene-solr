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
 * Class which constructs a GeoPath representing an arbitrary path.
 *
 * @lucene.experimental
 */
public class GeoPathFactory {
  private GeoPathFactory() {
  }

  /**
   * Create a GeoPath of the right kind given the specified information.
   * @param planetModel is the planet model.
   * @param maxCutoffAngle is the width of the path, measured as an angle.
   * @param pathPoints are the points in the path.
   * @return a GeoPath corresponding to what was specified.
   */
  public static GeoPath makeGeoPath(final PlanetModel planetModel, final double maxCutoffAngle, final GeoPoint[] pathPoints) {
    return new GeoStandardPath(planetModel, maxCutoffAngle, pathPoints);
  }

}
