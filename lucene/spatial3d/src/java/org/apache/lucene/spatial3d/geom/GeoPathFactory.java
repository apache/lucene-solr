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

import java.util.ArrayList;
import java.util.List;

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
    if (maxCutoffAngle < Vector.MINIMUM_ANGULAR_RESOLUTION) {
      return new GeoDegeneratePath(planetModel, filterPoints(pathPoints));
    }
    return new GeoStandardPath(planetModel, maxCutoffAngle, filterPoints(pathPoints));
  }

  /** Filter duplicate points.
   * @param pathPoints with the arras of points.
   * @return the filtered array.
   */
  private static GeoPoint[] filterPoints(final GeoPoint[] pathPoints) {
    final List<GeoPoint> noIdenticalPoints = new ArrayList<>(pathPoints.length);
    for (int i = 0; i < pathPoints.length - 1; i++) {
      if (!pathPoints[i].isNumericallyIdentical(pathPoints[i + 1])) {
        noIdenticalPoints.add(pathPoints[i]);
      }
    }
    noIdenticalPoints.add(pathPoints[pathPoints.length - 1]);
    return noIdenticalPoints.toArray(new GeoPoint[noIdenticalPoints.size()]);
  }

}
