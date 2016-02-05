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
package org.apache.lucene.geo3d;

/**
 * Arc distance computation style.
 *
 * @lucene.experimental
 */
public class ArcDistance implements DistanceStyle {
  
  /** An instance of the ArcDistance DistanceStyle. */
  public final static ArcDistance INSTANCE = new ArcDistance();
  
  /** Constructor.
   */
  public ArcDistance() {
  }
  
  @Override
  public double computeDistance(final GeoPoint point1, final GeoPoint point2) {
    return point1.arcDistance(point2);
  }
  
  @Override
  public double computeDistance(final GeoPoint point1, final double x2, final double y2, final double z2) {
    return point1.arcDistance(x2,y2,z2);
  }

  @Override
  public double computeDistance(final PlanetModel planetModel, final Plane plane, final GeoPoint point, final Membership... bounds) {
    return plane.arcDistance(planetModel, point, bounds);
  }
  
  @Override
  public double computeDistance(final PlanetModel planetModel, final Plane plane, final double x, final double y, final double z, final Membership... bounds) {
    return plane.arcDistance(planetModel, x,y,z, bounds);
  }

}


