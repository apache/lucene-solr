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
 * Base class to create a composite of GeoMembershipShapes
 *
 * @param <T> is the type of GeoMembershipShapes of the composite.
 * @lucene.internal
 */
abstract class GeoBaseCompositeMembershipShape<T extends GeoMembershipShape>
    extends GeoBaseCompositeShape<T> implements GeoMembershipShape{

  /**
   * Constructor.
   */
  GeoBaseCompositeMembershipShape(PlanetModel planetModel) {
    super(planetModel);
  }

  @Override
  public double computeOutsideDistance(final DistanceStyle distanceStyle, final GeoPoint point) {
    return computeOutsideDistance(distanceStyle, point.x, point.y, point.z);
  }

  @Override
  public double computeOutsideDistance(final DistanceStyle distanceStyle, final double x, final double y, final double z) {
    if (isWithin(x,y,z))
      return 0.0;
    double distance = Double.POSITIVE_INFINITY;
    for (GeoMembershipShape shape : shapes) {
      final double normalDistance = shape.computeOutsideDistance(distanceStyle, x, y, z);
      if (normalDistance < distance) {
        distance = normalDistance;
      }
    }
    return distance;
  }
}
