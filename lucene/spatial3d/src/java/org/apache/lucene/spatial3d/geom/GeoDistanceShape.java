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
 * Distance shapes have capabilities of both geohashing and distance computation (which also
 * includes point membership determination).
 *
 * @lucene.experimental
 */
public interface GeoDistanceShape extends GeoAreaShape, GeoDistance {

  /**
   * Compute a bound based on a provided distance measure. This method takes an input distance and
   * distance metric and provides bounds on the shape if reduced to match that distance. The method
   * is allowed to return bounds that are larger than the distance would indicate, but never
   * smaller.
   *
   * @param bounds is the bounds object to update.
   * @param distanceStyle describes the type of distance metric provided.
   * @param distanceValue is the distance metric to use. It is presumed that the distance metric was
   *     produced with the same distance style as is provided to this method.
   */
  public void getDistanceBounds(
      final Bounds bounds, final DistanceStyle distanceStyle, final double distanceValue);
}
