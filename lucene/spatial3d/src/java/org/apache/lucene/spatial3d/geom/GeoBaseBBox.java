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
 * All bounding box shapes can derive from this base class, which furnishes
 * some common code
 *
 * @lucene.internal
 */
abstract class GeoBaseBBox extends GeoBaseMembershipShape implements GeoBBox {

  /** Construct, given planet model.
   *@param planetModel is the planet model.
   */
  public GeoBaseBBox(final PlanetModel planetModel) {
    super(planetModel);
  }

  // Signals for relationship of edge points to shape
  
  /** All edgepoints inside shape */
  protected final static int ALL_INSIDE = 0;
  /** Some edgepoints inside shape */
  protected final static int SOME_INSIDE = 1;
  /** No edgepoints inside shape */
  protected final static int NONE_INSIDE = 2;

  /** Determine the relationship between this BBox and the provided
   * shape's edgepoints.
   *@param path is the shape.
   *@return the relationship.
   */
  protected int isShapeInsideBBox(final GeoShape path) {
    final GeoPoint[] pathPoints = path.getEdgePoints();
    boolean foundOutside = false;
    boolean foundInside = false;
    for (GeoPoint p : pathPoints) {
      if (isWithin(p)) {
        foundInside = true;
      } else {
        foundOutside = true;
      }
      if (foundInside && foundOutside) {
        return SOME_INSIDE;
      }
    }
    if (!foundInside && !foundOutside)
      return NONE_INSIDE;
    if (foundInside && !foundOutside)
      return ALL_INSIDE;
    if (foundOutside && !foundInside)
      return NONE_INSIDE;
    return SOME_INSIDE;
  }

}

