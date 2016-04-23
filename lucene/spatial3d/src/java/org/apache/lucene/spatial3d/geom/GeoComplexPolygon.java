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
import java.util.BitSet;
import java.util.List;
import java.util.HashMap;
import java.util.Map;

/**
 * GeoComplexPolygon objects are structures designed to handle very large numbers of edges.
 * They perform very well in this case compared to the alternatives, which all have O(N) evaluation
 * and O(N^2) setup times.  Complex polygons have O(N) setup times and best case O(log(N))
 * evaluation times.
 *
 * The tradeoff is that these objects perform object creation when evaluating intersects() and
 * isWithin().
 *
 * @lucene.internal
 */
class GeoComplexPolygon extends GeoBasePolygon {
  
  /**
   * Create a complex polygon from multiple lists of points, and a single point which is known to be in or out of
   * set.
   *@param planetModel is the planet model.
   *@param pointsList is the list of lists of edge points.  The edge points describe edges, and have an implied
   *  return boundary, so that N edges require N points.  These points have furthermore been filtered so that
   *  no adjacent points are identical (within the bounds of the definition used by this package).  It is assumed
   *  that no edges intersect, but the structure can contain both outer rings as well as holes.
   *@param testPoint is the point whose in/out of setness is known.
   *@param testPointInSet is true if the test point is considered "within" the polygon.
   */
  public GeoComplexPolygon(final PlanetModel planetModel, final List<List<GeoPoint>> pointsList, final GeoPoint testPoint, final boolean testPointInSet) {
    super(planetModel);
    // MHL
  }

  /** Compute a legal point index from a possibly illegal one, that may have wrapped.
   *@param index is the index.
   *@return the normalized index.
   */
  protected int legalIndex(int index) {
    while (index >= points.size())
      index -= points.size();
    while (index < 0) {
      index += points.size();
    }
    return index;
  }

  @Override
  public boolean isWithin(final double x, final double y, final double z) {
    // MHL
    return false;
  }
  
  @Override
  public GeoPoint[] getEdgePoints() {
    return edgePoints;
  }

  @Override
  public boolean intersects(final Plane p, final GeoPoint[] notablePoints, final Membership... bounds) {
    // MHL
    return false;
  }


  @Override
  public void getBounds(Bounds bounds) {
    super.getBounds(bounds);
    // MHL
  }

  @Override
  protected double outsideDistance(final DistanceStyle distanceStyle, final double x, final double y, final double z) {
    // MHL
  }

  @Override
  public boolean equals(Object o) {
    // MHL
    return false;
  }

  @Override
  public int hashCode() {
    // MHL
  }

  @Override
  public String toString() {
    return "GeoComplexPolygon: {planetmodel=" + planetModel + "}";
  }
}
  
