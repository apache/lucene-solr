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
 * Generic shape. This describes methods that help GeoAreas figure out how they interact with a
 * shape, for the purposes of coming up with a set of geo hash values.
 *
 * @lucene.experimental
 */
public interface GeoShape extends Bounded, Membership, PlanetObject {

  /**
   * Return a sample point that is on the outside edge/boundary of the shape.
   *
   * @return samples of all edge points from distinct edge sections. Typically one point is
   *     returned, but zero or two are also possible.
   */
  public GeoPoint[] getEdgePoints();

  /**
   * Assess whether a plane, within the provided bounds, intersects with the shape's edges. Any
   * overlap, even a single point, is considered to be an intersection. Note well that this method
   * is allowed to return "true" if there are internal edges of a composite shape which intersect
   * the plane. Doing this can cause getRelationship() for most GeoBBox shapes to return OVERLAPS
   * rather than the more correct CONTAINS, but that cannot be helped for some complex shapes that
   * are built out of overlapping parts.
   *
   * @param plane is the plane to assess for intersection with the shape's edges or bounding curves.
   * @param notablePoints represents the intersections of the plane with the supplied bounds. These
   *     are used to disambiguate when two planes are identical and it needs to be determined
   *     whether any points exist that fulfill all the bounds.
   * @param bounds are a set of bounds that define an area that an intersection must be within in
   *     order to qualify (provided by a GeoArea).
   * @return true if there's such an intersection, false if not.
   */
  public boolean intersects(
      final Plane plane, final GeoPoint[] notablePoints, final Membership... bounds);
}
