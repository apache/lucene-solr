package org.apache.lucene.spatial.spatial4j.geo3d;

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
 * Base extended shape object.
 *
 * @lucene.internal
 */
public abstract class GeoBaseExtendedShape implements GeoShape {
  protected final static GeoPoint NORTH_POLE = new GeoPoint(0.0, 0.0, 1.0);
  protected final static GeoPoint SOUTH_POLE = new GeoPoint(0.0, 0.0, -1.0);

  public GeoBaseExtendedShape() {
  }

  /**
   * Check if a point is within this shape.
   *
   * @param point is the point to check.
   * @return true if the point is within this shape
   */
  @Override
  public abstract boolean isWithin(final Vector point);

  /**
   * Check if a point is within this shape.
   *
   * @param x is x coordinate of point to check.
   * @param y is y coordinate of point to check.
   * @param z is z coordinate of point to check.
   * @return true if the point is within this shape
   */
  @Override
  public abstract boolean isWithin(final double x, final double y, final double z);

  /**
   * Return a sample point that is on the edge of the shape.
   *
   * @return a number of edge points, one for each disconnected edge.
   */
  @Override
  public abstract GeoPoint[] getEdgePoints();

  /**
   * Assess whether a plane, within the provided bounds, intersects
   * with the shape.
   *
   * @param plane  is the plane to assess for intersection with the shape's edges or
   *               bounding curves.
   * @param bounds are a set of bounds that define an area that an
   *               intersection must be within in order to qualify (provided by a GeoArea).
   * @return true if there's such an intersection, false if not.
   */
  @Override
  public abstract boolean intersects(final Plane plane, final GeoPoint[] notablePoints, final Membership... bounds);

  /**
   * Compute longitude/latitude bounds for the shape.
   *
   * @param bounds is the optional input bounds object.  If this is null,
   *               a bounds object will be created.  Otherwise, the input object will be modified.
   * @return a Bounds object describing the shape's bounds.  If the bounds cannot
   * be computed, then return a Bounds object with noLongitudeBound,
   * noTopLatitudeBound, and noBottomLatitudeBound.
   */
  @Override
  public Bounds getBounds(Bounds bounds) {
    if (bounds == null)
      bounds = new Bounds();
    if (isWithin(NORTH_POLE)) {
      bounds.noTopLatitudeBound().noLongitudeBound();
    }
    if (isWithin(SOUTH_POLE)) {
      bounds.noBottomLatitudeBound().noLongitudeBound();
    }
    return bounds;
  }
}
