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
 * Base class of a family of 3D rectangles, bounded on six sides by X,Y,Z limits
 *
 * @lucene.internal
 */
abstract class BaseXYZSolid extends BasePlanetObject implements XYZSolid {

  /** Unit vector in x */
  protected static final Vector xUnitVector = new Vector(1.0, 0.0, 0.0);
  /** Unit vector in y */
  protected static final Vector yUnitVector = new Vector(0.0, 1.0, 0.0);
  /** Unit vector in z */
  protected static final Vector zUnitVector = new Vector(0.0, 0.0, 1.0);
  
  /** Vertical plane normal to x unit vector passing through origin */
  protected static final Plane xVerticalPlane = new Plane(0.0, 1.0, 0.0, 0.0);
  /** Vertical plane normal to y unit vector passing through origin */
  protected static final Plane yVerticalPlane = new Plane(1.0, 0.0, 0.0, 0.0);

  /** Empty point vector */
  protected static final GeoPoint[] EMPTY_POINTS = new GeoPoint[0];
  
  /**
   * Base solid constructor.
   *@param planetModel is the planet model.
   */
  public BaseXYZSolid(final PlanetModel planetModel) {
    super(planetModel);
  }
  
  /** Construct a single array from a number of individual arrays.
   * @param pointArrays is the array of point arrays.
   * @return the single unified array.
   */
  protected static GeoPoint[] glueTogether(final GeoPoint[]... pointArrays) {
    int count = 0;
    for (final GeoPoint[] pointArray : pointArrays) {
      count += pointArray.length;
    }
    final GeoPoint[] rval = new GeoPoint[count];
    count = 0;
    for (final GeoPoint[] pointArray : pointArrays) {
      for (final GeoPoint point : pointArray) {
        rval[count++] = point;
      }
    }
    return rval;
  }
  
  @Override
  public boolean isWithin(final Vector point) {
    return isWithin(point.x, point.y, point.z);
  }
  
  @Override
  public abstract boolean isWithin(final double x, final double y, final double z);
  
  // Signals for relationship of edge points to shape
  
  /** All edgepoints inside shape */
  protected final static int ALL_INSIDE = 0;
  /** Some edgepoints inside shape */
  protected final static int SOME_INSIDE = 1;
  /** No edgepoints inside shape */
  protected final static int NONE_INSIDE = 2;
  /** No edgepoints at all (means a shape that is the whole world) */
  protected final static int NO_EDGEPOINTS = 3;

  /** Determine the relationship between this area and the provided
   * shape's edgepoints.
   *@param path is the shape.
   *@return the relationship.
   */
  protected int isShapeInsideArea(final GeoShape path) {
    final GeoPoint[] pathPoints = path.getEdgePoints();
    if (pathPoints.length == 0)
      return NO_EDGEPOINTS;
    boolean foundOutside = false;
    boolean foundInside = false;
    for (final GeoPoint p : pathPoints) {
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

  /** Determine the relationship between a shape and this area's
   * edgepoints.
   *@param path is the shape.
   *@return the relationship.
   */
  protected int isAreaInsideShape(final GeoShape path) {
    final GeoPoint[] edgePoints = getEdgePoints();
    if (edgePoints.length == 0) {
      return NO_EDGEPOINTS;
    }
    boolean foundOutside = false;
    boolean foundInside = false;
    for (final GeoPoint p : edgePoints) {
      if (path.isWithin(p)) {
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

  /** Get the edge points for this shape.
   *@return the edge points.
   */
  protected abstract GeoPoint[] getEdgePoints();
  
  @Override
  public boolean equals(Object o) {
    if (!(o instanceof BaseXYZSolid))
      return false;
    BaseXYZSolid other = (BaseXYZSolid) o;
    return super.equals(other);
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }

}
  
