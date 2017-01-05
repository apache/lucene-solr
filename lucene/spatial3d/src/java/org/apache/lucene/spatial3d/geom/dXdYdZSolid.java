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
 * 3D rectangle, bounded on six sides by X,Y,Z limits, degenerate in all dimensions
 *
 * @lucene.internal
 */
class dXdYdZSolid extends BaseXYZSolid {

  /** On surface? */
  protected final boolean isOnSurface;
  /** The point */
  protected final GeoPoint thePoint;
  
  /** These are the edge points of the shape, which are defined to be at least one point on
   * each surface area boundary.  In the case of a solid, this includes points which represent
   * the intersection of XYZ bounding planes and the planet, as well as points representing
   * the intersection of single bounding planes with the planet itself.
   */
  protected final GeoPoint[] edgePoints;

  /** Empty array of {@link GeoPoint}. */
  protected static final GeoPoint[] nullPoints = new GeoPoint[0];
  
  /**
   * Sole constructor
   *
   *@param planetModel is the planet model.
   *@param X is the X value.
   *@param Y is the Y value.
   *@param Z is the Z value.
   */
  public dXdYdZSolid(final PlanetModel planetModel,
    final double X,
    final double Y,
    final double Z) {
    super(planetModel);
    isOnSurface = planetModel.pointOnSurface(X,Y,Z);
    if (isOnSurface) {
      thePoint = new GeoPoint(X,Y,Z);
      edgePoints = new GeoPoint[]{thePoint};
    } else {
      thePoint = null;
      edgePoints = nullPoints;
    }
  }

  @Override
  protected GeoPoint[] getEdgePoints() {
    return edgePoints;
  }
  
  @Override
  public boolean isWithin(final double x, final double y, final double z) {
    if (!isOnSurface) {
      return false;
    }
    return thePoint.isIdentical(x,y,z);
  }

  @Override
  public int getRelationship(final GeoShape path) {
    if (!isOnSurface) {
      return DISJOINT;
    }
    
    //System.err.println(this+" getrelationship with "+path);
    final int insideRectangle = isShapeInsideArea(path);
    if (insideRectangle == SOME_INSIDE) {
      //System.err.println(" some shape points inside area");
      return OVERLAPS;
    }

    // Figure out if the entire XYZArea is contained by the shape.
    final int insideShape = isAreaInsideShape(path);
    if (insideShape == SOME_INSIDE) {
      //System.err.println(" some area points inside shape");
      return OVERLAPS;
    }

    if (insideRectangle == ALL_INSIDE && insideShape == ALL_INSIDE) {
      //System.err.println(" inside of each other");
      return OVERLAPS;
    }

    if (insideRectangle == ALL_INSIDE) {
      //System.err.println(" shape inside area entirely");
      return WITHIN;
    }

    if (insideShape == ALL_INSIDE) {
      //System.err.println(" shape contains area entirely");
      return CONTAINS;
    }
    //System.err.println(" disjoint");
    return DISJOINT;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof dXdYdZSolid))
      return false;
    dXdYdZSolid other = (dXdYdZSolid) o;
    if (!super.equals(other) ||
      other.isOnSurface != isOnSurface) {
      return false;
    }
    if (isOnSurface) {
      return other.thePoint.equals(thePoint);
    }
    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (isOnSurface?1:0);
    if (isOnSurface) {
      result = 31 * result  + thePoint.hashCode();
    }
    return result;
  }

  @Override
  public String toString() {
    return "dXdYdZSolid: {planetmodel="+planetModel+", isOnSurface="+isOnSurface+", thePoint="+thePoint+"}";
  }
  
}
  
