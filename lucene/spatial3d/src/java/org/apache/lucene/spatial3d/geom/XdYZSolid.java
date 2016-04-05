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
 * 3D rectangle, bounded on six sides by X,Y,Z limits, degenerate in Y
 *
 * @lucene.internal
 */
class XdYZSolid extends BaseXYZSolid {

  /** Min-X plane */
  protected final SidedPlane minXPlane;
  /** Max-X plane */
  protected final SidedPlane maxXPlane;
  /** Y plane */
  protected final Plane yPlane;
  /** Min-Z plane */
  protected final SidedPlane minZPlane;
  /** Max-Z plane */
  protected final SidedPlane maxZPlane;
  
  /** These are the edge points of the shape, which are defined to be at least one point on
   * each surface area boundary.  In the case of a solid, this includes points which represent
   * the intersection of XYZ bounding planes and the planet, as well as points representing
   * the intersection of single bounding planes with the planet itself.
   */
  protected final GeoPoint[] edgePoints;

  /** Notable points for YPlane */
  protected final GeoPoint[] notableYPoints;

  /**
   * Sole constructor
   *
   *@param planetModel is the planet model.
   *@param minX is the minimum X value.
   *@param maxX is the maximum X value.
   *@param Y is the Y value.
   *@param minZ is the minimum Z value.
   *@param maxZ is the maximum Z value.
   */
  public XdYZSolid(final PlanetModel planetModel,
    final double minX,
    final double maxX,
    final double Y,
    final double minZ,
    final double maxZ) {
    super(planetModel);
    // Argument checking
    if (maxX - minX < Vector.MINIMUM_RESOLUTION)
      throw new IllegalArgumentException("X values in wrong order or identical");
    if (maxZ - minZ < Vector.MINIMUM_RESOLUTION)
      throw new IllegalArgumentException("Z values in wrong order or identical");

    final double worldMinY = planetModel.getMinimumYValue();
    final double worldMaxY = planetModel.getMaximumYValue();
    
    // Construct the planes
    minXPlane = new SidedPlane(maxX,0.0,0.0,xUnitVector,-minX);
    maxXPlane = new SidedPlane(minX,0.0,0.0,xUnitVector,-maxX);
    yPlane = new Plane(yUnitVector,-Y);
    minZPlane = new SidedPlane(0.0,0.0,maxZ,zUnitVector,-minZ);
    maxZPlane = new SidedPlane(0.0,0.0,minZ,zUnitVector,-maxZ);
      
    // We need at least one point on the planet surface for each manifestation of the shape.
    // There can be up to 2 (on opposite sides of the world).  But we have to go through
    // 4 combinations of adjacent planes in order to find out if any have 2 intersection solution.
    // Typically, this requires 4 square root operations. 
    final GeoPoint[] minXY = minXPlane.findIntersections(planetModel,yPlane,maxXPlane,minZPlane,maxZPlane);
    final GeoPoint[] maxXY = maxXPlane.findIntersections(planetModel,yPlane,minXPlane,minZPlane,maxZPlane);
    final GeoPoint[] YminZ = yPlane.findIntersections(planetModel,minZPlane,maxZPlane,minXPlane,maxXPlane);
    final GeoPoint[] YmaxZ = yPlane.findIntersections(planetModel,maxZPlane,minZPlane,minXPlane,maxXPlane);
      
    notableYPoints = glueTogether(minXY, maxXY, YminZ, YmaxZ);

    // Now, compute the edge points.
    // This is the trickiest part of setting up an XYZSolid.  We've computed intersections already, so
    // we'll start there.  We know that at most there will be two disconnected shapes on the planet surface.
    // But there's also a case where exactly one plane slices through the world, and none of the bounding plane
    // intersections do.  Thus, if we don't find any of the edge intersection cases, we have to look for that last case.
      
    // We need to look at single-plane/world intersections.
    // We detect these by looking at the world model and noting its x, y, and z bounds.
    // The cases we are looking for are when the four corner points for any given
    // plane are all outside of the world, AND that plane intersects the world.
    // There are four corner points all told; we must evaluate these WRT the planet surface.
    final boolean minXYminZ = planetModel.pointOutside(minX, Y, minZ);
    final boolean minXYmaxZ = planetModel.pointOutside(minX, Y, maxZ);
    final boolean maxXYminZ = planetModel.pointOutside(maxX, Y, minZ);
    final boolean maxXYmaxZ = planetModel.pointOutside(maxX, Y, maxZ);

    final GeoPoint[] yEdges;
    if (Y - worldMinY >= -Vector.MINIMUM_RESOLUTION && Y - worldMaxY <= Vector.MINIMUM_RESOLUTION &&
      minX < 0.0 && maxX > 0.0 && minZ < 0.0 && maxZ > 0.0 &&
      minXYminZ && minXYmaxZ && maxXYminZ && maxXYmaxZ) {
      // Find any point on the minY plane that intersects the world
      // First construct a perpendicular plane that will allow us to find a sample point.
      // This plane is vertical and goes through the points (0,0,0) and (0,1,0)
      // Then use it to compute a sample point.
      final GeoPoint intPoint = yPlane.getSampleIntersectionPoint(planetModel, yVerticalPlane);
      if (intPoint != null) {
        yEdges = new GeoPoint[]{intPoint};
      } else {
        yEdges = EMPTY_POINTS;
      }
    } else {
      yEdges = EMPTY_POINTS;
    }

    this.edgePoints = glueTogether(minXY, maxXY, YminZ, YmaxZ, yEdges);
  }

  @Override
  protected GeoPoint[] getEdgePoints() {
    return edgePoints;
  }
  
  @Override
  public boolean isWithin(final double x, final double y, final double z) {
    return minXPlane.isWithin(x, y, z) &&
      maxXPlane.isWithin(x, y, z) &&
      yPlane.evaluateIsZero(x, y, z) &&
      minZPlane.isWithin(x, y, z) &&
      maxZPlane.isWithin(x, y, z);
  }

  @Override
  public int getRelationship(final GeoShape path) {
    //System.err.println(this+" getrelationship with "+path);
    final int insideRectangle = isShapeInsideArea(path);
    if (insideRectangle == SOME_INSIDE) {
      //System.err.println(" some inside");
      return OVERLAPS;
    }

    // Figure out if the entire XYZArea is contained by the shape.
    final int insideShape = isAreaInsideShape(path);
    if (insideShape == SOME_INSIDE) {
      return OVERLAPS;
    }

    if (insideRectangle == ALL_INSIDE && insideShape == ALL_INSIDE) {
      //System.err.println(" inside of each other");
      return OVERLAPS;
    }

    if (path.intersects(yPlane, notableYPoints, minXPlane, maxXPlane, minZPlane, maxZPlane)) {
      //System.err.println(" edges intersect");
      return OVERLAPS;
    }

    if (insideRectangle == ALL_INSIDE) {
      //System.err.println(" shape inside rectangle");
      return WITHIN;
    }

    if (insideShape == ALL_INSIDE) {
      //System.err.println(" shape contains rectangle");
      return CONTAINS;
    }
    //System.err.println(" disjoint");
    return DISJOINT;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof XdYZSolid))
      return false;
    XdYZSolid other = (XdYZSolid) o;
    if (!super.equals(other)) {
      return false;
    }
    return other.minXPlane.equals(minXPlane) &&
      other.maxXPlane.equals(maxXPlane) &&
      other.yPlane.equals(yPlane) &&
      other.minZPlane.equals(minZPlane) &&
      other.maxZPlane.equals(maxZPlane);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result  + minXPlane.hashCode();
    result = 31 * result  + maxXPlane.hashCode();
    result = 31 * result  + yPlane.hashCode();
    result = 31 * result  + minZPlane.hashCode();
    result = 31 * result  + maxZPlane.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "XdYZSolid: {planetmodel="+planetModel+", minXplane="+minXPlane+", maxXplane="+maxXPlane+", yplane="+yPlane+", minZplane="+minZPlane+", maxZplane="+maxZPlane+"}";
  }
  
}
  
