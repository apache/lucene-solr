package org.apache.lucene.geo3d;

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
 * 3D rectangle, bounded on six sides by X,Y,Z limits
 *
 * @lucene.internal
 */
public class XYZSolid extends BaseXYZSolid {

  /** Whole world? */
  protected final boolean isWholeWorld;
  /** Min-X plane */
  protected final SidedPlane minXPlane;
  /** Max-X plane */
  protected final SidedPlane maxXPlane;
  /** Min-Y plane */
  protected final SidedPlane minYPlane;
  /** Max-Y plane */
  protected final SidedPlane maxYPlane;
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

  /** Notable points for minXPlane */
  protected final GeoPoint[] notableMinXPoints;
  /** Notable points for maxXPlane */
  protected final GeoPoint[] notableMaxXPoints;
  /** Notable points for minYPlane */
  protected final GeoPoint[] notableMinYPoints;
  /** Notable points for maxYPlane */
  protected final GeoPoint[] notableMaxYPoints;
  /** Notable points for minZPlane */
  protected final GeoPoint[] notableMinZPoints;
  /** Notable points for maxZPlane */
  protected final GeoPoint[] notableMaxZPoints;

  /**
   *@param planetModel is the planet model.
   *@param minX is the minimum X value.
   *@param maxX is the maximum X value.
   *@param minY is the minimum Y value.
   *@param maxY is the maximum Y value.
   *@param minZ is the minimum Z value.
   *@param maxZ is the maximum Z value.
   */
  public XYZSolid(final PlanetModel planetModel,
    final double minX,
    final double maxX,
    final double minY,
    final double maxY,
    final double minZ,
    final double maxZ) {
    super(planetModel);
    // Argument checking
    if (maxX - minX < Vector.MINIMUM_RESOLUTION)
      throw new IllegalArgumentException("X values in wrong order or identical");
    if (maxY - minY < Vector.MINIMUM_RESOLUTION)
      throw new IllegalArgumentException("Y values in wrong order or identical");
    if (maxZ - minZ < Vector.MINIMUM_RESOLUTION)
      throw new IllegalArgumentException("Z values in wrong order or identical");

    final double worldMinX = planetModel.getMinimumXValue();
    final double worldMaxX = planetModel.getMaximumXValue();
    final double worldMinY = planetModel.getMinimumYValue();
    final double worldMaxY = planetModel.getMaximumYValue();
    final double worldMinZ = planetModel.getMinimumZValue();
    final double worldMaxZ = planetModel.getMaximumZValue();
    
    // We must distinguish between the case where the solid represents the entire world,
    // and when the solid has no overlap with any part of the surface.  In both cases,
    // there will be no edgepoints.
    isWholeWorld =
        (minX - worldMinX < -Vector.MINIMUM_RESOLUTION) &&
        (maxX - worldMaxX > Vector.MINIMUM_RESOLUTION) &&
        (minY - worldMinY < -Vector.MINIMUM_RESOLUTION) &&
        (maxY - worldMaxY > Vector.MINIMUM_RESOLUTION) &&
        (minZ - worldMinZ < -Vector.MINIMUM_RESOLUTION) &&
        (maxZ - worldMaxZ > Vector.MINIMUM_RESOLUTION);

    if (isWholeWorld) {
      minXPlane = null;
      maxXPlane = null;
      minYPlane = null;
      maxYPlane = null;
      minZPlane = null;
      maxZPlane = null;
      notableMinXPoints = null;
      notableMaxXPoints = null;
      notableMinYPoints = null;
      notableMaxYPoints = null;
      notableMinZPoints = null;
      notableMaxZPoints = null;
      edgePoints = null;
    } else {
      // Construct the planes
      minXPlane = new SidedPlane(maxX,0.0,0.0,xUnitVector,-minX);
      maxXPlane = new SidedPlane(minX,0.0,0.0,xUnitVector,-maxX);
      minYPlane = new SidedPlane(0.0,maxY,0.0,yUnitVector,-minY);
      maxYPlane = new SidedPlane(0.0,minY,0.0,yUnitVector,-maxY);
      minZPlane = new SidedPlane(0.0,0.0,maxZ,zUnitVector,-minZ);
      maxZPlane = new SidedPlane(0.0,0.0,minZ,zUnitVector,-maxZ);
      
      // We need at least one point on the planet surface for each manifestation of the shape.
      // There can be up to 2 (on opposite sides of the world).  But we have to go through
      // 12 combinations of adjacent planes in order to find out if any have 2 intersection solution.
      // Typically, this requires 12 square root operations. 
      final GeoPoint[] minXminY = minXPlane.findIntersections(planetModel,minYPlane,maxXPlane,maxYPlane,minZPlane,maxZPlane);
      final GeoPoint[] minXmaxY = minXPlane.findIntersections(planetModel,maxYPlane,maxXPlane,minYPlane,minZPlane,maxZPlane);
      final GeoPoint[] minXminZ = minXPlane.findIntersections(planetModel,minZPlane,maxXPlane,maxZPlane,minYPlane,maxYPlane);
      final GeoPoint[] minXmaxZ = minXPlane.findIntersections(planetModel,maxZPlane,maxXPlane,minZPlane,minYPlane,maxYPlane);

      final GeoPoint[] maxXminY = maxXPlane.findIntersections(planetModel,minYPlane,minXPlane,maxYPlane,minZPlane,maxZPlane);
      final GeoPoint[] maxXmaxY = maxXPlane.findIntersections(planetModel,maxYPlane,minXPlane,minYPlane,minZPlane,maxZPlane);
      final GeoPoint[] maxXminZ = maxXPlane.findIntersections(planetModel,minZPlane,minXPlane,maxZPlane,minYPlane,maxYPlane);
      final GeoPoint[] maxXmaxZ = maxXPlane.findIntersections(planetModel,maxZPlane,minXPlane,minZPlane,minYPlane,maxYPlane);
      
      final GeoPoint[] minYminZ = minYPlane.findIntersections(planetModel,minZPlane,maxYPlane,maxZPlane,minXPlane,maxXPlane);
      final GeoPoint[] minYmaxZ = minYPlane.findIntersections(planetModel,maxZPlane,maxYPlane,minZPlane,minXPlane,maxXPlane);
      final GeoPoint[] maxYminZ = maxYPlane.findIntersections(planetModel,minZPlane,minYPlane,maxZPlane,minXPlane,maxXPlane);
      final GeoPoint[] maxYmaxZ = maxYPlane.findIntersections(planetModel,maxZPlane,minYPlane,minZPlane,minXPlane,maxXPlane);
      
      notableMinXPoints = glueTogether(minXminY, minXmaxY, minXminZ, minXmaxZ);
      notableMaxXPoints = glueTogether(maxXminY, maxXmaxY, maxXminZ, maxXmaxZ);
      notableMinYPoints = glueTogether(minXminY, maxXminY, minYminZ, minYmaxZ);
      notableMaxYPoints = glueTogether(minXmaxY, maxXmaxY, maxYminZ, maxYmaxZ);
      notableMinZPoints = glueTogether(minXminZ, maxXminZ, minYminZ, maxYminZ);
      notableMaxZPoints = glueTogether(minXmaxZ, maxXmaxZ, minYmaxZ, maxYmaxZ);

      // Now, compute the edge points.
      // This is the trickiest part of setting up an XYZSolid.  We've computed intersections already, so
      // we'll start there.  We know that at most there will be two disconnected shapes on the planet surface.
      // But there's also a case where exactly one plane slices through the world, and none of the bounding plane
      // intersections do.  Thus, if we don't find any of the edge intersection cases, we have to look for that last case.
      GeoPoint[] edgePoints = findLargestSolution(minXminY,minXmaxY,minXminZ,minXmaxZ,
        maxXminY,maxXmaxY,maxXminZ,maxXmaxZ,
        minYminZ,minYmaxZ,maxYminZ,maxYmaxZ);
      
      if (edgePoints.length == 0) {
        // If we still haven't encountered anything, we need to look at single-plane/world intersections.
        // We detect these by looking at the world model and noting its x, y, and z bounds.

        if (minX - worldMinX >= -Vector.MINIMUM_RESOLUTION && minX - worldMaxX <= Vector.MINIMUM_RESOLUTION &&
          (maxX - worldMaxX > Vector.MINIMUM_RESOLUTION) &&
          (minY - worldMinY < -Vector.MINIMUM_RESOLUTION) &&
          (maxY - worldMaxY > Vector.MINIMUM_RESOLUTION) &&
          (minZ - worldMinZ < -Vector.MINIMUM_RESOLUTION) &&
          (maxZ - worldMaxZ > Vector.MINIMUM_RESOLUTION)) {
          // Find any point on the minX plane that intersects the world
          // First construct a perpendicular plane that will allow us to find a sample point.
          // This plane is vertical and goes through the points (0,0,0) and (1,0,0)
          // Then use it to compute a sample point.
          edgePoints = new GeoPoint[]{minXPlane.getSampleIntersectionPoint(planetModel, xVerticalPlane)};
        } else if (maxX - worldMinX >= -Vector.MINIMUM_RESOLUTION && maxX - worldMaxX <= Vector.MINIMUM_RESOLUTION &&
          (minX - worldMinX < -Vector.MINIMUM_RESOLUTION) &&
          (minY - worldMinY < -Vector.MINIMUM_RESOLUTION) &&
          (maxY - worldMaxY > Vector.MINIMUM_RESOLUTION) &&
          (minZ - worldMinZ < -Vector.MINIMUM_RESOLUTION) &&
          (maxZ - worldMaxZ > Vector.MINIMUM_RESOLUTION)) {
          // Find any point on the maxX plane that intersects the world
          // First construct a perpendicular plane that will allow us to find a sample point.
          // This plane is vertical and goes through the points (0,0,0) and (1,0,0)
          // Then use it to compute a sample point.
          edgePoints = new GeoPoint[]{maxXPlane.getSampleIntersectionPoint(planetModel, xVerticalPlane)};
        } else if (minY - worldMinY >= -Vector.MINIMUM_RESOLUTION && minY - worldMaxY <= Vector.MINIMUM_RESOLUTION &&
          (maxY - worldMaxY > Vector.MINIMUM_RESOLUTION) &&
          (minX - worldMinX < -Vector.MINIMUM_RESOLUTION) &&
          (maxX - worldMaxX > Vector.MINIMUM_RESOLUTION) &&
          (minZ - worldMinZ < -Vector.MINIMUM_RESOLUTION) &&
          (maxZ - worldMaxZ > Vector.MINIMUM_RESOLUTION)) {
          // Find any point on the minY plane that intersects the world
          // First construct a perpendicular plane that will allow us to find a sample point.
          // This plane is vertical and goes through the points (0,0,0) and (0,1,0)
          // Then use it to compute a sample point.
          edgePoints = new GeoPoint[]{minYPlane.getSampleIntersectionPoint(planetModel, yVerticalPlane)};
        } else if (maxY - worldMinY >= -Vector.MINIMUM_RESOLUTION && maxY - worldMaxY <= Vector.MINIMUM_RESOLUTION &&
          (minY - worldMinY < -Vector.MINIMUM_RESOLUTION) &&
          (minX - worldMinX < -Vector.MINIMUM_RESOLUTION) &&
          (maxX - worldMaxX > Vector.MINIMUM_RESOLUTION) &&
          (minZ - worldMinZ < -Vector.MINIMUM_RESOLUTION) &&
          (maxZ - worldMaxZ > Vector.MINIMUM_RESOLUTION)) {
          // Find any point on the maxY plane that intersects the world
          // First construct a perpendicular plane that will allow us to find a sample point.
          // This plane is vertical and goes through the points (0,0,0) and (0,1,0)
          // Then use it to compute a sample point.
          edgePoints = new GeoPoint[]{maxYPlane.getSampleIntersectionPoint(planetModel, yVerticalPlane)};
        } else if (minZ - worldMinZ >= -Vector.MINIMUM_RESOLUTION && minZ - worldMaxZ <= Vector.MINIMUM_RESOLUTION &&
          (maxZ - worldMaxZ > Vector.MINIMUM_RESOLUTION) &&
          (minY - worldMinY < -Vector.MINIMUM_RESOLUTION) &&
          (maxY - worldMaxY > Vector.MINIMUM_RESOLUTION) &&
          (minX - worldMinX < -Vector.MINIMUM_RESOLUTION) &&
          (maxX - worldMaxX > Vector.MINIMUM_RESOLUTION)) {
          // Find any point on the minZ plane that intersects the world
          // First construct a perpendicular plane that will allow us to find a sample point.
          // This plane is vertical and goes through the points (0,0,0) and (1,0,0)
          // Then use it to compute a sample point.
          edgePoints = new GeoPoint[]{minZPlane.getSampleIntersectionPoint(planetModel, xVerticalPlane)};
        } else if (maxZ - worldMinZ >= -Vector.MINIMUM_RESOLUTION && maxZ - worldMaxZ <= Vector.MINIMUM_RESOLUTION &&
          (minZ - worldMinZ < -Vector.MINIMUM_RESOLUTION) &&
          (minY - worldMinY < -Vector.MINIMUM_RESOLUTION) &&
          (maxY - worldMaxY > Vector.MINIMUM_RESOLUTION) &&
          (minX - worldMinX < -Vector.MINIMUM_RESOLUTION) &&
          (maxX - worldMaxX > Vector.MINIMUM_RESOLUTION)) {
          // Find any point on the maxZ plane that intersects the world
          // First construct a perpendicular plane that will allow us to find a sample point.
          // This plane is vertical and goes through the points (0,0,0) and (1,0,0) (that is, its orientation doesn't matter)
          // Then use it to compute a sample point.
          edgePoints = new GeoPoint[]{maxZPlane.getSampleIntersectionPoint(planetModel, xVerticalPlane)};
        }
      }

      this.edgePoints = edgePoints;
    }
  }

  @Override
  protected GeoPoint[] getEdgePoints() {
    return edgePoints;
  }
  
  @Override
  public boolean isWithin(final double x, final double y, final double z) {
    if (isWholeWorld) {
      return true;
    }
    return minXPlane.isWithin(x, y, z) &&
      maxXPlane.isWithin(x, y, z) &&
      minYPlane.isWithin(x, y, z) &&
      maxYPlane.isWithin(x, y, z) &&
      minZPlane.isWithin(x, y, z) &&
      maxZPlane.isWithin(x, y, z);
  }

  @Override
  public int getRelationship(final GeoShape path) {
    if (isWholeWorld) {
      if (path.getEdgePoints().length > 0)
        return WITHIN;
      return OVERLAPS;
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

    if (path.intersects(minXPlane, notableMinXPoints, maxXPlane, minYPlane, maxYPlane, minZPlane, maxZPlane) ||
        path.intersects(maxXPlane, notableMaxXPoints, minXPlane, minYPlane, maxYPlane, minZPlane, maxZPlane) ||
        path.intersects(minYPlane, notableMinYPoints, maxYPlane, minXPlane, maxXPlane, minZPlane, maxZPlane) ||
        path.intersects(maxYPlane, notableMaxYPoints, minYPlane, minXPlane, maxXPlane, minZPlane, maxZPlane) ||
        path.intersects(minZPlane, notableMinZPoints, maxZPlane, minXPlane, maxXPlane, minYPlane, maxYPlane) ||
        path.intersects(maxZPlane, notableMaxZPoints, minZPlane, minXPlane, maxXPlane, minYPlane, maxYPlane)) {
      //System.err.println(" edges intersect");
      return OVERLAPS;
    }

    if (insideRectangle == ALL_INSIDE) {
      //System.err.println(" all shape points inside area");
      return WITHIN;
    }

    if (insideShape == ALL_INSIDE) {
      //System.err.println(" all area points inside shape");
      return CONTAINS;
    }
    //System.err.println(" disjoint");
    return DISJOINT;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof XYZSolid))
      return false;
    XYZSolid other = (XYZSolid) o;
    if (!super.equals(other) ||
      other.isWholeWorld != isWholeWorld) {
      return false;
    }
    if (!isWholeWorld) {
      return other.minXPlane.equals(minXPlane) &&
        other.maxXPlane.equals(maxXPlane) &&
        other.minYPlane.equals(minYPlane) &&
        other.maxYPlane.equals(maxYPlane) &&
        other.minZPlane.equals(minZPlane) &&
        other.maxZPlane.equals(maxZPlane);
    }
    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (isWholeWorld?1:0);
    if (!isWholeWorld) {
      result = 31 * result  + minXPlane.hashCode();
      result = 31 * result  + maxXPlane.hashCode();
      result = 31 * result  + minYPlane.hashCode();
      result = 31 * result  + maxYPlane.hashCode();
      result = 31 * result  + minZPlane.hashCode();
      result = 31 * result  + maxZPlane.hashCode();
    }
    return result;
  }

  @Override
  public String toString() {
    return "XYZSolid: {planetmodel="+planetModel+", isWholeWorld="+isWholeWorld+", minXplane="+minXPlane+", maxXplane="+maxXPlane+", minYplane="+minYPlane+", maxYplane="+maxYPlane+", minZplane="+minZPlane+", maxZplane="+maxZPlane+"}";
  }
  
}
  
