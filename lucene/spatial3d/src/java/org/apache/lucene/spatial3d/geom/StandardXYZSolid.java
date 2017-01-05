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
 * 3D rectangle, bounded on six sides by X,Y,Z limits
 *
 * @lucene.internal
 */
class StandardXYZSolid extends BaseXYZSolid {

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
  
  /** true if minXPlane intersects globe */
  protected final boolean minXPlaneIntersects;
  /** true if maxXPlane intersects globe */
  protected final boolean maxXPlaneIntersects;
  /** true if minYPlane intersects globe */
  protected final boolean minYPlaneIntersects;
  /** true if maxYPlane intersects globe */
  protected final boolean maxYPlaneIntersects;
  /** true if minZPlane intersects globe */
  protected final boolean minZPlaneIntersects;
  /** true if maxZPlane intersects globe */
  protected final boolean maxZPlaneIntersects;

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
   * Sole constructor
   *
   *@param planetModel is the planet model.
   *@param minX is the minimum X value.
   *@param maxX is the maximum X value.
   *@param minY is the minimum Y value.
   *@param maxY is the maximum Y value.
   *@param minZ is the minimum Z value.
   *@param maxZ is the maximum Z value.
   */
  public StandardXYZSolid(final PlanetModel planetModel,
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
      minXPlaneIntersects = false;
      maxXPlaneIntersects = false;
      minYPlaneIntersects = false;
      maxYPlaneIntersects = false;
      minZPlaneIntersects = false;
      maxZPlaneIntersects = false;
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


      //System.err.println(
      //  " notableMinXPoints="+Arrays.asList(notableMinXPoints)+" notableMaxXPoints="+Arrays.asList(notableMaxXPoints)+
      //  " notableMinYPoints="+Arrays.asList(notableMinYPoints)+" notableMaxYPoints="+Arrays.asList(notableMaxYPoints)+
      //  " notableMinZPoints="+Arrays.asList(notableMinZPoints)+" notableMaxZPoints="+Arrays.asList(notableMaxZPoints));

      // Now, compute the edge points.
      // This is the trickiest part of setting up an XYZSolid.  We've computed intersections already, so
      // we'll start there.
      // There can be a number of shapes, each of which needs an edgepoint.  Each side by itself might contribute
      // an edgepoint, for instance, if the plane describing that side intercepts the planet in such a way that the ellipse
      // of interception does not meet any other planes.  Plane intersections can each contribute 0, 1, or 2 edgepoints.
      //
      // All of this makes for a lot of potential edgepoints, but I believe these can be pruned back with careful analysis.
      // I haven't yet done that analysis, however, so I will treat them all as individual edgepoints.
      
      // The cases we are looking for are when the four corner points for any given
      // plane are all outside of the world, AND that plane intersects the world.
      // There are eight corner points all told; we must evaluate these WRT the planet surface.
      final boolean minXminYminZ = planetModel.pointOutside(minX, minY, minZ);
      final boolean minXminYmaxZ = planetModel.pointOutside(minX, minY, maxZ);
      final boolean minXmaxYminZ = planetModel.pointOutside(minX, maxY, minZ);
      final boolean minXmaxYmaxZ = planetModel.pointOutside(minX, maxY, maxZ);
      final boolean maxXminYminZ = planetModel.pointOutside(maxX, minY, minZ);
      final boolean maxXminYmaxZ = planetModel.pointOutside(maxX, minY, maxZ);
      final boolean maxXmaxYminZ = planetModel.pointOutside(maxX, maxY, minZ);
      final boolean maxXmaxYmaxZ = planetModel.pointOutside(maxX, maxY, maxZ);
      
      //System.err.println("Outside world: minXminYminZ="+minXminYminZ+" minXminYmaxZ="+minXminYmaxZ+" minXmaxYminZ="+minXmaxYminZ+
      //  " minXmaxYmaxZ="+minXmaxYmaxZ+" maxXminYminZ="+maxXminYminZ+" maxXminYmaxZ="+maxXminYmaxZ+" maxXmaxYminZ="+maxXmaxYminZ+
      //  " maxXmaxYmaxZ="+maxXmaxYmaxZ);

      // Look at single-plane/world intersections.
      // We detect these by looking at the world model and noting its x, y, and z bounds.

      final GeoPoint[] minXEdges;
      if (minX - worldMinX >= -Vector.MINIMUM_RESOLUTION && minX - worldMaxX <= Vector.MINIMUM_RESOLUTION &&
        minY < 0.0 && maxY > 0.0 && minZ < 0.0 && maxZ > 0.0 &&
        minXminYminZ && minXminYmaxZ && minXmaxYminZ && minXmaxYmaxZ) {
        // Find any point on the minX plane that intersects the world
        // First construct a perpendicular plane that will allow us to find a sample point.
        // This plane is vertical and goes through the points (0,0,0) and (1,0,0)
        // Then use it to compute a sample point.
        final GeoPoint intPoint = minXPlane.getSampleIntersectionPoint(planetModel, xVerticalPlane);
        if (intPoint != null) {
          minXEdges = new GeoPoint[]{intPoint};
        } else {
          // No intersection found?
          minXEdges = EMPTY_POINTS;
        }
      } else {
        minXEdges = EMPTY_POINTS;
      }
      
      final GeoPoint[] maxXEdges;
      if (maxX - worldMinX >= -Vector.MINIMUM_RESOLUTION && maxX - worldMaxX <= Vector.MINIMUM_RESOLUTION &&
        minY < 0.0 && maxY > 0.0 && minZ < 0.0 && maxZ > 0.0 &&
        maxXminYminZ && maxXminYmaxZ && maxXmaxYminZ && maxXmaxYmaxZ) {
        // Find any point on the maxX plane that intersects the world
        // First construct a perpendicular plane that will allow us to find a sample point.
        // This plane is vertical and goes through the points (0,0,0) and (1,0,0)
        // Then use it to compute a sample point.
        final GeoPoint intPoint = maxXPlane.getSampleIntersectionPoint(planetModel, xVerticalPlane);
        if (intPoint != null) {
          maxXEdges = new GeoPoint[]{intPoint};
        } else {
          maxXEdges = EMPTY_POINTS;
        }
      } else {
        maxXEdges = EMPTY_POINTS;
      }
      
      final GeoPoint[] minYEdges;
      if (minY - worldMinY >= -Vector.MINIMUM_RESOLUTION && minY - worldMaxY <= Vector.MINIMUM_RESOLUTION &&
        minX < 0.0 && maxX > 0.0 && minZ < 0.0 && maxZ > 0.0 &&
        minXminYminZ && minXminYmaxZ && maxXminYminZ && maxXminYmaxZ) {
        // Find any point on the minY plane that intersects the world
        // First construct a perpendicular plane that will allow us to find a sample point.
        // This plane is vertical and goes through the points (0,0,0) and (0,1,0)
        // Then use it to compute a sample point.
        final GeoPoint intPoint = minYPlane.getSampleIntersectionPoint(planetModel, yVerticalPlane);
        if (intPoint != null) {
          minYEdges = new GeoPoint[]{intPoint};
        } else {
          minYEdges = EMPTY_POINTS;
        }
      } else {
        minYEdges = EMPTY_POINTS;
      }
      
      final GeoPoint[] maxYEdges;
      if (maxY - worldMinY >= -Vector.MINIMUM_RESOLUTION && maxY - worldMaxY <= Vector.MINIMUM_RESOLUTION &&
        minX < 0.0 && maxX > 0.0 && minZ < 0.0 && maxZ > 0.0 &&
        minXmaxYminZ && minXmaxYmaxZ && maxXmaxYminZ && maxXmaxYmaxZ) {
        // Find any point on the maxY plane that intersects the world
        // First construct a perpendicular plane that will allow us to find a sample point.
        // This plane is vertical and goes through the points (0,0,0) and (0,1,0)
        // Then use it to compute a sample point.
        final GeoPoint intPoint = maxYPlane.getSampleIntersectionPoint(planetModel, yVerticalPlane);
        if (intPoint != null) {
          maxYEdges = new GeoPoint[]{intPoint};
        } else {
          maxYEdges = EMPTY_POINTS;
        }
      } else {
        maxYEdges = EMPTY_POINTS;
      }
      
      final GeoPoint[] minZEdges;
      if (minZ - worldMinZ >= -Vector.MINIMUM_RESOLUTION && minZ - worldMaxZ <= Vector.MINIMUM_RESOLUTION &&
        minX < 0.0 && maxX > 0.0 && minY < 0.0 && maxY > 0.0 &&
        minXminYminZ && minXmaxYminZ && maxXminYminZ && maxXmaxYminZ) {
        // Find any point on the minZ plane that intersects the world
        // First construct a perpendicular plane that will allow us to find a sample point.
        // This plane is vertical and goes through the points (0,0,0) and (1,0,0)
        // Then use it to compute a sample point.
        final GeoPoint intPoint = minZPlane.getSampleIntersectionPoint(planetModel, xVerticalPlane);
        if (intPoint != null) {
          minZEdges = new GeoPoint[]{intPoint};
        } else {
          minZEdges = EMPTY_POINTS;
        }
      } else {
        minZEdges = EMPTY_POINTS;
      }
      
      final GeoPoint[] maxZEdges;
      if (maxZ - worldMinZ >= -Vector.MINIMUM_RESOLUTION && maxZ - worldMaxZ <= Vector.MINIMUM_RESOLUTION &&
        minX < 0.0 && maxX > 0.0 && minY < 0.0 && maxY > 0.0 &&
        minXminYmaxZ && minXmaxYmaxZ && maxXminYmaxZ && maxXmaxYmaxZ) {
        // Find any point on the maxZ plane that intersects the world
        // First construct a perpendicular plane that will allow us to find a sample point.
        // This plane is vertical and goes through the points (0,0,0) and (1,0,0) (that is, its orientation doesn't matter)
        // Then use it to compute a sample point.
        final GeoPoint intPoint = maxZPlane.getSampleIntersectionPoint(planetModel, xVerticalPlane);
        if (intPoint != null) {
          maxZEdges = new GeoPoint[]{intPoint};
        } else {
          maxZEdges = EMPTY_POINTS;
        }
      } else {
        maxZEdges = EMPTY_POINTS;
      }
      
      //System.err.println(
      //  " minXEdges="+Arrays.asList(minXEdges)+" maxXEdges="+Arrays.asList(maxXEdges)+
      //  " minYEdges="+Arrays.asList(minYEdges)+" maxYEdges="+Arrays.asList(maxYEdges)+
      //  " minZEdges="+Arrays.asList(minZEdges)+" maxZEdges="+Arrays.asList(maxZEdges));

      minXPlaneIntersects = notableMinXPoints.length + minXEdges.length > 0;
      maxXPlaneIntersects = notableMaxXPoints.length + maxXEdges.length > 0;
      minYPlaneIntersects = notableMinYPoints.length + minYEdges.length > 0;
      maxYPlaneIntersects = notableMaxYPoints.length + maxYEdges.length > 0;
      minZPlaneIntersects = notableMinZPoints.length + minZEdges.length > 0;
      maxZPlaneIntersects = notableMaxZPoints.length + maxZEdges.length > 0;

      // Glue everything together.  This is not a minimal set of edgepoints, as of now, but it does completely describe all shapes on the
      // planet.
      this.edgePoints = glueTogether(minXminY, minXmaxY, minXminZ, minXmaxZ,
        maxXminY, maxXmaxY, maxXminZ, maxXmaxZ,
        minYminZ, minYmaxZ, maxYminZ, maxYmaxZ,
        minXEdges, maxXEdges, minYEdges, maxYEdges, minZEdges, maxZEdges);
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
    
    /*
    for (GeoPoint p : getEdgePoints()) {
      System.err.println(" Edge point "+p+" path.isWithin()? "+path.isWithin(p));
    }
    
    for (GeoPoint p : path.getEdgePoints()) {
      System.err.println(" path edge point "+p+" isWithin()? "+isWithin(p)+" minx="+minXPlane.evaluate(p)+" maxx="+maxXPlane.evaluate(p)+" miny="+minYPlane.evaluate(p)+" maxy="+maxYPlane.evaluate(p)+" minz="+minZPlane.evaluate(p)+" maxz="+maxZPlane.evaluate(p));
    }
    */
    
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

    if ((minXPlaneIntersects && path.intersects(minXPlane, notableMinXPoints, maxXPlane, minYPlane, maxYPlane, minZPlane, maxZPlane)) ||
        (maxXPlaneIntersects && path.intersects(maxXPlane, notableMaxXPoints, minXPlane, minYPlane, maxYPlane, minZPlane, maxZPlane)) ||
        (minYPlaneIntersects && path.intersects(minYPlane, notableMinYPoints, maxYPlane, minXPlane, maxXPlane, minZPlane, maxZPlane)) ||
        (maxYPlaneIntersects && path.intersects(maxYPlane, notableMaxYPoints, minYPlane, minXPlane, maxXPlane, minZPlane, maxZPlane)) ||
        (minZPlaneIntersects && path.intersects(minZPlane, notableMinZPoints, maxZPlane, minXPlane, maxXPlane, minYPlane, maxYPlane)) ||
        (maxZPlaneIntersects && path.intersects(maxZPlane, notableMaxZPoints, minZPlane, minXPlane, maxXPlane, minYPlane, maxYPlane))) {
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
    if (!(o instanceof StandardXYZSolid))
      return false;
    StandardXYZSolid other = (StandardXYZSolid) o;
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
    return "StandardXYZSolid: {planetmodel="+planetModel+", isWholeWorld="+isWholeWorld+", minXplane="+minXPlane+", maxXplane="+maxXPlane+", minYplane="+minYPlane+", maxYplane="+maxYPlane+", minZplane="+minZPlane+", maxZplane="+maxZPlane+"}";
  }
  
}
  
