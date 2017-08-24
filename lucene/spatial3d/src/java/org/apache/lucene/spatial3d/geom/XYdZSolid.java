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

import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;

/**
 * 3D rectangle, bounded on six sides by X,Y,Z limits, degenerate in Z
 *
 * @lucene.internal
 */
class XYdZSolid extends BaseXYZSolid {

  /** Min-X */
  protected final double minX;
  /** Max-X */
  protected final double maxX;
  /** Min-Y */
  protected final double minY;
  /** Max-Y */
  protected final double maxY;
  /** Z */
  protected final double Z;

  /** Min-X plane */
  protected final SidedPlane minXPlane;
  /** Max-X plane */
  protected final SidedPlane maxXPlane;
  /** Min-Y plane */
  protected final SidedPlane minYPlane;
  /** Max-Y plane */
  protected final SidedPlane maxYPlane;
  /** Z plane */
  protected final Plane zPlane;
  
  /** These are the edge points of the shape, which are defined to be at least one point on
   * each surface area boundary.  In the case of a solid, this includes points which represent
   * the intersection of XYZ bounding planes and the planet, as well as points representing
   * the intersection of single bounding planes with the planet itself.
   */
  protected final GeoPoint[] edgePoints;

  /** Notable points for ZPlane */
  protected final GeoPoint[] notableZPoints;

  /**
   * Sole constructor
   *
   *@param planetModel is the planet model.
   *@param minX is the minimum X value.
   *@param maxX is the maximum X value.
   *@param minY is the minimum Y value.
   *@param maxY is the maximum Y value.
   *@param Z is the Z value.
   */
  public XYdZSolid(final PlanetModel planetModel,
    final double minX,
    final double maxX,
    final double minY,
    final double maxY,
    final double Z) {
    super(planetModel);
    // Argument checking
    if (maxX - minX < Vector.MINIMUM_RESOLUTION)
      throw new IllegalArgumentException("X values in wrong order or identical");
    if (maxY - minY < Vector.MINIMUM_RESOLUTION)
      throw new IllegalArgumentException("Y values in wrong order or identical");

    this.minX = minX;
    this.maxX = maxX;
    this.minY = minY;
    this.maxY = maxY;
    this.Z = Z;

    final double worldMinZ = planetModel.getMinimumZValue();
    final double worldMaxZ = planetModel.getMaximumZValue();
    
    // Construct the planes
    minXPlane = new SidedPlane(maxX,0.0,0.0,xUnitVector,-minX);
    maxXPlane = new SidedPlane(minX,0.0,0.0,xUnitVector,-maxX);
    minYPlane = new SidedPlane(0.0,maxY,0.0,yUnitVector,-minY);
    maxYPlane = new SidedPlane(0.0,minY,0.0,yUnitVector,-maxY);
    zPlane = new Plane(zUnitVector,-Z);
      
    // We need at least one point on the planet surface for each manifestation of the shape.
    // There can be up to 2 (on opposite sides of the world).  But we have to go through
    // 4 combinations of adjacent planes in order to find out if any have 2 intersection solution.
    // Typically, this requires 4 square root operations. 
    final GeoPoint[] minXZ = minXPlane.findIntersections(planetModel,zPlane,maxXPlane,minYPlane,maxYPlane);
    final GeoPoint[] maxXZ = maxXPlane.findIntersections(planetModel,zPlane,minXPlane,minYPlane,maxYPlane);
    final GeoPoint[] minYZ = minYPlane.findIntersections(planetModel,zPlane,maxYPlane,minXPlane,maxXPlane);
    final GeoPoint[] maxYZ = maxYPlane.findIntersections(planetModel,zPlane,minYPlane,minXPlane,maxXPlane);
      
    notableZPoints = glueTogether(minXZ, maxXZ, minYZ, maxYZ);

    // Now, compute the edge points.
    // This is the trickiest part of setting up an XYZSolid.  We've computed intersections already, so
    // we'll start there.  We know that at most there will be two disconnected shapes on the planet surface.
    // But there's also a case where exactly one plane slices through the world, and none of the bounding plane
    // intersections do.  Thus, if we don't find any of the edge intersection cases, we have to look for that last case.
      
    // If we still haven't encountered anything, we need to look at single-plane/world intersections.
    // We detect these by looking at the world model and noting its x, y, and z bounds.
    // The cases we are looking for are when the four corner points for any given
    // plane are all outside of the world, AND that plane intersects the world.
    // There are four corner points all told; we must evaluate these WRT the planet surface.
    final boolean minXminYZ = planetModel.pointOutside(minX, minY, Z);
    final boolean minXmaxYZ = planetModel.pointOutside(minX, maxY, Z);
    final boolean maxXminYZ = planetModel.pointOutside(maxX, minY, Z);
    final boolean maxXmaxYZ = planetModel.pointOutside(maxX, maxY, Z);

    final GeoPoint[] zEdges;
    if (Z - worldMinZ >= -Vector.MINIMUM_RESOLUTION && Z - worldMaxZ <= Vector.MINIMUM_RESOLUTION &&
      minX < 0.0 && maxX > 0.0 && minY < 0.0 && maxY > 0.0 &&
      minXminYZ && minXmaxYZ && maxXminYZ && maxXmaxYZ) {
      // Find any point on the minZ plane that intersects the world
      // First construct a perpendicular plane that will allow us to find a sample point.
      // This plane is vertical and goes through the points (0,0,0) and (1,0,0)
      // Then use it to compute a sample point.
      final GeoPoint intPoint = zPlane.getSampleIntersectionPoint(planetModel, xVerticalPlane);
      if (intPoint != null) {
        zEdges = new GeoPoint[]{intPoint};
      } else {
        zEdges = EMPTY_POINTS;
      }
    } else {
      zEdges= EMPTY_POINTS;
    }

    this.edgePoints = glueTogether(minXZ, maxXZ, minYZ, maxYZ, zEdges);
  }

  /**
   * Constructor for deserialization.
   * @param planetModel is the planet model.
   * @param inputStream is the input stream.
   */
  public XYdZSolid(final PlanetModel planetModel, final InputStream inputStream) throws IOException {
    this(planetModel, 
      SerializableObject.readDouble(inputStream),
      SerializableObject.readDouble(inputStream),
      SerializableObject.readDouble(inputStream),
      SerializableObject.readDouble(inputStream),
      SerializableObject.readDouble(inputStream));
  }

  @Override
  public void write(final OutputStream outputStream) throws IOException {
    SerializableObject.writeDouble(outputStream, minX);
    SerializableObject.writeDouble(outputStream, maxX);
    SerializableObject.writeDouble(outputStream, minY);
    SerializableObject.writeDouble(outputStream, maxY);
    SerializableObject.writeDouble(outputStream, Z);
  }

  @Override
  protected GeoPoint[] getEdgePoints() {
    return edgePoints;
  }
  
  @Override
  public boolean isWithin(final double x, final double y, final double z) {
    return minXPlane.isWithin(x, y, z) &&
      maxXPlane.isWithin(x, y, z) &&
      minYPlane.isWithin(x, y, z) &&
      maxYPlane.isWithin(x, y, z) &&
      zPlane.evaluateIsZero(x, y, z);
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

    if (path.intersects(zPlane, notableZPoints, minXPlane, maxXPlane, minYPlane, maxYPlane)) {
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
    if (!(o instanceof XYdZSolid))
      return false;
    XYdZSolid other = (XYdZSolid) o;
    if (!super.equals(other)) {
      return false;
    }
    return other.minXPlane.equals(minXPlane) &&
      other.maxXPlane.equals(maxXPlane) &&
      other.minYPlane.equals(minYPlane) &&
      other.maxYPlane.equals(maxYPlane) &&
      other.zPlane.equals(zPlane);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result  + minXPlane.hashCode();
    result = 31 * result  + maxXPlane.hashCode();
    result = 31 * result  + minYPlane.hashCode();
    result = 31 * result  + maxYPlane.hashCode();
    result = 31 * result  + zPlane.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "XYdZSolid: {planetmodel="+planetModel+", minXplane="+minXPlane+", maxXplane="+maxXPlane+", minYplane="+minYPlane+", maxYplane="+maxYPlane+", zplane="+zPlane+"}";
  }
  
}
  
